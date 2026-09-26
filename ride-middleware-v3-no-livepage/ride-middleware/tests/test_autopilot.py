"""Autopilot (Person 1): decisions + healing + scaling, with fake Docker and fake Prometheus."""
from services.autopilot.autopilot import Autopilot, Prometheus
from services.autopilot.docker_ops import DockerOps
from services.autopilot.policy import Policy, PolicyConfig, lag_by_service


def test_lag_maps_consumer_groups_to_services():
    assert lag_by_service({"booking-writer": 1500, "validator": 3, "unknown-group": 99}) == \
        {"consumer": 1500.0, "validator": 3.0}


def test_scale_up_on_lag_then_cooldown():
    p = Policy(PolicyConfig(scale_up_lag=1000, cooldown_s=60))
    d = p.decide({"consumer": 5000}, {"consumer": 1}, now=0)
    assert [(x.service, x.action) for x in d] == [("consumer", "SCALE_UP")]
    assert p.decide({"consumer": 5000}, {"consumer": 2}, now=30) == []            # cooling down
    assert p.decide({"consumer": 5000}, {"consumer": 2}, now=61)[0].action == "SCALE_UP"


def test_never_more_copies_than_partitions():
    p = Policy()
    assert p.decide({"consumer": 99999}, {"consumer": 6}, now=0) == []


def test_scale_down_only_after_a_calm_minute_and_not_below_min():
    p = Policy(PolicyConfig(calm_checks=4, cooldown_s=0))
    for t in range(3):
        assert p.decide({"consumer": 0}, {"consumer": 3}, now=t) == []
    assert p.decide({"consumer": 0}, {"consumer": 3}, now=4)[0].action == "SCALE_DOWN"
    for t in range(10):
        assert p.decide({"consumer": 0}, {"consumer": 1}, now=10 + t) == []       # min 1


def test_api_scales_on_requests_per_copy_and_keeps_three():
    p = Policy(PolicyConfig(api_rps_per_copy=300, cooldown_s=0, calm_checks=1))
    assert p.decide({"api": 450}, {"api": 3}, now=0)[0].action == "SCALE_UP"
    assert p.decide({"api": 10}, {"api": 3}, now=1) == []                          # min 3 for HA
    assert p.decide({"api": 10}, {"api": 2}, now=2)[0].action == "SCALE_UP"        # a copy died -> refill


def test_no_data_means_no_action():
    assert Policy().decide({}, {"consumer": 1}, now=0) == []


# ---------------------------------------------------------------- Autopilot loop with fakes
class FakeContainer:
    def __init__(self, name, service, managed=False, number=1, health=None, status="running"):
        self.name, self.status = name, status
        self.labels = {"com.docker.compose.project": "ride-middleware", "com.docker.compose.service": service,
                       "com.docker.compose.container-number": str(number)}
        if managed:
            self.labels["autopilot.managed"] = "true"
        self.attrs = {"State": {"Health": {"Status": health}} if health else {}}
        self.restarted = self.stopped = self.removed = False

    def restart(self, timeout=None):
        self.restarted = True

    def stop(self, timeout=None):
        self.stopped = True

    def remove(self):
        self.removed = True


class FakeDocker:
    def __init__(self, containers):
        self.containers, self.cloned = containers, []

    def replicas(self, services):
        out = {s: 0 for s in services}
        for c in self.containers:
            s = c.labels["com.docker.compose.service"]
            if s in out:
                out[s] += 1
        return out

    def unhealthy(self):
        return [c for c in self.containers if c.attrs["State"].get("Health", {}).get("Status") == "unhealthy"]

    def restart(self, c):
        c.restart()

    def clone(self, service):
        self.cloned.append(service)
        name = f"ride-middleware-{service}-auto-{101 + len(self.cloned)}"
        self.containers.append(FakeContainer(name, service, managed=True))
        return name

    def remove_one(self, service):
        mine = [c for c in self.containers if c.labels["com.docker.compose.service"] == service
                and c.labels.get("autopilot.managed")]
        if not mine:
            return None
        self.containers.remove(mine[-1])
        return mine[-1].name


class FakeProm:
    def __init__(self, load=None, fail=False):
        self._load, self.fail = load or {}, fail

    def load(self, api_copies):
        if self.fail:
            raise ConnectionError("prometheus down")
        return self._load


def pilot(docker, prom, **cfg):
    events = []
    ap = Autopilot(docker, prom, Policy(PolicyConfig(**cfg)), lambda a, s, d: events.append((a, s, d)))
    return ap, events


def test_heals_unhealthy_container_and_reports_it():
    sick = FakeContainer("ride-middleware-consumer-1", "consumer", health="unhealthy")
    ap, events = pilot(FakeDocker([sick]), FakeProm())
    assert ap.tick(0) == [("HEAL", "consumer")]
    assert sick.restarted and events[0][:2] == ("HEAL", "consumer")


def test_heals_even_when_prometheus_is_down():
    sick = FakeContainer("x", "validator", health="unhealthy")
    ap, _ = pilot(FakeDocker([sick]), FakeProm(fail=True))
    assert ap.tick(0) == [("HEAL", "validator")]


def test_scale_up_under_lag_then_back_down_when_calm():
    docker = FakeDocker([FakeContainer("ride-middleware-consumer-1", "consumer")])
    prom = FakeProm({"consumer": 20000})
    ap, events = pilot(docker, prom, cooldown_s=0, calm_checks=2)
    assert ap.tick(0) == [("SCALE_UP", "consumer")]
    assert ap.tick(1) == [("SCALE_UP", "consumer")]
    assert docker.replicas(["consumer"])["consumer"] == 3
    prom._load = {"consumer": 0}
    ap.tick(2)
    assert ap.tick(3) == [("SCALE_DOWN", "consumer")]
    ap.tick(4)
    assert ap.tick(5) == [("SCALE_DOWN", "consumer")]
    ap.tick(6)
    assert ap.tick(7) == []                              # only the compose copy is left: never removed
    assert [c.name for c in docker.containers] == ["ride-middleware-consumer-1"]
    assert [e[0] for e in events] == ["SCALE_UP", "SCALE_UP", "SCALE_DOWN", "SCALE_DOWN"]


def test_dry_run_touches_nothing():
    docker = FakeDocker([FakeContainer("c1", "consumer")])
    ap, events = pilot(docker, FakeProm({"consumer": 20000}), cooldown_s=0)
    ap.dry = True
    assert ap.tick(0) == [("SCALE_UP", "consumer")]
    assert docker.cloned == [] and events[0][2]["dry_run"] is True


def test_prometheus_query_parsing():
    class Resp:
        def __init__(self, result):
            self.result = result

        def raise_for_status(self):
            pass

        def json(self):
            return {"data": {"result": self.result}}

    class Http:
        def get(self, url, params):
            if "consumergroup" in params["query"]:
                return Resp([{"metric": {"consumergroup": "booking-writer"}, "value": [0, "2500"]},
                             {"metric": {"consumergroup": "dispatcher"}, "value": [0, "7"]}])
            return Resp([{"metric": {}, "value": [0, "900"]}])

    load = Prometheus("http://prom:9090", http=Http()).load(api_copies=3)
    assert load == {"consumer": 2500.0, "dispatcher": 7.0, "api": 300.0}


# ---------------------------------------------------------------- DockerOps with a fake docker client
class SdkContainer(FakeContainer):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.attrs.update({"Config": {"Image": "ride-middleware/app", "Cmd": ["python", "-m", "x"], "Env": ["A=1"],
                                      "Labels": dict(self.labels), "User": "10001", "Entrypoint": None,
                                      "Healthcheck": None},
                           "HostConfig": {"RestartPolicy": {"Name": "unless-stopped"}, "ReadonlyRootfs": True,
                                          "SecurityOpt": ["no-new-privileges:true"], "CapDrop": ["ALL"],
                                          "Memory": 268435456, "Tmpfs": {"/tmp": ""}, "Binds": None},
                           "NetworkSettings": {"Networks": {"ride-middleware_default": {}}}})
        self.started = False

    def start(self):
        self.started = True


class FakeSdk:
    def __init__(self, existing):
        self.existing, self.created, self.connected = existing, [], []
        outer = self

        class Containers:
            def list(self, filters=None, **_):
                want = dict(f.split("=", 1) for f in filters["label"])
                return [c for c in outer.existing if all(c.labels.get(k) == v for k, v in want.items())]

            def create(self, **kw):
                c = SdkContainer(kw["name"], kw["labels"]["com.docker.compose.service"], managed=True,
                                 number=int(kw["labels"]["com.docker.compose.container-number"]))
                outer.created.append(kw)
                outer.existing.append(c)
                return c

        class Net:
            def __init__(self, name):
                self.name = name

            def connect(self, c, aliases=None):
                outer.connected.append((self.name, c.name, aliases))

            def disconnect(self, c):
                pass

        class Networks:
            def get(self, name):
                return Net(name)

        self.containers, self.networks = Containers(), Networks()


def test_clone_copies_security_settings_and_joins_network_with_alias():
    sdk = FakeSdk([SdkContainer("ride-middleware-dispatcher-1", "dispatcher")])
    ops = DockerOps("ride-middleware", client=sdk)
    name = ops.clone("dispatcher")
    kw = sdk.created[0]
    assert name == "ride-middleware-dispatcher-auto-101"
    assert kw["user"] == "10001" and kw["read_only"] is True and kw["cap_drop"] == ["ALL"]
    assert kw["security_opt"] == ["no-new-privileges:true"] and kw["labels"]["autopilot.managed"] == "true"
    assert sdk.connected == [("ride-middleware_default", name, ["dispatcher"])]
    assert ops.replicas(["dispatcher"]) == {"dispatcher": 2}
    assert ops.remove_one("dispatcher") == name
