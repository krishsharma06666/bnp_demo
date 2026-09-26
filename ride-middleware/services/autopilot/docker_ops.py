"""Talks to Docker for the Autopilot: count, clone, remove and restart containers. Owner: Person 1.

A new copy is a CLONE of a running copy of the same service (same image, command, env, limits,
security options), joined to the same network with the service name as DNS alias - so HAProxy
(api), Prometheus (dns_sd) and Kafka (consumer group) pick it up automatically. Clones carry the
compose labels, so `docker compose down` removes them too, plus `autopilot.managed=true` so the
Autopilot only ever removes copies it created itself.
"""
PROJECT_LABEL = "com.docker.compose.project"
SERVICE_LABEL = "com.docker.compose.service"
NUMBER_LABEL = "com.docker.compose.container-number"
MANAGED_LABEL = "autopilot.managed"


class DockerOps:
    def __init__(self, project: str, client=None):
        if client is None:
            import docker
            client = docker.from_env()
        self.d, self.project = client, project

    def _list(self, service: str | None = None, all_=False):
        labels = [f"{PROJECT_LABEL}={self.project}"] + ([f"{SERVICE_LABEL}={service}"] if service else [])
        return self.d.containers.list(all=all_, filters={"label": labels})

    def replicas(self, services) -> dict[str, int]:
        counts = {s: 0 for s in services}
        for c in self._list():
            s = c.labels.get(SERVICE_LABEL)
            if s in counts and c.status == "running":
                counts[s] += 1
        return counts

    def unhealthy(self):
        out = []
        for c in self._list():
            health = (c.attrs.get("State") or {}).get("Health") or {}
            if health.get("Status") == "unhealthy":
                out.append(c)
        return out

    def restart(self, container):
        container.restart(timeout=20)

    def clone(self, service: str) -> str:
        running = [c for c in self._list(service) if c.status == "running"]
        if not running:
            raise RuntimeError(f"no running copy of {service} to clone")
        src = running[0]
        cfg, host = src.attrs["Config"], src.attrs["HostConfig"]
        numbers = [int(c.labels.get(NUMBER_LABEL, 0) or 0) for c in self._list(service, all_=True)]
        num = max([100, *numbers]) + 1
        name = f"{self.project}-{service}-auto-{num}"
        labels = {**cfg.get("Labels", {}), NUMBER_LABEL: str(num), MANAGED_LABEL: "true"}
        restart = host.get("RestartPolicy") or {}
        c = self.d.containers.create(
            image=cfg["Image"], command=cfg.get("Cmd"), entrypoint=cfg.get("Entrypoint"),
            environment=cfg.get("Env"), labels=labels, name=name, hostname=name, user=cfg.get("User") or None,
            healthcheck=cfg.get("Healthcheck"),
            restart_policy={"Name": restart.get("Name") or "unless-stopped"},
            read_only=host.get("ReadonlyRootfs", False), security_opt=host.get("SecurityOpt"),
            cap_drop=host.get("CapDrop"), mem_limit=host.get("Memory") or None, tmpfs=host.get("Tmpfs"),
            volumes=host.get("Binds"), detach=True)
        networks = list(src.attrs["NetworkSettings"]["Networks"])
        for net in networks:                                  # same networks, service name as DNS alias
            self.d.networks.get(net).connect(c, aliases=[service])
        if "bridge" not in networks:
            try:
                self.d.networks.get("bridge").disconnect(c)
            except Exception:
                pass
        c.start()
        return name

    def remove_one(self, service: str) -> str | None:
        """Stop the newest copy that the Autopilot created (never a compose-defined copy)."""
        mine = [c for c in self._list(service) if c.labels.get(MANAGED_LABEL) == "true"]
        if not mine:
            return None
        newest = max(mine, key=lambda c: int(c.labels.get(NUMBER_LABEL, 0)))
        newest.stop(timeout=30)                              # SIGTERM -> consumer finishes batch, commits
        newest.remove()
        return newest.name
