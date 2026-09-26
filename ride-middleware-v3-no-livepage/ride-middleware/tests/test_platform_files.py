"""Platform files (Person 1): the scripts and configs that `docker compose up` depends on."""
import re
import subprocess
from pathlib import Path

import yaml

from common.topics import ALL_TOPICS

ROOT = Path(__file__).resolve().parents[1]


def test_create_topics_script_is_valid_bash_and_creates_exactly_our_topics(tmp_path):
    script = ROOT / "infra/kafka/create_topics.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)                   # syntax
    fake = tmp_path / "kafka-topics.sh"                                        # run it with a fake kafka CLI
    fake.write_text('#!/bin/bash\nfor a in "$@"; do [ "$prev" = "--topic" ] && echo "CREATE $a"; prev=$a; done\n')
    fake.chmod(0o755)
    body = script.read_text().replace("KT=/opt/kafka/bin/kafka-topics.sh", f"KT={fake}")
    out = subprocess.run(["bash", "-c", body], capture_output=True, text=True, check=True).stdout
    created = re.findall(r"^CREATE (\S+)$", out, re.M)
    assert sorted(created) == sorted(ALL_TOPICS)


def test_compose_services_all_hardened():
    """Every Python service inherits the hardened base (non-root, read-only, no-new-privileges)."""
    for f in ["db/compose.yml", "services/producer/compose.yml", "services/validator/compose.yml",
              "services/consumer/compose.yml", "services/simulator/compose.yml"]:
        for name, svc in yaml.safe_load((ROOT / f).read_text())["services"].items():
            if "image" in svc and "ride-middleware" not in svc["image"]:
                continue                                                       # third-party (postgres)
            assert svc.get("extends", {}).get("service") == "app", f"{f}:{name} must extend the hardened base"
    base = yaml.safe_load((ROOT / "infra/compose/app.yml").read_text())["services"]["app"]
    assert base["user"] == "10001:10001" and base["read_only"] is True and base["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in base["security_opt"] and base["restart"] == "unless-stopped"


def test_no_real_secrets_in_example_env():
    env = (ROOT / ".env.example").read_text()
    for line in env.splitlines():
        if line.startswith(("REDIS_PASSWORD", "RIDER_API_KEY", "DRIVER_API_KEY", "ADMIN_API_KEY",
                            "GRAFANA_ADMIN_PASSWORD")):
            assert "change-me" in line
    assert ".env" in (ROOT / ".gitignore").read_text().split()

