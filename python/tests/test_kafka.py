import re
from pathlib import Path

from velograph.common.kafka import Topics

REPO = Path(__file__).resolve().parents[2]


def test_python_topic_names_match_the_bootstrap_script():
    script = (REPO / "infra" / "kafka" / "create-topics.sh").read_text(encoding="utf-8")
    created = set(re.findall(r"^create (\S+)", script, flags=re.MULTILINE))
    assert created == set(Topics.ALL)
    assert all("_" not in t for t in created)  # '.' and '_' collide in Kafka metric names
