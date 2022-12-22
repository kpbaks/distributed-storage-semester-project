from typing import Any, Iterable, List

YELLOW = "\033[93m"
NC = "\033[0m"  # No Color


STORAGE_MODES: List[str] = [
    "raid1",
    "erasure_coding_rs",
    "erasure_coding_rlnc",
    "fake-hdfs",
]


TOPIC_HEARTBEAT = "heartbeat"

PORT_HEARTBEAT = 5555
