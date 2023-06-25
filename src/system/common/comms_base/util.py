import subprocess
import re
import os

from .protocol import CommsProtocol

HOST_ID_COMMAND = [
    "/bin/bash",
    "-c",
    r'nslookup $(hostname -i | grep -o -E "^[^ ]*") | head -n 1',
]
HOST_ID_REGEX = r" = (?P<name>[^\.]*-(?P<id>\d+))\..*"

STATUS_FILE = os.getenv("STATUS_FILE", "status.txt")


def set_healthy(message: str) -> None:
    with open(STATUS_FILE, "w") as f:
        f.write(message)


def setup_job_queues(
    comms: CommsProtocol, exchange: str, queues: dict[str, list[str]], job_id: str
) -> None:
    """
    Creates the queues and binds them to the exchange with the routing keys.
    Formats all the names with the job_id.
    """
    for name, r_keys in queues.items():
        queue = name.format(job_id=job_id)
        comms.channel.queue_declare(queue)
        for routing_key_fmt in r_keys:
            comms.channel.queue_bind(
                queue,
                exchange,
                routing_key_fmt.format(job_id=job_id),
            )


def get_host_data() -> tuple[str, str]:
    """
    Gets this node's host name and id
    """
    p = subprocess.run(HOST_ID_COMMAND, stdout=subprocess.PIPE)
    res = p.stdout.decode()
    groups = re.search(HOST_ID_REGEX, res)
    if not groups:
        raise RuntimeError("Could not find host data in output")
    return groups["name"], groups["id"]
