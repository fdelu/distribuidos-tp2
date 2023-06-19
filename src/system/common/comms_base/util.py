import subprocess
import re
import logging

from .protocol import CommsProtocol

HOST_ID_COMMAND = [
    "/bin/bash",
    "-c",
    r'nslookup $(hostname -i | grep -o -E "^[^ ]*") | head -n 1',
]
HOST_ID_REGEX = r" = [^\.]*-(\d+)\..*"


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


def get_host_id() -> str:
    """
    Gets this node's host id
    """
    p = subprocess.run(HOST_ID_COMMAND, stdout=subprocess.PIPE)
    res = p.stdout.decode()
    id_match = re.search(HOST_ID_REGEX, res)
    logging.critical(res)
    if id_match is None:
        raise RuntimeError("Could not find ID in output")
    return id_match[1]
