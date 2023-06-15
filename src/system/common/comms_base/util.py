from .protocol import CommsProtocol


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
