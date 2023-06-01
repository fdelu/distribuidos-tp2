import logging
import docker
from shared.log import setup_logs
from .config import Config


def main() -> None:
    config = Config()
    setup_logs(config.log_level)
    logging.info("started medic")
    docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    container_list = docker_client.containers.list()
    container_names = [container.name for container in container_list]
    logging.info(f"containers: {container_names}")


main()
