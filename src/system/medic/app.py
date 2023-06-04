import logging
import docker
from shared.log import setup_logs
from .config import Config


def main() -> None:
    config = Config()
    setup_logs(config.log_level)
    logging.info("started medic")
    docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')

    container = docker_client.containers.run(
        'distribuidos-tp2-client', detach=True, name='distribuidos-tp2-client')
    print(container.logs().decode('utf-8'))


main()
