import logging
import docker
from shared.log import setup_logs
from .config import Config
import yaml
from docker.types import Mount
import os
from typing import Any, Optional, cast


def parse_compose() -> dict[str, Any]:
    with open("compose.yaml", "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return {}


def make_abs_path(path: str, abs_path: str) -> str:
    return f"{abs_path}/{path}"


def create_mount(image_compose: Any, abs_path: str) -> list[Mount]:
    return [Mount(target=mount["target"],
            source=make_abs_path(mount["source"], abs_path),
            type=mount["type"])
            for mount in image_compose["volumes"]]


def make_image_name(proyect_name: str, service_name: str) -> str:
    return f"{proyect_name}-{service_name}"


def main() -> None:
    config = Config()
    setup_logs(config.log_level)
    abs_path = cast(str, os.environ.get("ABS_PATH"))
    logging.info(f"started medic{abs_path}")
    compose = parse_compose()
    if not compose:
        logging.error("compose.yaml couldnt be parsed")
        return

    docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')

    mount = create_mount(compose["services"]["client"], abs_path)
    container = docker_client.containers.run(
        'distribuidos-tp2-client', detach=True, name='distribuidos-tp2-client',
        mounts=mount
    )
    print(container.logs().decode('utf-8'))


main()
