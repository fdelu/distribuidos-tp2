import logging
import yaml
from typing import Any, cast
from .config import Config

COMPOSE_FILE = "compose.yaml"


def parse_compose() -> dict[str, Any]:
    # get the json of the compose file
    with open(COMPOSE_FILE, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return {}


def make_container_name(proyect_name: str, service_name: str, id: int) -> str:
    # make the name of the container
    return f"{proyect_name}-{service_name}-{id}"


def get_replication_factor(service_compose: Any, config: Config) -> int:
    # get the replication factor of a service
    # if it is not defined, return 1
    try:
        replica_env = cast(str, service_compose["deploy"]["replicas"])
        replica_strip = replica_env.split("{")[1].split("}")[0]
    except Exception as e:
        logging.debug(f"error getting replica factor: {e}")
        return 1
    replica_int = config.get_int(replica_strip)
    return replica_int


def exclude_container(container_name: str, id: int) -> bool:
    # exclude containers that dont send healthchecks
    if "client" in container_name:
        return True
    if "rabbitmq" in container_name:
        return True
    if f"medic-{id}" in container_name:
        return True
    return False


def get_containers(config: Config, id: int) -> list[str]:
    # get the list of containers that need healthchecks
    compose = parse_compose()
    if not compose:
        return []
    containers = []
    for service_name in compose["services"].keys():
        service_compose = compose["services"][service_name]
        replica_factor = get_replication_factor(service_compose, config)
        for i in range(1, replica_factor + 1):
            container_name = make_container_name(compose["name"], service_name, i)
            if exclude_container(container_name, id):
                continue
            containers.append(container_name)
    return containers
