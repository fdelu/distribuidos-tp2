import logging
import docker
from shared.log import setup_logs
from .config import Config
import yaml
from docker.types import Mount
from typing import Any, Optional, cast
import subprocess


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


def get_replication_factor(service_compose: Any, config: Config) -> Optional[int]:
    try:
        replica_env = cast(str, service_compose["deploy"]["replicas"])
        replica_strip = replica_env.split("{")[1].split("}")[0]
        replica_int = int(config.get_env(replica_strip))
        return replica_int
    except Exception as e:
        logging.error(f"error getting replica factor, {replica_strip}: {e}")
        return 1


def main() -> None:
    config = Config()
    setup_logs(config.log_level)
    logging.info("started medic")
    # compose = parse_compose()
    # if not compose:
    #     logging.error("compose.yaml couldnt be parsed")
    #     return

    # docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')

    # mount = create_mount(compose["services"]["year-joiners"], config.abs_path)
    # name = make_image_name(compose["name"], "year-joiners")
    # replication = get_replication_factor(compose["services"]["year-joiners"], config)
    # logging.info(f"about to run container: {replication}")
    # container = docker_client.containers.run(
    #     name, detach=True, name=name,
    #     mounts=mount
    # )
    # print(container.logs().decode('utf-8'))
    # logging.info("finished running container")
    result = subprocess.run(['docker', 'start', 'caca'],
                            check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logging.info(f"Command executed. Result={result.returncode}. Output={result.stdout}. Error={result.stderr}")


main()
