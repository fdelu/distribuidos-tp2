import logging
from typing import Any
from .config import Config
import subprocess
from .messages.bully_messages import AliveMessage
from .compose_parser import get_containers
from .system_communication import SystemCommunication


class HealthMonitor:
    comms: SystemCommunication
    is_leader: bool
    started: bool
    id: int
    timer_dict: dict[str, Any]
    container_list: list[str]
    alive_message: AliveMessage

    def __init__(self, comms: SystemCommunication) -> None:
        self.comms = comms
        self.is_leader = True
        self.started = False
        self.id = int(comms.id)
        self.timer_dict = {}
        self.container_list = get_containers(self.id)
        self.alive_message = AliveMessage(self.comms.name)
        # TODO: maybe auto start main medic

    def start_timers(self) -> None:
        # start a timer for each container
        for container_name in self.container_list:
            self.restart_container_timer(
                container_name,
                Config().first_heartbeat_timeout,
            )
            # time to consider a container dead if no previous heartbeats
            # have been received

    def im_leader(self) -> None:
        if not self.is_leader or not self.started:
            self.is_leader = True
            self.started = True
            self.start_timers()
            self.comms.bind_heartbeat_route()

    def stop_timers(self) -> None:
        for timer in self.timer_dict.values():
            self.comms.cancel_timer(timer)

    def im_not_leader(self) -> None:
        if self.is_leader:
            self.is_leader = False
            self.comms.unbind_heartbeat_route()
            self.stop_timers()
            self.start_heartbeat()

    def send_heartbeat(self) -> None:
        if not self.is_leader:
            self.comms.send(self.alive_message)
            self.comms.set_timer(self.send_heartbeat, Config().heartbeat_interval)
            # time between heartbeats

    def start_heartbeat(self) -> None:
        self.send_heartbeat()

    def restart_container_timer(self, container_name: str, time: int) -> None:
        if container_name in self.timer_dict:
            self.comms.cancel_timer(self.timer_dict[container_name])
        self.timer_dict[container_name] = self.comms.set_timer(
            lambda: self.container_dead(container_name), time
        )

    def handle_heartbeat(self, message: AliveMessage) -> None:
        # logging.debug(f"Received heartbeat from {message.container_name}")
        self.restart_container_timer(message.container_name, Config().heartbeat_timeout)
        # time to consider a container dead

    def container_dead(self, container_name: str) -> None:
        logging.info(f"Container {container_name} timed out, starting it")
        subprocess.Popen(["docker", "start", container_name], stdout=subprocess.DEVNULL)
        self.restart_container_timer(container_name, Config().restart_timeout)
        # time to consider a container dead after restarting it
