import logging
import subprocess
from typing import Any
from .config import Config
from common.comms_base.util import set_healthy
from .compose_parser import get_containers
from .system_communication import SystemCommunication
from .messages.bully_messages import (
    BullyMessage,
    ElectionMessage,
    CoordinatorMessage,
    AnswerMessage,
    AliveMessage,
    AliveLeaderMessage,
)
from .leader_heartbeat import LeaderChecker, LeaderHeartbeat


class HealthMonitor:
    comms: SystemCommunication
    is_leader: bool
    started: bool
    id: int
    timer_dict: dict[str, Any]
    container_list: list[str]
    config: Config

    def __init__(self, comms: SystemCommunication, id: int, config: Config) -> None:
        self.comms = comms
        self.is_leader = True
        self.started = False
        self.id = id
        self.timer_dict = {}
        self.container_list = get_containers(config, id)
        self.config = config
        # logging.info(f"container list: {self.container_list}")
        # TODO: maybe auto start main medic

    def start_timers(self) -> None:
        # start a timer for each container
        for container_name in self.container_list:
            self.timer_dict[container_name] = self.comms.set_timer(
                lambda: self.container_dead(container_name),
                self.config.first_heartbeat_timeout
            )
            # time to consider a container dead if no previous heartbeats
            # have been received

    def im_leader(self) -> None:
        if not self.is_leader or not self.started:
            self.is_leader = True
            self.started = True
            self.comms.bind_heartbeat_route()
            self.start_timers()

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
            self.comms.send(AliveMessage(self.comms.name))
            self.comms.set_timer(self.send_heartbeat, self.config.heartbeat_interval)
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
        logging.debug(f"Received heartbeat from {message.container_name}")
        self.restart_container_timer(message.container_name,
                                     self.config.heartbeat_timeout)
        # time to consider a container dead

    def container_dead(self, container_name: str) -> None:
        logging.info(f"Container {container_name} dead")
        subprocess.Popen(["docker", "start", container_name])
        self.restart_container_timer(container_name, self.config.restart_timeout)
        # time to consider a container dead after restarting it


class Bully:
    comms: SystemCommunication
    id: int
    medic_scale: int
    is_leader: bool
    current_leader: int
    election_started: bool
    received_awnser: bool
    awnser_timer_id: Any | None
    coordination_timer_id: Any | None
    leader_checker: LeaderChecker
    leader_heartbeat: LeaderHeartbeat
    health_monitor: HealthMonitor
    config: Config

    timer: Any | None

    def __init__(self, config: Config) -> None:
        self.comms = SystemCommunication(config)
        self.id = int(self.comms.id)
        self.medic_scale = config.medic_scale
        self.is_leader = False
        self.election_started = False
        self.current_leader = -1
        self.coordination_timer_id = None
        self.leader_checker = LeaderChecker(self, self.comms, config)
        self.leader_heartbeat = LeaderHeartbeat(self.comms, self.id, config)
        self.health_monitor = HealthMonitor(self.comms, self.id, config)
        self.awnser_timer_id = None
        self.config = config

    def is_in_election(self) -> bool:
        return self.election_started

    def handle_message(self, message: BullyMessage) -> None:
        message.be_handled_by(self)

    def run(self) -> None:
        logging.info("Starting bully")
        self.start_election()
        self.comms.set_callback(self.handle_message)
        self.comms._start_consuming_from(self.comms.bully_queue)
        set_healthy("HEALTHY")
        self.comms.start_consuming()

    def start_election_no_leader_yet(self) -> None:
        if self.current_leader == -1:
            self.start_election()

    def start_election(self) -> None:
        self.election_started = True
        if self.id == self.medic_scale:
            self.win_election()
        else:
            self.send_election_message()

    def win_election(self) -> None:
        logging.info("I won the election")
        self.is_leader = True
        self.health_monitor.im_leader()
        self.leader_heartbeat.start_hearbeat()
        self.current_leader = self.id
        self.send_coordinator_message()
        self.election_started = False

    def __timer_awnser(self) -> None:
        logging.info("Awnser timeout")
        self.win_election()

    def send_election_message(self) -> None:
        logging.info("Sending election message")
        # sends a ElectionMessage to all medic with id > self.id
        self.comms.send(ElectionMessage(self.id))
        # also set timer that waits for a AnswerMessage or declare itself as the leader
        self.awnser_timer_id = self.comms.set_timer(self.__timer_awnser,
                                                    self.config.awnser_timeout)
        # time to wait for a AnswerMessage or declare itself as the leader
        self.received_awnser = False

    def send_answer_message(self, id: int) -> None:
        # sends a AnswerMessage to medic with id = id
        self.comms.send(AnswerMessage(self.id, id))

    def send_coordinator_message(self) -> None:
        # sends a CoordinatorMessage to all medic
        logging.info("Sending coordinator message")
        self.comms.send(CoordinatorMessage(self.id))

    def handle_election(self, message: ElectionMessage) -> None:
        logging.info(f"Election message received from medic {message.id}")
        self.send_answer_message(message.id)
        if not self.election_started:
            self.start_election()

    def __timer_coordinator(self) -> None:
        logging.info("Coordinator timeout")
        self.start_election()

    def handle_answer(self, message: AnswerMessage) -> None:
        self.received_awnser = True
        if self.awnser_timer_id is not None:
            self.comms.cancel_timer(self.awnser_timer_id)
            self.awnser_timer_id = None
        self.coordination_timer_id = self.comms.set_timer(
            self.__timer_coordinator, self.config.coordinator_timeout)
        # time that waits for a CoordinatorMessage or restart election

    def handle_coordinator(self, message: CoordinatorMessage) -> None:
        logging.info(f"Election won by medic {message.id_coordinator}")
        if self.coordination_timer_id is not None:
            self.comms.cancel_timer(self.coordination_timer_id)
            self.coordination_timer_id = None
        if self.awnser_timer_id is not None:
            self.comms.cancel_timer(self.awnser_timer_id)
            self.awnser_timer_id = None
        self.current_leader = message.id_coordinator
        self.election_started = False
        self.is_leader = False
        self.health_monitor.im_not_leader()
        self.leader_heartbeat.stop_hearbeat()

    def handle_alive_leader(self, message: AliveLeaderMessage) -> None:
        self.leader_checker.handle_heartbeat()

    def handle_alive(self, message: AliveMessage) -> None:
        self.health_monitor.handle_heartbeat(message)
