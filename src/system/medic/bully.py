from typing import Any
from .messages.bully_messages import (
    BullyMessage,
    ElectionMessage,
    CoordinatorMessage,
    AnswerMessage,
    AliveMessage,
    AliveLeaderMessage,
)
from .leader_heartbeat import LeaderChecker, LeaderHeartbeat
from .config import Config
import logging
from .system_communication import SystemCommunication
import subprocess
from common.comms_base.util import set_healthy


class HealthMonitor:
    comms: SystemCommunication
    is_leader: bool
    id: int
    timer_dict: dict[str, Any]

    def __init__(self, comms: SystemCommunication, id: int) -> None:
        self.comms = comms
        self.is_leader = True
        self.id = id
        self.timer_dict = {}

    def im_leader(self) -> None:
        self.is_leader = True
        self.comms.bind_heartbeat_route()

    def im_not_leader(self) -> None:
        if self.is_leader:
            self.is_leader = False
            self.comms.unbind_heartbeat_route()
            self.start_heartbeat()

    def send_heartbeat(self) -> None:
        if not self.is_leader:
            logging.info("Sending heartbeat")
            self.comms.send(AliveMessage(f"distribuidos-tp2-medic{self.id}-1"))
            self.comms.set_timer(self.send_heartbeat, 1)

    def start_heartbeat(self) -> None:
        self.comms.set_timer(self.send_heartbeat, 1)

    def handle_heartbeat(self, message: AliveMessage) -> None:
        logging.info(f"Received heartbeat from {message.container_name}")
        if message.container_name in self.timer_dict:
            self.comms.cancel_timer(self.timer_dict[message.container_name])
        self.timer_dict[message.container_name] = self.comms.set_timer(
            lambda: self.container_dead(message.container_name), 5)

    def container_dead(self, container_name: str) -> None:
        logging.info(f"Container {container_name} dead")
        result = subprocess.run(['docker', 'start', container_name],
                                check=False, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        # TODO: check if can do this async and check errors
        logging.info(f"Command executed. Result={result.returncode}. Output={result.stdout}. Error={result.stderr}")


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

    timer: Any | None

    def __init__(self, config: Config) -> None:
        self.id = config.medic_id
        self.medic_scale = config.medic_scale
        self.is_leader = False
        self.election_started = False
        self.comms = SystemCommunication(config, self.id)
        self.coordination_timer_id = None
        self.leader_checker = LeaderChecker(self, self.comms)
        self.leader_heartbeat = LeaderHeartbeat(self.comms, self.id)
        self.health_monitor = HealthMonitor(self.comms, self.id)

    def is_in_election(self) -> bool:
        return self.election_started

    def handle_message(self, message: BullyMessage) -> None:
        message.be_handled_by(self)

    def run(self) -> None:
        logging.info("Starting bully")
        if self.id == self.medic_scale:
            self.start_election()
            # TODO: add timeout for receiving coordinator message
        self.comms.set_callback(self.handle_message)
        self.comms._start_consuming_from(self.comms.bully_queue)
        set_healthy("HEALTHY")
        self.comms.start_consuming()

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
        self.election_started = False  # TODO: set timer for this

    def __timer_awnser(self) -> None:
        logging.info("Awnser timeout")
        self.win_election()

    def send_election_message(self) -> None:
        logging.info("Sending election message")
        # sends a ElectionMessage to all medic with id > self.id
        self.comms.send(ElectionMessage(self.id))
        # also set timer that waits for a AnswerMessage or declare itself as the leader
        self.awnser_timer_id = self.comms.set_timer(self.__timer_awnser, 10)
        self.received_awnser = False

    def send_answer_message(self, id: int) -> None:
        # sends a AnswerMessage to medic with id = id
        self.comms.send(AnswerMessage(self.id, id))

    def send_coordinator_message(self) -> None:
        # sends a CoordinatorMessage to all medic
        logging.info("Sending coordinator message")
        self.comms.send(CoordinatorMessage(self.id))

    def handle_election(self, message: ElectionMessage) -> None:
        logging.info(f"Election message received from medic {self.election_started}")
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
        self.coordination_timer_id = self.comms.set_timer(self.__timer_coordinator, 10)
        # sets timer that waits for a CoordinatorMessage or restart election

    def handle_coordinator(self, message: CoordinatorMessage) -> None:
        logging.info(f"Election won by medic {message.id_coordinator}")
        self.current_leader = message.id_coordinator
        self.election_started = False
        self.is_leader = False
        self.health_monitor.im_not_leader()
        self.leader_heartbeat.stop_hearbeat()
        if self.coordination_timer_id is not None:
            self.comms.cancel_timer(self.coordination_timer_id)
            self.coordination_timer_id = None

    def handle_alive_leader(self, message: AliveLeaderMessage) -> None:
        self.leader_checker.handle_heartbeat()

    def handle_alive(self, message: AliveMessage) -> None:
        self.health_monitor.handle_heartbeat(message)
