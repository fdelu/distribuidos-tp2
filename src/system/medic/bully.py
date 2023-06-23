import logging
from typing import Any
from .config import Config
from common.comms_base.util import set_healthy
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
from .health_monitor import HealthMonitor


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

    def cancel_awnser_timer(self) -> None:
        if self.awnser_timer_id is not None:
            self.comms.cancel_timer(self.awnser_timer_id)
            self.awnser_timer_id = None

    def handle_answer(self, message: AnswerMessage) -> None:
        self.received_awnser = True
        self.cancel_awnser_timer()
        self.coordination_timer_id = self.comms.set_timer(
            self.__timer_coordinator, self.config.coordinator_timeout)
        # time that waits for a CoordinatorMessage or restart election

    def handle_coordinator(self, message: CoordinatorMessage) -> None:
        logging.info(f"Election won by medic {message.id_coordinator}")
        if self.coordination_timer_id is not None:
            self.comms.cancel_timer(self.coordination_timer_id)
            self.coordination_timer_id = None
        self.cancel_awnser_timer()
        self.current_leader = message.id_coordinator
        self.election_started = False
        self.is_leader = False
        self.health_monitor.im_not_leader()
        self.leader_heartbeat.stop_hearbeat()

    def handle_alive_leader(self, message: AliveLeaderMessage) -> None:
        self.leader_checker.handle_heartbeat()

    def handle_alive(self, message: AliveMessage) -> None:
        self.health_monitor.handle_heartbeat(message)
