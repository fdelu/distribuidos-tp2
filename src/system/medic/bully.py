from typing import Any
from common.comms_base import SystemCommunicationBase, CommsReceive, CommsSend
from .messages.bully_messages import (
    BullyMessage,
    ElectionMessage,
    CoordinatorMessage,
    AnswerMessage,
    AliveMessage,
)
from .config import Config
import logging


class SystemCommunication(
    CommsReceive[BullyMessage], CommsSend[BullyMessage], SystemCommunicationBase
):
    EXCHANGE = "medics"
    id: int
    bully_queue: str
    config: Config
    medic_scale: int

    def __init__(self, config: Config, id: int) -> None:
        self.id = id
        self.bully_queue = f"bully_{self.id}"
        self.medic_scale = config.medic_scale
        super().__init__(config)

    def _load_definitions(self) -> None:
        self.channel.queue_declare(queue=self.bully_queue, exclusive=True)
        self.channel.queue_bind(self.bully_queue, self.EXCHANGE, f"#.{self.id}.#")

    def create_routing_key(self, start: int, end: int) -> str:
        routing_key = ""
        for i in range(start, end + 1):
            if i == self.id:
                continue
            routing_key += str(i) + "."
        return routing_key[:-1]

    def _get_routing_details(self, record: BullyMessage) -> tuple[str, str]:
        routing_key = self.create_routing_key(1, self.medic_scale)
        if isinstance(record, ElectionMessage):
            routing_key = self.create_routing_key(self.id + 1, self.medic_scale)
            logging.info(f"Election message to route: {routing_key}")
        elif isinstance(record, CoordinatorMessage):
            routing_key = self.create_routing_key(1, self.medic_scale)
            logging.info(f"Coordinator message to route: {routing_key}")
        elif isinstance(record, AnswerMessage):
            routing_key = self.create_routing_key(
                record.receiver_id, record.receiver_id)
            logging.info(f"Awnser message to route: {routing_key}")
        elif isinstance(record, AliveMessage):
            routing_key = self.create_routing_key(1, self.medic_scale)
            logging.info(f"Alive message to route: {routing_key}")
        return self.EXCHANGE, routing_key


class LeaderChecker:
    comms: SystemCommunication
    bully: Any  # TODO: Bully
    heart_beat_timer_id: Any | None

    def __init__(self, bully: Any, comms: SystemCommunication) -> None:
        self.heart_beat_timer_id = None
        self.bully = bully
        self.comms = comms

    def handle_heartbeat(self) -> None:
        logging.info("Received leader heartbeat")
        if self.heart_beat_timer_id is not None:
            self.comms.cancel_timer(self.heart_beat_timer_id)
        self.heart_beat_timer_id = self.comms.set_timer(self.leader_dead, 5)
        # TODO: may be good to add variance to this time so that not all
        # medics send their start election at the same time

    def leader_dead(self) -> None:
        self.comms.cancel_timer(self.heart_beat_timer_id)
        logging.info("Leader dead")
        if not self.bully.is_in_election():
            self.bully.start_election()


class LeaderHeartbeat:
    send_heartbeat: bool
    comms: SystemCommunication
    id: int

    def __init__(self, comms: SystemCommunication, id: int) -> None:
        self.send_heartbeat = False
        self.comms = comms
        self.id = id

    def start_hearbeat(self) -> None:
        self.send_heartbeat = True
        self.send_heartbeat_message()

    def send_heartbeat_message(self) -> None:
        if self.send_heartbeat:
            logging.info("Sending leader heartbeat")
            self.comms.send(AliveMessage(self.comms.id))
            self.comms.set_timer(self.send_heartbeat_message, 1)

    def stop_hearbeat(self) -> None:
        self.send_heartbeat = False


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

    timer: Any | None

    def __init__(self, config: Config) -> None:
        self.id = config.medic_id
        self.medic_scale = config.medic_scale
        self.is_leader = False
        self.comms = SystemCommunication(config, self.id)
        self.coordination_timer_id = None
        self.leader_checker = LeaderChecker(self, self.comms)
        self.leader_heartbeat = LeaderHeartbeat(self.comms, self.id)

    def is_in_election(self) -> bool:
        return self.election_started

    def handle_message(self, message: BullyMessage) -> None:
        message.be_handled_by(self)

    def run(self) -> None:
        logging.info("Starting bully")
        if self.id == self.medic_scale:
            self.start_election()
        self.comms.set_callback(self.handle_message)
        self.comms._start_consuming_from(self.comms.bully_queue)
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
        self.leader_heartbeat.stop_hearbeat()
        if self.coordination_timer_id is not None:
            self.comms.cancel_timer(self.coordination_timer_id)
            self.coordination_timer_id = None

    def handle_alive(self, message: AliveMessage) -> None:
        self.leader_checker.handle_heartbeat()
