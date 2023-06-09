from typing import Any
from common.comms_base import SystemCommunicationBase, CommsReceive, CommsSend
from messages.bully_messages import (
    BullyMessage,
    ElectionMessage,
    CoordinatorMessage,
    AnswerMessage,
    AliveMessage,
)
from .config import Config


class SystemCommunication(
    CommsReceive[BullyMessage], CommsSend[BullyMessage], SystemCommunicationBase
):
    EXCHANGE = "medics"
    id: int
    bully_queue: str
    config: Config

    def __init__(self, config: Config, id: int) -> None:
        self.id = id
        self.bully_queue = f"bully_{self.id}"
        super().__init__(config)

    def _load_definitions(self) -> None:
        self.ch.queue_declare(queue=self.bully_queue, exclusive=True)
        self.ch.queue_bind(self.bully_queue, self.EXCHANGE, f"#.{self.id}.#")

    def create_routing_key(self, start: int, end: int) -> str:
        routing_key = ""
        for i in range(start, end + 1):
            if i == self.medic_scale:
                continue
            routing_key += str(i) + "."
        return routing_key[:-1]

    def _get_routing_details(self, record: BullyMessage) -> tuple[str, str]:
        routing_key = self.create_routing_key(self.id + 1, self.config.medic_scale)
        return self.EXCHANGE, routing_key


class Bully:
    comms: SystemCommunication
    id: int
    medic_scale: int
    is_leader: bool
    current_leader: int
    election_started: bool
    received_awnser: bool

    timer: Any | None

    def __init__(self, config: Config) -> None:
        self.id = config.medic_id
        self.medic_scale = config.medic_scale
        self.is_leader = False
        self.comms = SystemCommunication(config, self.id)
        self.comms.set_callback(self.handle_message)

    def handle_message(self, message: BullyMessage) -> None:
        message.be_handled_by(self)

    def run(self) -> None:
        pass

    def start_election(self) -> None:
        self.election_started = True
        if self.id == self.medic_scale:
            self.win_election()
        else:
            self.send_election_message()

    def win_election(self) -> None:
        self.is_leader = True
        self.current_leader = self.id
        self.send_coordinator_message()

    def send_election_message(self) -> None:
        self.comms.send(ElectionMessage(self.id))
        # TODO: send a ElectionMessage to all medic with id > self.id
        # also set timer that waits for a AnswerMessage or declare itself as the leader
        self.received_awnser = False
        # routing_key = self.create_routing_key(self.id + 1, self.medic_scale)
        pass

    def send_answer_message(self, id: int) -> None:
        # TODO: send a AnswerMessage to medic with id = id
        # routing_key = self.create_routing_key(id, id)
        self.comms.set_timer(self.__timer_func, 10)
        pass

    def __timer_func(self) -> None:
        print("hola")
        self.comms.set_timer(self.__timer_func, 10)

    def send_coordinator_message(self) -> None:
        # TODO: send a CoordinatorMessage to all medic
        # routing_key = self.create_routing_key(1, self.medic_scale)
        pass

    def handle_election(self, message: ElectionMessage) -> None:
        self.send_answer_message(message.id)
        if not self.election_started:
            self.start_election()

    def handle_answer(self, message: AnswerMessage) -> None:
        self.received_awnser = True
        # TODO: set timer that waits for a CoordinatorMessage or restart election
        pass

    def handle_coordinator(self, message: CoordinatorMessage) -> None:
        self.current_leader = message.id_coordinator
        self.election_started = False
        self.is_leader = False

    def handle_alive(self, message: AliveMessage) -> None:
        pass
