from common.comms_base import SystemCommunicationBase, CommsReceive
from messages.bully_messages import BullyMessage, ElectionMessage, CoordinatorMessage, AnswerMessage
from .config import Config


class SystemCommunication(CommsReceive[BullyMessage], SystemCommunicationBase):

    def _load_definitions(self) -> None:
        pass


class Bully:
    comms: SystemCommunication
    id: int
    medic_scale: int
    is_lider: bool
    current_lider: int
    election_started: bool
    received_awnser: bool

    def __init__(self, config: Config) -> None:
        self.id = config.medic_id
        self.medic_scale = config.medic_scale
        self.is_lider = False
        # self.comms = SystemCommunication(config, self)

    def run(self) -> None:
        pass

    def start_election(self) -> None:
        self.election_started = True
        if self.id == self.medic_scale:
            self.win_election()
        else:
            self.send_election_message()

    def win_election(self) -> None:
        self.is_lider = True
        self.current_lider = self.id
        self.send_coordinator_message()

    def create_routing_key(self, start: int, end: int) -> str:
        routing_key = ""
        for i in range(start, end + 1):
            if i == self.medic_scale:
                continue
            routing_key += str(i) + "."
        return routing_key[:-1]

    def send_election_message(self) -> None:
        # TODO: send a ElectionMessage to all medic with id > self.id
        # also set timer that waits for a AnswerMessage or declare itself as the leader
        self.received_awnser = False
        routing_key = self.create_routing_key(self.id + 1, self.medic_scale)
        pass

    def receive_election_message(self, message: ElectionMessage) -> None:
        self.send_answer_message(message.id)
        if not self.election_started:
            self.start_election()

    def send_answer_message(self, id: int) -> None:
        # TODO: send a AnswerMessage to medic with id = id
        routing_key = self.create_routing_key(id, id)
        pass

    def receive_awnser_message(self, message: AnswerMessage) -> None:
        self.received_awnser = True
        # TODO: set timer that waits for a CoordinatorMessage or restart election
        pass

    def send_coordinator_message(self) -> None:
        # TODO: send a CoordinatorMessage to all medic
        routing_key = self.create_routing_key(1, self.medic_scale)
        pass

    def receive_coordinator_message(self, message: CoordinatorMessage) -> None:
        self.current_lider = message.id_coordinator
        self.election_started = False
        self.is_lider = False
