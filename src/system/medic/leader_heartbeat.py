from .system_communication import SystemCommunication
from .messages.bully_messages import AliveMessage
from typing import Any
import logging


class LeaderChecker:
    comms: SystemCommunication
    bully: Any
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
        self.heart_beat_timer_id = None


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
