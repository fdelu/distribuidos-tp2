from .system_communication import SystemCommunication
from .messages.bully_messages import AliveLeaderMessage
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
        if self.heart_beat_timer_id is not None:
            self.comms.cancel_timer(self.heart_beat_timer_id)
        self.heart_beat_timer_id = self.comms.set_timer(self.leader_dead, 5)
        # TODO: may be good to add variance to this time so that not all
        # medics send their start election at the same time

    def leader_dead(self) -> None:
        # the leader has died, starts an election if it is not already in one
        self.comms.cancel_timer(self.heart_beat_timer_id)
        logging.info("Leader dead")
        if not self.bully.is_in_election():
            self.bully.start_election()
        self.heart_beat_timer_id = None


class LeaderHeartbeat:
    # used by the leader to send heartbeats to the other medics
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
            self.comms.send(AliveLeaderMessage(int(self.comms.id)))
            self.comms.set_timer(self.send_heartbeat_message, 1)
            # time between leader heartbeats

    def stop_hearbeat(self) -> None:
        self.send_heartbeat = False
