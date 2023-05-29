import json
import logging
import signal
from threading import Event
from typing import Any, Iterable
import zmq


from shared.socket import SocketStopWrapper
from shared.messages import RecordType
from shared.messages import Phase, SplitChar, StatType
from .config import Config

TIMEOUT_MILLISECONDS = 1000


class BikeRidesAnalyzer:
    phase: Phase
    config: Config
    interrupted: Event

    context: zmq.Context[zmq.Socket[None]]
    input_socket: SocketStopWrapper
    output_socket: SocketStopWrapper

    def __init__(self, config: Config) -> None:
        self.config = config
        self.phase = Phase.StationsWeather
        self.context = zmq.Context()
        self.context.setsockopt(zmq.LINGER, 0)  # Don't block on close
        self.interrupted = Event()

        input_socket = self.context.socket(zmq.PAIR)
        input_socket.connect(config.input_address)
        self.input_socket = SocketStopWrapper(input_socket, self.interrupted)
        output_socket = self.context.socket(zmq.REQ)
        output_socket.connect(config.output_address)
        self.output_socket = SocketStopWrapper(output_socket, self.interrupted)

    def interrupt_on_signal(self, signum: signal.Signals) -> None:
        signal.signal(signum, lambda *_: self.interrupted.set())

    def send_stations(self, city: str, lines: Iterable[str]) -> None:
        logging.info(f"Sending stations for {city}")
        if self.phase != Phase.StationsWeather:
            raise ValueError(f"Can't send stations in this phase: {self.phase}")

        self.__send_batchs(city, lines, RecordType.STATION)

    def send_weather(self, city: str, lines: Iterable[str]) -> None:
        logging.info(f"Sending weather for {city}")
        if self.phase != Phase.StationsWeather:
            raise ValueError(f"Can't send weather in this phase: {self.phase}")

        self.__send_batchs(city, lines, RecordType.WEATHER)

    def send_trips(self, city: str, lines: Iterable[str]) -> None:
        logging.info(f"Sending trips for {city}")
        if self.phase == Phase.StationsWeather:
            self.phase = Phase.Trips
        if self.phase != Phase.Trips:
            raise ValueError(f"Can't send trips in this phase: {self.phase}")

        self.__send_batchs(city, lines, RecordType.TRIP)

    def get_rain_averages(self) -> dict[str, float]:
        return self.__get_stat(StatType.RAIN)

    def get_year_counts(self) -> dict[str, list[int]]:
        return self.__get_stat(StatType.YEAR)

    def get_city_averages(self) -> dict[str, float]:
        return self.__get_stat(StatType.CITY)

    def close(self) -> None:
        self.input_socket.close()
        self.output_socket.close()
        self.context.term()

    def __get_stat(self, stat: StatType) -> Any:
        if self.phase == Phase.Trips:
            self.input_socket.send(RecordType.END)
            self.phase = Phase.End
        if self.phase != Phase.End:
            raise ValueError(f"Can't get stat in this phase: {self.phase}")

        self.output_socket.send(stat)
        logging.debug(f"Requesting stat {stat}")
        response = self.output_socket.recv()
        logging.debug(f"Stat {stat} received")
        return json.loads(response)

    def __send_batchs(self, city: str, lines: Iterable[str], type: RecordType) -> int:
        count = -1  # don't count the header

        self.input_socket.send(f"{type}{SplitChar.HEADER}{city}")

        for batch in self.__batch(lines):
            count += len(batch)
            self.input_socket.send(SplitChar.RECORDS.join(batch))

        self.input_socket.send(RecordType.END)

        if self.input_socket.recv() != RecordType.ACK:
            raise RuntimeError("Did not receive ACK for this batch")
        logging.info(f"{self.phase} | {city} | Sent {count} {type} records")
        return count

    def __batch(self, lines: Iterable[str]) -> Iterable[list[str]]:
        batch = []
        for line in lines:
            batch.append(line)
            if len(batch) == self.config.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
