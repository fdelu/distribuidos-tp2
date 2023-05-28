import json
import logging
import os
import signal
from typing import Any, Iterable
from datetime import datetime

from shared.log import setup_logs

from .config import Config
from .bike_rides_analyzer import BikeRidesAnalyzer


def main() -> None:
    config = Config()
    setup_logs(config.log_level)

    bike_rides_analyzer = BikeRidesAnalyzer(config)
    bike_rides_analyzer.interrupt_on_signal(signal.SIGTERM)
    try:
        get_stats(bike_rides_analyzer, config)
    except (InterruptedError, KeyboardInterrupt):
        logging.info("Interrupted by user")

    bike_rides_analyzer.close()
    logging.info("Exiting gracefully")


def get_stats(bike_rides_analyzer: BikeRidesAnalyzer, config: Config) -> None:
    cities = os.listdir(config.data_path)
    logging.info(f"Using data in {config.data_path}. Cities: {', '.join(cities)}")

    for city in cities:
        path = f"{config.data_path}/{city}"
        bike_rides_analyzer.send_stations(city, line_reader(f"{path}/stations.csv"))
        bike_rides_analyzer.send_weather(city, line_reader(f"{path}/weather.csv"))

    for city in cities:
        path = f"{config.data_path}/{city}"
        bike_rides_analyzer.send_trips(city, line_reader(f"{path}/trips.csv"))

    rain_stats = bike_rides_analyzer.get_rain_averages()
    save_results(config, "rain_stats", rain_stats)
    city_stats = bike_rides_analyzer.get_city_averages()
    save_results(config, "city_stats", city_stats)
    year_stats = bike_rides_analyzer.get_year_counts()
    save_results(config, "year_stats", year_stats)


def line_reader(file_path: str) -> Iterable[str]:
    with open(file_path) as file:
        for line in file:
            yield line.strip()


def save_results(config: Config, name: str, data: Any) -> None:
    with open(f"{config.result_path}/{name}.json", "w") as file:
        file.write(
            json.dumps(
                {"timestamp": f"{datetime.now()}", "data": data},
                indent=2,
                ensure_ascii=False,
            )
        )


main()
