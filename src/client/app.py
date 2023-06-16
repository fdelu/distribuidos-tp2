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
    job_id = bike_rides_analyzer.job_id
    logging.info(f"Job id: {job_id}")
    bike_rides_analyzer.interrupt_on_signal(signal.SIGTERM)
    try:
        get_stats(bike_rides_analyzer, config, job_id)
    except (InterruptedError, KeyboardInterrupt):
        logging.info("Interrupted by user")

    bike_rides_analyzer.close()
    logging.info("Exiting gracefully")


def get_stats(
    bike_rides_analyzer: BikeRidesAnalyzer, config: Config, job_id: str
) -> None:
    cities = os.listdir(config.data_path)
    logging.info(f"Using data in {config.data_path}. Cities: {', '.join(cities)}")

    for city in cities:
        path = f"{config.data_path}/{city}"
        bike_rides_analyzer.send_stations(city, line_reader(f"{path}/stations.csv"))
        bike_rides_analyzer.send_weather(city, line_reader(f"{path}/weather.csv"))

    for city in cities:
        path = f"{config.data_path}/{city}"
        bike_rides_analyzer.send_trips(city, line_reader(f"{path}/trips.csv"))

    logging.info("All data sent. Waiting for results")
    rain_stats = bike_rides_analyzer.get_rain_averages()
    save_results(config, "rain_stats", job_id, rain_stats)
    city_stats = bike_rides_analyzer.get_city_averages()
    save_results(config, "city_stats", job_id, city_stats)
    year_stats = bike_rides_analyzer.get_year_counts()
    save_results(config, "year_stats", job_id, year_stats)


def line_reader(file_path: str) -> Iterable[str]:
    with open(file_path) as file:
        for line in file:
            yield line.strip()


def save_results(config: Config, name: str, job_id: str, data: Any) -> None:
    full_path = f"{config.result_path}/{job_id}/{name}.json"
    dir = os.path.dirname(full_path)
    if not os.path.exists(dir):
        os.makedirs(dir)

    with open(full_path, "w") as file:
        file.write(
            json.dumps(
                {"timestamp": f"{datetime.now()}", "data": data},
                indent=2,
                ensure_ascii=False,
            )
        )


main()
