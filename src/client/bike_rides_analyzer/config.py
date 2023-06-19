from typing import Protocol


class BikeRidesAnalyzerConfig(Protocol):
    batch_size: int
    input_address: str
    output_address: str
