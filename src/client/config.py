import configparser


class Config:
    batch_size: int
    input_address: str
    output_address: str
    data_path: str
    result_path: str
    log_level: str | None

    SECTION = "client"

    def __init__(self):
        parser = configparser.ConfigParser()
        parser.read("/config.ini")
        self.batch_size = parser.getint(self.SECTION, "BatchSize")
        self.input_address = parser.get(self.SECTION, "InputAddress")
        self.log_level = parser.get(self.SECTION, "LogLevel", fallback=None)
        self.output_address = parser.get(self.SECTION, "OutputAddress")
        self.data_path = parser.get(self.SECTION, "DataPath")
        self.result_path = parser.get(self.SECTION, "ResultsPath")
