import logging
from datetime import datetime

class LoggerSetup:
    def __init__(self):
        self.info_logger = self.setup_logger('info', 'process.log', logging.INFO)
        self.error_logger = self.setup_logger('error', 'error.log', logging.ERROR)

    def setup_logger(self, name, log_file, level):

        handler = logging.FileHandler(log_file)

        logger_instance = logging.getLogger(name)
        logger_instance.setLevel(level)
        logger_instance.addHandler(handler)

        return logger_instance

    def current_datetime(self):
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
        return dt_string

    def logInfo(self, message: str):
        self.info_logger.info(self.current_datetime() + ' - ' + message)
        print(message)
        return

    def logError(self, message: str):
        self.error_logger.error(self.current_datetime() + ' - ' + message)
        return

logger = LoggerSetup()