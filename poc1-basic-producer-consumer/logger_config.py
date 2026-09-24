import logging

# Define a constant for log level
LOG_LEVEL = logging.INFO

# Define the log format string
LOG_FORMATTER = '%(asctime)s - %(levelname)s - %(request_id)s %(filename)s:%(lineno)s - %(funcName)2s() : %(message)s\n'


class MyLogger:

    @staticmethod
    def get_logger(logger_name=None):
        """
        Get a logger instance for logging.

        Args:
            logger_name (str, optional): The name of the logger. Defaults to None.

        Returns:
            logging.Logger: A logger instance.
        """
        logging.basicConfig()
        logger = logging.getLogger(logger_name)
        logger.setLevel(LOG_LEVEL)

        return logger
