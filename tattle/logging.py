import contextlib
import logging
import logging.config

import sys

TRACE, DEBUG, INFO, WARN, ERROR, NOTSET = 5, logging.DEBUG, logging.INFO, logging.WARN, logging.ERROR, logging.NOTSET


class TraceLogger(logging.getLoggerClass()):
    def __init__(self, name, level=logging.NOTSET):
        super(TraceLogger, self).__init__(name, level)

    def trace(self, msg, *args, **kwargs):
        if self.isEnabledFor(TRACE):
            self._log(TRACE, msg, args, **kwargs)


logging.addLevelName(TRACE, "TRACE")
logging.setLoggerClass(TraceLogger)


def get_logger(name, level=None):
    """
    Return a logger with the given name
    :param name:
    :param level:
    :return: logger
    :rtype: TraceLogger
    """
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)
    return logger


def init_logger(level=logging.DEBUG):
    # clear existing handlers
    logging._handlers = []

    # configure root logger
    logger = logging.getLogger()
    formatter = ConsoleLogFormatter('[%(context)s] [%(asctime)s] [$LEVEL%(name)s$RESET] %(message)s')
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.addFilter(LogContextFilter())
    handler.setFormatter(formatter)
    handler.setLevel(level)
    logger.setLevel(logging.NOTSET)
    logger.addHandler(handler)
    return logger


# noinspection PyPep8Naming
def LogContext(name):
    """Factory to create LogContext context managers"""

    @contextlib.contextmanager
    def _context_manager():
        LogContextFilter.context = name
        yield
        LogContextFilter.context = None

    return _context_manager


class LogContextFilter(logging.Filter):
    context = None

    def filter(self, record):
        record.context = self.context
        return True


class ConsoleLogFormatter(logging.Formatter):
    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, LIGHT_GRAY = range(30, 38)
    DARK_GRAY, LIGHT_RED, LIGHT_GREEN, LIGHT_YELLOW, LIGHT_BLUE, LIGHT_MAGENTA, LIGHT_CYAN, WHITE = range(90, 98)

    LEVELS = {
        'TRACE': DARK_GRAY,
        'DEBUG': LIGHT_GRAY,
        'INFO': GREEN,
        'WARNING': MAGENTA,
        'CRITICAL': RED,
        'ERROR': RED,
    }

    RESET_SEQ = "\033[0m"
    COLOR_SEQ = "\033[%dm"
    BOLD_SEQ = "\033[1m"

    def __init__(self, fmt):
        logging.Formatter.__init__(self, fmt)

    def format(self, record):
        levelname = record.levelname
        level_color = self.COLOR_SEQ % (self.LEVELS[levelname])
        message = logging.Formatter.format(self, record)
        message = message.replace("$RESET", self.RESET_SEQ)
        message = message.replace("$BOLD", self.BOLD_SEQ)
        message = message.replace("$LEVEL", level_color)
        return message + self.RESET_SEQ
