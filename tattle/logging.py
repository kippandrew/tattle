from __future__ import absolute_import

import logging
import logging.config

import sys


def get_logger(name, level=None, context=None):
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)
    if context is not None:
        return context(logger)
    return logger


def init_logger():
    # clear existing handlers
    logging._handlers = []
    logger = get_logger(None)
    format = ConsoleFormatter(fmt='[$COLOR%(levelname)s$RESET] [%(asctime)s] [$COLOR%(name)s$RESET] %(message)s')
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(format)
    handler.setLevel(logging.DEBUG)
    logger.setLevel(logging.NOTSET)
    logger.addHandler(handler)
    return logger

class ConsoleFormatter(logging.Formatter):
    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

    COLORS = {
        'WARNING': MAGENTA,
        'INFO': GREEN,
        'DEBUG': WHITE,
        'CRITICAL': RED,
        'ERROR': RED,
        'RED': RED,
        'GREEN': GREEN,
        'YELLOW': YELLOW,
        'BLUE': BLUE,
        'MAGENTA': MAGENTA,
        'CYAN': CYAN,
        'WHITE': WHITE,
    }

    RESET_SEQ = "\033[0m"
    COLOR_SEQ = "\033[%dm"
    BOLD_SEQ = "\033[1m"

    def __init__(self, *args, **kwargs):
        logging.Formatter.__init__(self, *args, **kwargs)

    def format(self, record):
        levelname = record.levelname
        color = self.COLOR_SEQ % (30 + self.COLORS[levelname])
        message = logging.Formatter.format(self, record)
        message = message.replace("$RESET", self.RESET_SEQ) \
            .replace("$BOLD", self.BOLD_SEQ) \
            .replace("$COLOR", color)

        for k, v in self.COLORS.items():
            message = message.replace("$" + k, self.COLOR_SEQ % (v + 30)) \
                .replace("$BG" + k, self.COLOR_SEQ % (v + 40)) \
                .replace("$BG-" + k, self.COLOR_SEQ % (v + 40))
        return message + self.RESET_SEQ
