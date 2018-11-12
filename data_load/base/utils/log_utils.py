import logging
import datetime
import time

LOG_LEVEL_FATAL = -2
LOG_LEVEL_ERROR = -1
LOG_LEVEL_WARNING = 0
LOG_LEVEL_INFO = 1
LOG_LEVEL_DEBUG = 2
LOG_LEVEL_TRACE = 3


def create_logger(logger_name, log_files_directory):
    now = datetime.datetime.now()
    local_date = now.strftime("%m-%d-%Y_%H%M_%S")

    # create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # logger.setLevel(logging.DEBUG)
    # create file handler and set level to debug
    log_filename = log_files_directory + '/' + logger_name + '_' + str(local_date) + '.log'
    print 'Log file:', log_filename
    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.INFO)
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    # add formatter to ch
    fh.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(fh)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)

    return logger