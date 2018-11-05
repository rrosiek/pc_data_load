import time
import sys
import file_utils

LOG_LEVEL_FATAL = -2
LOG_LEVEL_ERROR = -1
LOG_LEVEL_WARNING = 0
LOG_LEVEL_INFO = 1
LOG_LEVEL_DEBUG = 2
LOG_LEVEL_TRACE = 3


current_milli_time = lambda: int(round(time.time() * 1000))


class Logger(object):

    def __init__(self, data_source_name):
        self.current_log_level = LOG_LEVEL_TRACE
        self.data_source_name = data_source_name

        self.last_time_stamp = 0
        self.log_delay = 1500  # milliseconds

        self.log_files_directory = 'logs'
        self.logs = []

    def log(self, log_level, *message):
        if log_level <= LOG_LEVEL_INFO or self.current_log_level >= LOG_LEVEL_TRACE or (current_milli_time() - self.last_time_stamp) >= self.log_delay:
            if self.current_log_level >= log_level:
                print '[' + self.data_source_name + ']', ' '.join((str(msg) for msg in message))

            self.last_time_stamp = current_milli_time()

    def log_replace(self, replace, log_level, *message):
        if log_level <= LOG_LEVEL_INFO or self.current_log_level >= LOG_LEVEL_TRACE or (current_milli_time() - self.last_time_stamp) >= self.log_delay:
            if self.current_log_level >= log_level:
                # sys.stdout.write("\033[F") # Cursor up one line
                print '[' + self.data_source_name + ']', ' '.join((str(msg) for msg in message))

            self.last_time_stamp = current_milli_time()

    def dump_to_file(self):
        logs_directory = self.log_files_directory + '/' + self.data_source_name
        file_utils.make_directory(logs_directory)

        