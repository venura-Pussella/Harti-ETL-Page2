import logging
import sys

format_string = "[%(asctime)s: %(levelname)s: %(module)s: %(message)s]"

log_messages = []

# make custom logging.Handler inherited class to store log messages in the log_messages list
class ListHandler(logging.Handler):

    def emit(self, record):
        log_entry = self.format(record)
        log_messages.append(log_entry)


list_handler = ListHandler()


logging.basicConfig(
    level = logging.INFO,
    format = format_string,

    handlers=[
        logging.StreamHandler(sys.stdout),    # To send logs to terminal output
        list_handler
        # logging.FileHandler(log_filepath)
    ]
)



logging.getLogger('azure').setLevel('WARNING') # otherwise Azure info logs are too numerous

