
""" This file contains some utils for connecting to Logentries
    as well as storing logs in a queue and sending them."""

VERSION = '2.0.2'

from logentries import helpers as le_helpers

import json
import logging
import threading
import socket
import sys
import random
import time

# Size of the internal event queue
QUEUE_SIZE = 32768
# Logentries API server address
LE_API = "api.logentries.com"
# Port number for token logging to Logentries API server
LE_PORT = 10000
# Minimal delay between attempts to reconnect in seconds
MIN_DELAY = 0.1
# Maximal delay between attempts to recconect in seconds
MAX_DELAY = 10
# LE appender signature - used for debugging messages
LE = "LE: "
# Error message displayed when an incorrect Token has been detected
INVALID_TOKEN = ("\n\nIt appears the LOGENTRIES_TOKEN "
                 "parameter you entered is incorrect!\n\n")
# Unicode Line separator character   \u2028
LINE_SEP = le_helpers.to_unicode('\u2028')
# Library Identifier to be sent to server to identify python lib
LIBRARY_IDENTIFIER = "###P01### - Library Initialised"


def dbg(msg):
    print(LE + msg)


class SocketAppender(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = True
        self._conn = None
        self._queue = le_helpers.create_queue(QUEUE_SIZE)

    def openConnection(self):
        self._conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._conn.connect((LE_API, LE_PORT))

    def reopenConnection(self):
        self.closeConnection()

        root_delay = MIN_DELAY
        while True:
            try:
                self.openConnection()
                return
            except Exception:
                dbg("Unable to connect to Logentries")

            root_delay *= 2
            if(root_delay > MAX_DELAY):
                root_delay = MAX_DELAY

            wait_for = root_delay + random.uniform(0, root_delay)

            try:
                time.sleep(wait_for)
            except KeyboardInterrupt:
                raise 

    def closeConnection(self):
        if self._conn is not None:
            self._conn.close()

    def run(self):
        try:
            # Open connection
            self.reopenConnection()

            # Send data in queue
            while True:
                # Take data from queue
                data = self._queue.get(block=True)

                # Replace newlines with Unicode line separator
                # for multi-line events
                if not le_helpers.is_unicode(data):
                    multiline = le_helpers.create_unicode(data).replace(
                        '\n', LINE_SEP)
                else:
                    multiline = data.replace('\n', LINE_SEP)
                multiline += "\n"
                # Send data, reconnect if needed
                while True:
                    try:
                        self._conn.send(multiline.encode('utf-8'))
                    except socket.error:
                        self.reopenConnection()
                        continue
                    break
        except KeyboardInterrupt:
            dbg("Logentries asynchronous socket client interrupted")

        self.closeConnection()


class LogentriesHandler(logging.Handler):
    def __init__(self, token):
        logging.Handler.__init__(self)
        self.token = token
        self.good_config = True
        if not le_helpers.check_token(token):
            dbg(INVALID_TOKEN)
            self.good_config = False
        format = logging.Formatter('%(asctime)s : %(levelname)s, %(message)s',
                                   '%a %b %d %H:%M:%S %Z %Y')
        self.setFormatter(format)
        self.setLevel(logging.DEBUG)
        self._thread = SocketAppender()
        # Add idenfiter to queue to be sent first on startup
        self._thread._queue.put(self.token + LIBRARY_IDENTIFIER + '\n')

    @property
    def _started(self):
        return self._thread.is_alive()

    def emit(self, record):
        if not self._started and self.good_config:
            dbg("Starting Logentries Asynchronous Socket Appender")
            self._thread.start()

        msg = self.format(record).rstrip('\n')
        msg = self.token + msg

        self._thread._queue.put(msg)

    def close(self):
        logging.Handler.close(self)


class LogentriesFormatter(logging.Formatter):
    def __init__(self):
        self.host = socket.gethostname()

    def format(self, record):
        message = {
            'message': record.getMessage(),
            'host': self.host,
            'path': record.pathname,
            'levelname': record.levelname,
            'logger': record.name
        }

        message.update(self.get_extra_fields(record))

        if record.exc_info:
            message.update(self.get_debug_fields(record))

        return json.dumps(message)

    def get_extra_fields(self, record):
        # The list contains all the attributes listed in
        # http://docs.python.org/library/logging.html#logrecord-attributes
        skip_list = (
            'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
            'funcName', 'id', 'levelname', 'levelno', 'lineno', 'module',
            'msecs', 'msecs', 'message', 'msg', 'name', 'pathname', 'process',
            'processName', 'relativeCreated', 'thread', 'threadName', 'extra'
        )

        if sys.version_info < (3, 0):
            easy_types = (basestring, bool, dict, float, int, list, type(None))
        else:
            easy_types = (str, bool, dict, float, int, list, type(None))

        fields = {}

        for key, value in record.__dict__.items():
            if key not in skip_list:
                if isinstance(value, easy_types):
                    fields[key] = value
                else:
                    fields[key] = repr(value)

        return fields

    def get_debug_fields(self, record):
        fields = {
            'exc_info': self.formatException(record.exc_info),
            'lineno': record.lineno,
            'process': record.process,
            'threadName': record.threadName,
        }

        # funcName was added in 2.5
        if not getattr(record, 'funcName', None):
            fields['funcName'] = record.funcName

        # processName was added in 2.6
        if not getattr(record, 'processName', None):
            fields['processName'] = record.processName

        return fields
