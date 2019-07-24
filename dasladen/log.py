"""
Log Module

Features:
- Log to file
- Log to console

"""

import time
from . import compat


class FileHandler(object):
    def __init__(self):
        self.file = None

    def open(self, key):
        self.file = compat.open('log/{}.log'.format(key), 'a', 0)

    def write(self, data):
        self.file.write(u"{} {}\n".format(get_time(), data))

    def close(self):
        self.file.close()


class ConsoleHandler(object):
    def open(self, key):
        pass

    # noinspection PyMethodMayBeStatic
    def write(self, data):
        print(u"{} {}".format(get_time(), data))

    def close(self):
        pass


class LogManager(object):
    def __init__(self):
        self.log_manager_instance = dict()
        self.handlers = [FileHandler()]

    def add(self, key):
        self.log_manager_instance[key] = self.handlers

    def get(self, key):
        return self.log_manager_instance[key]


_manager = LogManager()


def add_log_handler(log_handler):
    _manager.handlers.append(log_handler)


def get_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def get_time_filename():
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


class Logger(object):
    def __init__(self, filename):
        self.key = filename
        _manager.add(filename)

    def write(self, data):
        for handler in _manager.get(self.key):
            handler.write(data)

    def __enter__(self):
        for handler in _manager.get(self.key):
            handler.open(self.key)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for handler in _manager.get(self.key):
            handler.close()


