"""
Task Runner Module

External Dependencies:
- petl : (c) 2012 Alistair Miles - MIT License (https://pypi.python.org/pypi/petl/1.1.0)
- PyMySQL: (c) 2010, 2013 PyMySQL contributors - MIT License (https://pypi.python.org/pypi/PyMySQL/0.7.1)

Features:
- Wrapper to a json task file
- Facade to run the tasks

"""

import json
import os
import time
from . import compat

from .task import TaskFactory, DriverFactory


class Runner(object):
    """Wrapper for json task file"""

    def __init__(self, task):
        if os.path.isfile(task):
            with compat.open(task, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
        else:
            raise ValueError("Task file not found!")

    @staticmethod
    def is_task(filename):
        if filename.endswith(".json") and os.path.isfile(filename):
            with compat.open(filename, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # TODO: maybe a property id? jsonschema?
                return "tasks" in config
        return False

    @property
    def has_schedule(self):
        if "schedule" in self._config:
            recurring = self._config["schedule"].get("recurring", False)
            return recurring
        else:
            return False

    @property
    def schedule(self):
        return self._config["schedule"]

    @property
    def config(self):
        return self._config


class TaskRunner(object):
    """Facade to run the task runner object"""

    def __init__(self, runner):
        self._config = runner.config

    def run(self, log):
        if "tasks" in self._config:
            driver = DriverFactory(self._config)
            for item in self._config["tasks"]:
                start = time.time()
                log.write(u"Executing task item: {}".format(item["name"]))
                if item.get("disabled", False):
                    task = TaskFactory().get_task("nop")
                else:
                    task = TaskFactory().get_task(item["type"])
                task.run(driver, item, log)
                log.write(u"Task item finished: {0}, time: {1:.2f}s".format(item["name"], (time.time() - start)))
            return True
