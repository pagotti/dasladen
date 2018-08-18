"""
Processor Module
Processor classes for process files in capture folder

External Dependencies:
- schedule : (c) 2013 Daniel Bader (http://dbader.org) - MIT License (https://pypi.python.org/pypi/schedule)
- backports.tempfile: (c) Pi Delport - Python License (https://github.com/pjdelport/backports.tempfile)

Features:
- UnZip: Open zip files and process then in isolated folder
- Extract: Extract zip file tht contains task file and others files (mostly .csv) that form a task package
- Copy: Move the files to appropriated folders before execution
- Run: Open task file and checks if schedule or run the tasks
- Schedule task to later execution and recurring execution
- Capture new files on capture folder
- Process captured files

"""

import traceback
import zipfile
import schedule

from shutil import copy2
from backports import tempfile
from log import Logger, get_time_filename
from taskrun import *


class SchedulerJob(object):
    """Job information for scheduler processor"""

    def __init__(self, manager, filename, once):
        self.manager = manager
        self.once = once
        self.filename = filename

    def check(self):
        with Logger("scheduler_{}_{}".format(self.filename, get_time_filename())) as log:
            start = time.time()
            try:
                log.write("Executing Scheduled Tasks: {}".format(self.filename))
                self.manager.run(log)
            except StandardError:
                log.write("Error: {}".format(traceback.format_exc()))
            finally:
                log.write("Finished: {0}, elapsed: {1:.2f}s".format(self.filename, (time.time() - start)))
                if self.once:
                    return schedule.CancelJob


class Scheduler(object):
    """Create a schedule for late task execution based on schedule section of task file"""

    @staticmethod
    def run(job_item, job, at_time=""):
        if at_time:
            job.at(at_time).do(job_item.check)
        else:
            job.do(job_item.check)

    @staticmethod
    def enqueue(runner, filename):
        result = ""
        # get schedule info in task file
        props = runner.schedule()
        recurring = props.get("recurring", False)
        manager = TaskRunner(runner)
        job_item = SchedulerJob(manager, filename, not recurring)
        at_time = props.get("time", "")

        if recurring:
            freq = props.get("frequency", "daily")
            if freq == "daily":
                days = props.get("days", 1)
                job = schedule.every(days).day
                Scheduler.run(job_item, job, at_time)
                result = "each {} day(s)".format(days)

            elif freq == "weekly":
                weeks = props.get("weeks", 1)
                result = "each {} week(s) on {}".format(weeks, props["weekday"])

                if "monday" in props["weekday"]:
                    job = schedule.every(weeks).monday
                    Scheduler.run(job_item, job, at_time)

                if "tuesday" in props["weekday"]:
                    job = schedule.every(weeks).tuesday
                    Scheduler.run(job_item, job, at_time)

                if "wednesday" in props["weekday"]:
                    job = schedule.every(weeks).wednesday
                    Scheduler.run(job_item, job, at_time)

                if "thursday" in props["weekday"]:
                    job = schedule.every(weeks).thursday
                    Scheduler.run(job_item, job, at_time)

                if "friday" in props["weekday"]:
                    job = schedule.every(weeks).friday
                    Scheduler.run(job_item, job, at_time)

                if "saturday" in props["weekday"]:
                    job = schedule.every(weeks).saturday
                    Scheduler.run(job_item, job, at_time)

                if "sunday" in props["weekday"]:
                    job = schedule.every(weeks).sunday
                    Scheduler.run(job_item, job, at_time)

            elif freq == "minutes":
                amount = props.get("minutes", 1)
                job = schedule.every(amount).minutes
                Scheduler.run(job_item, job)
                result = "each {} minutes(s)".format(amount)

            elif freq == "hours":
                amount = props.get("hours", 1)
                job = schedule.every(amount).hours
                Scheduler.run(job_item, job)
                result = "each {} hour(s)".format(amount)

        else:
            if at_time:
                job = schedule.every().day
                Scheduler.run(job_item, job, at_time)

            else:
                return " (error) invalid schedule"  # not has a schedule

        if at_time:
            result = "{} at '{}'".format(result, at_time)
        return result


class TaskProcessor(object):
    """Main processor for task files. Take a task file and process they entries"""

    def __init__(self, files, log):
        self.log = log
        self.files = files

    def selection(self):
        return [f for f in self.files if f.endswith('.json')]

    def execute(self, path, filename):
        start = time.time()
        f = "{}/{}".format(path, filename)
        try:
            r = Runner(f)
            if r.has_schedule:
                props = r.schedule
                if "times" in props:
                    times = parse_to_int(props["times"], 0)
                    runner = TaskRunner(r)
                    current = 0
                    while current < times:
                        current += 1
                        self.log.write("Executing Tasks ({}/{}): {}".format(current, times, filename))
                        runner.run(self.log)

                elif ("infinity" in props) and props["infinity"]:
                    runner = TaskRunner(r)
                    while True:
                        self.log.write("Executing Tasks (infinity): {}".format(filename))
                        runner.run(self.log)

                else:
                    res = Scheduler.enqueue(r, filename)
                    self.log.write("Scheduling Tasks: {}, for: {}".format(filename, res))

            else:
                runner = TaskRunner(r)
                if runner:
                    self.log.write("Executing Tasks: {}".format(filename))
                    runner.run(self.log)

        except Exception:
            self.log.write("Error: {}".format(traceback.format_exc()))

        finally:
            self.log.write("Finished: {0}, elapsed: {1:.2f}s".format(filename, (time.time() - start)))
            os.remove(f)


def parse_to_int(value, fail=None):
    if isinstance(value, basestring):
        try:
            return int(value)
        except ValueError:
            return fail
    else:
        return 0


class CopyProcessor(object):
    """Copy CVS, SQL files to input folder"""

    def __init__(self, files, log):
        self.log = log
        self.files = files
        self.target_path = "input"

    def selection(self):
        return [f for f in self.files if not f.endswith('.zip')]

    def execute(self, path, filename):
        start = time.time()
        source_file = u"{}/{}".format(path, filename)
        target_path = "module" if filename.endswith('.py') else self.target_path
        target_file = u"{}/{}".format(target_path, filename)
        try:
            self.log.write("Moving File: {}".format(filename))
            if os.path.isfile(target_file):
                os.remove(target_file)
            if Runner.is_task(source_file):
                copy2(source_file, target_file)
            else:
                os.rename(source_file, target_file)
        except StandardError:
            self.log.write("Error: {}".format(traceback.format_exc()))
        finally:
            self.log.write("Finished: {0}, elapsed: {1:.2f}s".format(filename, (time.time() - start)))


class ExtractProcessor(object):
    """Extract zip files"""

    def __init__(self, files, log):
        self.log = log
        self.files = files
        self.target = ''

    def set_target(self, target):
        self.target = target

    def selection(self):
        return [f for f in self.files if f.endswith('.zip')]

    def execute(self, path, filename):
        start = time.time()
        source_file = "{}/{}".format(path, filename)
        target = self.target if self.target != '' else path
        try:
            self.log.write("Extracting: {} into '{}'".format(filename, target))
            z = zipfile.ZipFile(source_file)
            z.extractall(target)
            z.close()
            self.log.write("Removing ZIP: {}".format(filename))
            os.remove(source_file)
        except StandardError:
            self.log.write("Error: {}".format(traceback.format_exc()))
        finally:
            self.log.write("Finished: {0}, elapsed: {1:.2f}s".format(filename, (time.time() - start)))


def _process(processor, path):
    for f in processor.selection():
        processor.execute(path, f)


class ZipFilesProcessor(object):
    """Process zip files"""

    def __init__(self, files, log):
        self.log = log
        self.files = files

    def selection(self):
        return [f for f in self.files if f.endswith('.zip')]

    # noinspection PyUnusedLocal
    def execute(self, path, filename):
        extract = ExtractProcessor(self.files, self.log)
        try:
            for zip_file in extract.selection():
                with tempfile.TemporaryDirectory() as temp_folder:
                    self.log.write("Creating Temporary Dir: {}".format(temp_folder))
                    try:
                        extract.set_target(temp_folder)
                        extract.execute(path, zip_file)
                        files = [f for f in os.listdir(temp_folder)]
                        _process(CopyProcessor(files, self.log), temp_folder)
                        _process(TaskProcessor(files, self.log), temp_folder)
                    except StandardError:
                        self.log.write("Error: {}".format(traceback.format_exc()))
                    finally:
                        self.log.write("Removing Temporary Dir: {}".format(temp_folder))
        except IOError:
            self.log.write("IO Error: {}".format(traceback.format_exc()))
        except StandardError:
            self.log.write("Error: {}".format(traceback.format_exc()))


class Watcher(object):
    """Watch a folder to capture files and process task on it"""

    def __init__(self, path):
        self.path = path
        self.before = dict([(f, None) for f in os.listdir(path)])
        print(u"Watcher started on '{}'.\n".format(path))

    def _process(self, processor):
        for f in processor.selection():
            processor.execute(self.path, f)

    def process_file_list(self, file_list):
        """first unzip, then copy and finally execute tasks"""
        with Logger('watcher_{}'.format(get_time_filename())) as log:
            log.write("Starting a capture...")
            self._process(ZipFilesProcessor(file_list, log))
            self._process(CopyProcessor(file_list, log))
            self._process(TaskProcessor(file_list, log))

    def check(self):
        after = dict([(f, None) for f in os.listdir(self.path)])
        added = [f for f in after if f not in self.before]

        # on add, process files on that order
        if added:
            self.process_file_list(added)

        self.before = after


