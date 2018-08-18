
"""
Task Module

External Dependencies:
- petl : (c) 2012 Alistair Miles - MIT License (https://pypi.python.org/pypi/petl/1.1.0)

Features:
- CSV -> DB task
- DB -> CSV task
- DB -> DB task
- CSV -> CSV task
- XLS -> CSV task
- XML -> DB task
- XML -> CSV task
- FTP Upload task
- ZIP task
- Python Module task
- SQL task

"""

import os
import sys
import petl as etl
import ftputil
import zipfile
import importlib

from string import maketrans
from log import get_time_filename
from taskdriver import *


class Connection(object):
    def __init__(self, config):
        self._connections = config.get("connections", None)
        
    def get_connection(self, name):
        if self._connections is not None:
            for conn in self._connections:
                if conn["name"] == name:
                    return conn
        raise KeyError()


class DriverFactory(object):
    def __init__(self, config):
        self._connections = Connection(config)

    def get_connection(self, name):
        return self._connections.get_connection(name)

    def get_driver(self, name):
        item = self.get_connection(name)
        # setup environment configuration before open connection
        if "environment" in item:
            for env in item["environment"]:
                key = env["key"]
                key = key.encode('utf-8') if isinstance(key, unicode) else key
                value = env["value"]
                value = value.encode('utf-8') if isinstance(value, unicode) else value
                os.environ[key] = value
        # select driver 
        if item["driver"] == "MySQL":
            return MySQLDriver(item)
        elif item["driver"] == "Oracle":
            return OracleDriver(item)
        elif item["driver"] == "MSSQL":
            return MSSQLDriver(item)
        
        raise NotImplementedError    


class BaseTask(object):
    """Base class for tasks"""

    def run(self, driver, task, log):
        """Run Forrest, run
        :param driver: driver factory
        :param task: task configuration
        :param log: logger object
        """
        raise NotImplementedError("implement it")

    def _parse_sql(self, task_node):
        sql = u''
        if isinstance(task_node, dict):
            if "command" in task_node:
                sql = task_node["command"]
            elif "query" in task_node:
                path = task_node.get("path", "input")
                input_query = u"{}/{}".format(path, task_node["query"])
                with open(input_query, "r") as q:
                    sql = reduce(lambda x, y: u"{} {}".format(x, y), q.readlines())
            sql = sql.format(**task_node["params"]) if "params" in task_node else sql

        sql = sql[:-1] if sql.endswith(";") else sql
        return sql


class TransformSubTask(object):
    def __init__(self, task, log):
        self.task = task
        self.log = log

    def _modules_transform(self, record_set):
        if "transforms" in self.task:
            for transform in self.task["transforms"]:
                record_set = self._module_transform(record_set, transform)
        elif "transform" in self.task:
            record_set = self._module_transform(record_set, self.task["transform"])

        return record_set

    # noinspection PyMethodMayBeStatic
    def _module_transform(self, record_set, transform=None):
        if transform is not None:
            module_name = transform["module"]
            package = transform.get("package", None)
            module_obj = importlib.import_module(module_name, package)
            reload(module_obj)
            fields = transform.get("fields", [])
            args = transform.get("args", {})
            if "class" in transform:
                self.log.write(u"Transform data with {}".format(transform["class"]))
                instance = getattr(module_obj, transform["class"])(**args)
                record_set = instance.transform(record_set, *fields)
            else:
                record_set = module_obj.transform(record_set, *fields, **args)

        return record_set

    def _petl_transform(self, record_set):
        if "transform" in self.task:
            transform = self.task["transform"]
            if "convert" in transform:
                conversions = {}
                for field, func in transform["convert"]:
                    conversions[field] = func
                record_set = etl.convert(record_set, conversions)

            if "filter" in transform:
                record_set = etl.select(record_set, transform["filter"])

            if "remove" in transform:
                cuts = []
                for field in transform["remove"]:
                    cuts.append(field)
                record_set = etl.cutout(record_set, cuts)

            if "rename" in transform:
                names = {}
                for old, new_one in transform["rename"]:
                    names[old] = new_one
                record_set = etl.rename(record_set, names)

        return record_set

    def get_result(self, record_set):
        record_set = self._modules_transform(record_set)
        record_set = self._petl_transform(record_set)
        return record_set


class DbCsvTask(BaseTask):

    def run(self, driver, task, log):
        input_driver = driver.get_driver(task["source"]["connection"])
        sql = self._parse_sql(task["source"])
        db = input_driver.get_db()
        record_set = etl.fromdb(db, sql)
        if not etl.data(record_set).any():
            log.write("Task skipped. No rows on source")
        else:
            transform = TransformSubTask(task, log)
            record_set = transform.get_result(record_set)

            fld = task["target"].get("folder", "output")
            fld = fld.encode('latin1') if isinstance(fld, unicode) else fld
            target = task["target"]["file"]
            target = target.encode('latin1') if isinstance(target, unicode) else target
            out = "{}/{}".format(fld, target)

            separator = task["target"].get("delimiter", ";")
            separator = separator.encode('utf-8') if isinstance(separator, unicode) else separator
            enc = task["target"].get("encoding", "utf-8")

            task_log = "log/db-csv_{}_{}.log".format(task["name"], get_time_filename())
            with open(task_log, "w") as lg:
                if "truncate" in task["target"] and task["target"]["truncate"]:
                    record_set.progress(10000, out=lg).tocsv(out, encoding=enc, delimiter=separator)
                else:
                    record_set.progress(10000, out=lg).appendcsv(out, encoding=enc, delimiter=separator)
        db.close()


class CsvDbTask(BaseTask):

    def run(self, driver, task, log):
        source_folder = task["source"].get("folder", "input")
        source_folder = source_folder.encode('latin1') if isinstance(source_folder, unicode) else source_folder
        source = task["source"]["file"]
        source = source.encode('latin1') if isinstance(source, unicode) else source
        inp = "{}/{}".format(source_folder, source)

        separator = task["source"].get("delimiter", ";")
        separator = separator.encode('utf-8') if isinstance(separator, unicode) else separator

        enc = task["source"].get("encoding", "utf-8")
        enc = enc.encode('utf-8') if isinstance(enc, unicode) else enc

        record_set = etl.fromcsv(inp, encoding=enc, delimiter=separator)

        if not etl.data(record_set).any():
            log.write("Task skipped. No rows on source")
        else:
            transform = TransformSubTask(task, log)
            record_set = transform.get_result(record_set)

            output_driver = driver.get_driver(task["target"]["connection"])
            db = output_driver.get_db()

            table = task["target"]["table"]
            table = table.encode('utf-8') if isinstance(table, unicode) else table
            if "schema" in task["target"]:
                schema_name = task["target"]["schema"]
                schema_name = schema_name.encode('utf-8') if isinstance(schema_name, unicode) else schema_name
            else:
                schema_name = None

            task_log = "log/csv-db_{}_{}.log".format(task["name"], get_time_filename())
            with open(task_log, "w") as lg:
                if "truncate" in task["target"] and task["target"]["truncate"]:
                    record_set.progress(10000, out=lg).todb(output_driver.cursor(db), tablename=table, schema=schema_name)
                else:
                    record_set.progress(10000, out=lg).appenddb(output_driver.cursor(db), tablename=table, schema=schema_name)

            db.close()


class DbDbTask(BaseTask):

    def run(self, driver, task, log):
        input_driver = driver.get_driver(task["source"]["connection"])
        sql = self._parse_sql(task["source"])
        db = input_driver.get_db()
        record_set = etl.fromdb(db, sql)
        if not etl.data(record_set).any():
            log.write("Task skipped. No rows on source")
        else:
            transform = TransformSubTask(task, log)
            record_set = transform.get_result(record_set)

            output_driver = driver.get_driver(task["target"]["connection"])
            out_db = output_driver.get_db()

            table = task["target"]["table"]
            table = table.encode('utf-8') if isinstance(table, unicode) else table
            if "schema" in task["target"]:
                schema_name = task["target"]["schema"]
                schema_name = schema_name.encode('utf-8') if isinstance(schema_name, unicode) else schema_name
            else:
                schema_name = None

            task_log = "log/db-db_{}_{}.log".format(task["name"], get_time_filename())
            with open(task_log, "w") as lg:
                if "truncate" in task["target"] and task["target"]["truncate"]:
                    record_set.progress(10000, out=lg).todb(output_driver.cursor(out_db), tablename=table, schema=schema_name)
                else:
                    record_set.progress(10000, out=lg).appenddb(output_driver.cursor(out_db), tablename=table, schema=schema_name)

            out_db.close()
        db.close()


class CsvCsvTask(BaseTask):

    def run(self, driver, task, log):
        inp = task["source"]["file"]
        inp = inp.encode('latin1') if isinstance(inp, unicode) else inp
        inp = "input/{}".format(inp)
        separator = task["source"].get("delimiter", ";")
        separator = separator.encode('utf-8') if isinstance(separator, unicode) else separator

        enc = task["source"].get("encoding", "utf-8")
        enc = enc.encode('utf-8') if isinstance(enc, unicode) else enc
        
        record_set = etl.fromcsv(inp, encoding=enc, delimiter=separator)
        if not etl.data(record_set).any():
            log.write("Task skipped. No rows on source")
        else:
            transform = TransformSubTask(task, log)
            record_set = transform.get_result(record_set)

            out = task["target"]["file"]
            out = out.encode('latin1') if isinstance(out, unicode) else out
            out = "output/{}".format(out)
            separator = task["target"].get("delimiter", ";")
            separator = separator.encode('utf-8') if isinstance(separator, unicode) else separator
            enc = task["target"].get("encoding", "utf-8")

            task_log = "log/csv-csv_{}_{}.log".format(task["name"], get_time_filename())
            with open(task_log, "w") as lg:
                if "truncate" in task["target"] and task["target"]["truncate"]:
                    record_set.progress(10000, out=lg).tocsv(out, encoding=enc, delimiter=separator)
                else:
                    record_set.progress(10000, out=lg).appendcsv(out, encoding=enc, delimiter=separator)


class XlsCsvTask(BaseTask):

    def run(self, driver, task, log):
        inp = task["source"]["file"]
        inp = inp.encode('latin1') if isinstance(inp, unicode) else inp
        inp = "input/{}".format(inp)
        sheet = task["source"].get("sheet", None)
        use_view = task["source"].get("use_view", True)
       
        record_set = etl.fromxls(inp, sheet, use_view=use_view)
        if not etl.data(record_set).any():
            log.write("Task skipped. No rows on source")
        else:
            transform = TransformSubTask(task, log)
            record_set = transform.get_result(record_set)

            out = task["target"]["file"]
            out = out.encode('latin1') if isinstance(out, unicode) else out
            out = "output/{}".format(out)
            separator = task["target"].get("delimiter", ";")
            separator = separator.encode('utf-8') if isinstance(separator, unicode) else separator
            enc = task["target"].get("encoding", "utf-8")

            task_log = "log/xls-csv_{}_{}.log".format(task["name"], get_time_filename())
            with open(task_log, "w") as lg:
                if "truncate" in task["target"] and task["target"]["truncate"]:
                    record_set.progress(10000, out=lg).tocsv(out, encoding=enc, delimiter=separator)
                else:
                    record_set.progress(10000, out=lg).appendcsv(out, encoding=enc, delimiter=separator)


class XmlCsvTask(BaseTask):

    def run(self, driver, task, log):
        inp = task["source"]["file"]
        inp = inp.encode('latin1') if isinstance(inp, unicode) else inp
        inp = "input/{}".format(inp)
        row_match = task["source"].get("row", None)
        value_match = task["source"].get("value", None)
        attr = task["source"].get("attr", None)
        mapping = task["source"].get("mapping", None)

        if row_match and value_match:
            if attr:
                record_set = etl.fromxml(inp, row_match, value_match, attr)
            else:
                record_set = etl.fromxml(inp, row_match, value_match)
        elif row_match and mapping:
            record_set = etl.fromxml(inp, row_match, mapping)
        else:
            raise ValueError('Incorrect parameter for source')

        if not etl.data(record_set).any():
            log.write("Task skipped. No rows on source")
        else:
            transform = TransformSubTask(task, log)
            record_set = transform.get_result(record_set)

            out = task["target"]["file"]
            out = out.encode('latin1') if isinstance(out, unicode) else out
            out = "output/{}".format(out)
            separator = task["target"].get("delimiter", ";")
            separator = separator.encode('utf-8') if isinstance(separator, unicode) else separator
            enc = task["target"].get("encoding", "utf-8")

            task_log = "log/xml-csv_{}_{}.log".format(task["name"], get_time_filename())
            with open(task_log, "w") as lg:
                if "truncate" in task["target"] and task["target"]["truncate"]:
                    record_set.progress(10000, out=lg).tocsv(out, encoding=enc, delimiter=separator)
                else:
                    record_set.progress(10000, out=lg).appendcsv(out, encoding=enc, delimiter=separator)


class XmlDbTask(BaseTask):

    def run(self, driver, task, log):
        inp = task["source"]["file"]
        inp = inp.encode('latin1') if isinstance(inp, unicode) else inp
        inp = "input/{}".format(inp)
        row_match = task["source"].get("row", None)
        value_match = task["source"].get("value", None)
        attr = task["source"].get("attr", None)
        mapping = task["source"].get("mapping", None)

        if row_match and value_match:
            if attr:
                record_set = etl.fromxml(inp, row_match, value_match, attr)
            else:
                record_set = etl.fromxml(inp, row_match, value_match)
        elif row_match and mapping:
            record_set = etl.fromxml(inp, row_match, mapping)
        else:
            raise ValueError('Incorrect parameter for source')

        if not etl.data(record_set).any():
            log.write("Task skipped. No rows on source")
        else:
            transform = TransformSubTask(task, log)
            record_set = transform.get_result(record_set)

            output_driver = driver.get_driver(task["target"]["connection"])
            db = output_driver.get_db()

            table = task["target"]["table"]
            table = table.encode('latin1') if isinstance(table, unicode) else table
            if "schema" in task["target"]:
                schema_name = task["target"]["schema"]
                schema_name = schema_name.encode('latin1') if isinstance(schema_name, unicode) else schema_name
            else:
                schema_name = None

            task_log = "log/xml-db_{}_{}.log".format(task["name"], get_time_filename())
            with open(task_log, "w") as lg:
                if "truncate" in task["target"] and task["target"]["truncate"]:
                    record_set.progress(10000, out=lg).todb(output_driver.cursor(db),
                                                            tablename=table, schema=schema_name)
                else:
                    record_set.progress(10000, out=lg).appenddb(output_driver.cursor(db),
                                                                tablename=table, schema=schema_name)
            db.close()


class FtpUploadTask(BaseTask):

    def run(self, driver, task, log):
        source = task["source"]["file"]
        source = source.encode('latin1') if isinstance(source, unicode) else source
        source_path = task["source"].get("path", "output")
        source_path = source_path.encode('latin1') if isinstance(source_path, unicode) else source_path
        source_path = "{}/{}".format(source_path, source)
        target = task["target"].get("file", source)
        target_path = "{}/{}".format(task["target"]["path"], target)
        
        item = driver.get_connection(task["target"]["connection"])
        with ftputil.FTPHost(item["host"],
                             item["user"],
                             item["pass"]) as host:
            host.upload_if_newer(source_path, target_path)
            

class ZipTask(BaseTask):

    @staticmethod
    def _encode_cp437(s):
        return s.encode('cp437', errors='replace').translate(maketrans('?', '_'))

    def run(self, driver, task, log):
        source = task["source"]["files"]
        source_path = task["source"].get("path", "output")
        source_path = source_path.encode('latin1') if isinstance(source_path, unicode) else source_path
        remove_after = task["source"].get("remove_after", [])
        if "target" in task:
            target = task["target"]["file"] if "file" in task["target"] else "{}.zip".format(source[0])
            target_path = task["target"].get("path", source_path)
        else:
            target = "{}.zip".format(source[0])
            target_path = source_path
        target = target.encode('latin1') if isinstance(target, unicode) else target
        target = "{}.zip".format(target) if not target.endswith(".zip") else target
        target_path = target_path.encode('latin1') if isinstance(target_path, unicode) else target_path

        target_file = "{}/{}".format(target_path, target)
        with zipfile.ZipFile(target_file, 'w', zipfile.ZIP_DEFLATED) as z:
            for file_name in source:
                file_name = file_name.encode('latin1') if isinstance(file_name, unicode) else file_name
                z.write("{}/{}".format(target_path, file_name), ZipTask._encode_cp437(file_name))

        for file_name in remove_after:
            os.remove("{}/{}".format(target_path, file_name))


class PyExecTask(BaseTask):

    def run(self, driver, task, log):
        module_name = task["source"]["module"]
        package = task["source"].get("package", None)
        if "args" in task["source"]:
            args = task["source"]["args"]
            sys.argv[1:] = args
        else:
            sys.argv[1:] = []

        module_obj = importlib.import_module(module_name, package)
        reload(module_obj)
        # TODO: better no call main
        module_obj.main()


class SqlExecTask(BaseTask):

    def run(self, driver, task, log):
        output_driver = driver.get_driver(task["target"]["connection"])
        db = output_driver.get_db()
        sql = self._parse_sql(task["source"])
        cur = output_driver.cursor(db)
        cur.execute(sql)
        db.commit()
        db.close()


class NopTask(BaseTask):
    """Nop task to user programming json and disable a task without remove from task file"""
    def run(self, driver, task, log):
        log.write(u'Nothing to do. Disabled task? Check it John Snow!')


class CustomTask(BaseTask):

    def run(self, driver, task, log):
        log.write(u"Loading custom task.")
        module_name = task["module"]
        package = task.get("package", None)
        module_obj = importlib.import_module(module_name, package)
        reload(module_obj)
        task_class = getattr(module_obj, task["class"])
        task_instance = task_class()
        task_instance.run(driver, task, log)


class TaskFactory(object):
    _tasks = {
        "db-csv": DbCsvTask,
        "csv-db": CsvDbTask,
        "db-db": DbDbTask,
        "csv-csv": CsvCsvTask,
        "xls-csv": XlsCsvTask,
        "xml-csv": XmlCsvTask,
        "xml-db": XmlDbTask,
        "ftp-upload": FtpUploadTask,
        "zip": ZipTask,
        "py-exec": PyExecTask,
        "sql-exec": SqlExecTask,
        "nop": NopTask,
        "custom": CustomTask
    }

    def get_task(self, task_type):
        if task_type in self._tasks:
            return self._tasks[task_type]()
        raise NotImplementedError


