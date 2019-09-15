DasLaden is a simple, tiny and ridiculus ETL made with Python
=============================================================

Dasladen is a general purpose Python package to make an automate ETL (Extracting, Transforming and
Loading data) through the configuration of one or more .json files that represents tasks. 
It is based on `petl`. It can do some tasks like:

- load a .csv file to database table
- run a database query into a .csv file
- run a database query into a database table
- convert a .csv file into another .csv file
- convert a .xls file into a .csv file
- load a .xml file into a database table
- convert a .xls file into a .csv file

This tasks can be configured to do some basic transformations offer by `petl` and you can write your own
transformations in a Python module or class to be called by Dasladen during loading process.

There is others types of tasks to do things like:

- Compact files into .zip file
- Extract files from .zip file 
- Upload a file
- Download a file
- Execute a Python script
- Execute a SQL command

The tasks are configured in a `.json` file that supports a sequence of tasks that will be executed 
in configured order. Details of how to configure tasks will be in Wiki pages. 
 
The basic steps to use DasLaden is:

- Install dasladen package via `pip install dasladen` in your environment or in virtualenv.
- Install database driver package if you want to execute database tasks. Dasladen is prepared to run with the
following drivers: MySQL via `PyMySQL`, MS SQL Server via `pyodbc` and Oracle via `cx_Oracle`. Please see
the limitations on the driver package that you choose.
- Create a folder for you project.
- Prepare a folder structure in project folder with following names:
  - `input` Is the default folder to put input files, like .csv, .xml, .xls and .sql files
  - `output` Is the default folder that tasks write target files
  - `module` Is the folder for python scripts if you can't put then in project folder
  - `capture` Is the default folder to drop task files (.json or .zip)
  - `log` Is the folder that Dasladen write task logs
  - `tasks` Is the folder that you can put tasks files. It is only a suggestion.
- Create a `.json` file with your tasks in `tasks` folder.
- Start DasLaden from project folder calling `python -m dasladen`. 
- If you want to see log in console window, pass a `--verbose` as argument on call.
- Copy the `.json` tasks file from `tasks` to the `capture` folder.

The watcher will open the tasks file and process it. To see result you can open `log` folder and search 
for `watcher_DD_TT.log` where DD_TT is the date and time that log was generated. In `log` folder you
can see individual tasks logs too.

It is important that you copy the task file instead move it, because on finish it will be deleted.

If you drop a file other than `.zip` in `capture` folder, that file will be move to `input` folder.

You can zip the `.json` file with all other dependent files (.csv, .xls, etc.) and copy
that zip into `capture` folder too. Watcher will unzip then at a temporary folder, copy input
files (other than `.json` files) to input folder and execute the `.json` file.

In the `.json` file you can configure a scheduler to run the tasks. With it you can delay a execution or 
configure its recurrence. 

Data drivers via PyPi packages: 
- MySQL via [PyMySQL](https://pypi.org/project/PyMySQL/) package. v >= 0.7.5
- MS SQL Server via [pyodbc](https://pypi.org/project/pyodbc/) package. v >= 3.0.10
- Oracle via [cx_Oracle](https://pypi.org/project/cx_Oracle/) package. v >= 5.2.1
- PostgreSQL via [psycopg2](https://pypi.org/project/psycopg2/) package. v >= 2.8.3

The current version works with Python 2 and 3.
