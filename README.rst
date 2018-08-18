DasLaden is a simple, tiny and ridiculus ETL made with Python
=============================================================

``dasladen`` is a general purpose Python package for make an automate ETL (Extracting, Transforming and
Loading data) through the task configuration with .json files. It is based on ``petl``.
It can make some tasks like:

- .csv file to database table
- Database query to .csv file
- Database query to database table
- .csv file to .csv file
- .xls file to .csv file
- .xml file to database table
- .xls file to .csv file

This tasks can be configure to has some basic transformations offer by ``petl`` and you can write your own
transformations in a Python module or class to be called by Dasladen during loading process.

There is others tasks like:

- Compact files into .zip file
- File upload
- Executing Python scripts
- Executing SQL commands

The basic steps to use DasLaden is:

- Install package
- Prepare folder structure on the folder that you initiate DasLaden
 - ``input`` Is the default folder to input files, like .csv, .xml, .xls and .sql files
 - ``output`` Is the default folder that tasks write target files
 - ``module`` Is the folder for python scripts if you can't put then in execution root
 - ``capture`` Is the default folder to drop task files (.json)
 - ``log`` Is the folder that Dasladen write logs
- Start DasLaden (call ``python -m dasladen`` It will start a watcher in capture folder)

After this, you can create .json files with task configuration and drop it into capture folder.
The watcher will open that file and process its tasks. To see result, you can open log folder
or you can run Watcher with optional --verbose parameter to output task log do console.

You can drop the input files into capture folder too. It will be move to the input folder.

You can zip the .json task file with all input files (.csv, .xls, etc.) dependencies and drop
the zip into capture folder too. Watcher will unzip then at the temporary folder, copy input
files (other than .json task files) to input folder and execute the task file.

Features support in task file:

- Configure database connections
- Configure tasks that will execute in order that it appear in file
- Configure a schedule to run the task. It can delay execution or config recurrence

Data drivers support: **install package for database that you will use. They not installed with DasLaden**

- MySQL via ``PyMySQL`` package
- MS SQL via ``pyodbc`` package
- Oracle via ``cx_Oracle`` package


