"""
Dasladen ETL
A Simple, Tiny and Ridiculus ETL Engine made with Python
"""

import os
import sys
import logging

from argparse import ArgumentParser
from schedule import run_pending, every
from time import sleep
from shutil import copy

from .processor import Watcher
from .log import add_log_handler, ConsoleHandler, FileHandler, DebugHandler


def init():
    folders = ["capture", "input", "output", "log", "module"]
    log = ConsoleHandler()
    for folder in folders:
        if not os.path.exists(folder):
            os.mkdir(folder)
            log.write("Folder '{}' created".format(folder))
    

def main():
    parser = ArgumentParser(description="DasLaden ETL")
    parser.add_argument("-task", nargs="?", default=None, const=None, help="Task file to process")
    parser.add_argument("-capture", default="capture", help="Capture folder. Default 'capture'")
    parser.add_argument("-watch-time", default=10, help="Capture watch time in seconds. Default 10s")
    parser.add_argument("--no-log", nargs="?", default=False, const=True, help="Disable file log")
    parser.add_argument("--verbose", nargs="?", default=False, const=True, help="Output logs to console")
    parser.add_argument("--no-init", nargs="?", default=False, const=False, help="Don't create folder structure")
    
    args = parser.parse_args()
    v = vars(args)    

    if not v["no_init"]:
        init()

    if not v["no_log"]:
        add_log_handler(FileHandler())

    if v["verbose"]:
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
        add_log_handler(DebugHandler())

    if not v["task"] and not os.path.exists(v["capture"]):
        print("The '{}' folder does not exists.".format(v["capture"]))

    else:
        # make path for dynamic module import
        sys.path.append('{}/module'.format(os.getcwd()))
        watch = Watcher(v["capture"])

        if v["task"]:
            print("DasLaden ETL started.")
            watch.process_file(v["task"])

        else:
            print("DasLaden ETL started. (Press CTRL+C to stop)")

            if os.path.isfile("./start.zip"):
                copy('./start.zip', v["capture"])

            watch_time = v["watch_time"]
            every(watch_time).seconds.do(watch.check)

            while True:
                try:
                    run_pending()
                    sleep(1)
                except KeyboardInterrupt:
                    print("\nDasLaden ETL finished.")
                    break


if __name__ == "__main__":
    main()

