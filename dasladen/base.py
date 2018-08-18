"""
Dasladen ETL
A Simple, Tiny and Ridiculus ETL Engine made with Python
"""

import os
import sys

from argparse import ArgumentParser
from schedule import run_pending, every
from time import sleep
from shutil import copy
from processor import Watcher
from log import add_log_handler, ConsoleHandler


def main():
    parser = ArgumentParser(description="DasLaden ETL")
    parser.add_argument("-capture", default="capture", help="Capture folder")
    parser.add_argument("--verbose", nargs="?", default=False, const=True, help="Output logs to console")
    args = parser.parse_args()
    v = vars(args)

    if v["verbose"]:
        add_log_handler(ConsoleHandler())

    print ("DasLaden ETL started. (Press CTRL+C to stop)")
    watch = Watcher(v["capture"])

    if os.path.isfile("./start.zip"):
        copy('./start.zip', v["capture"])

    # make path for dynamic module import
    sys.path.append('{}/module'.format(os.getcwd()))

    # TODO: receive a parameter for watch time
    every(10).seconds.do(watch.check)

    while True:
        try:
            run_pending()
            sleep(1)
        except KeyboardInterrupt:
            print ("\nDasLaden ETL finished.")
            break


if __name__ == "__main__":
    main()

