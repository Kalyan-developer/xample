"""Basic configuration script for use in multiple applications

This code is intended to be placed into many different programs to faciliate
good practices such as using a standard logging syntax and hosting any paths in
one location. This module is intended to be embedded two levels deep in a
repository as the __init__ module of that directory (i.e. src/apps/__init__.py).

Requires the toolz package as part of your environment

Variables:
    global conf -- Configuration dictionary that contains global variables
        intended to be shared across modules
    global logger -- Standard logging object with basic log and console outputs
"""

import logging as lg
from datetime import datetime
from os import path

from toolz import thread_first

global conf
global logger

try:
    PROJECT_DIR = thread_first(
        __file__,
        path.realpath,
        path.dirname,
        (path.join, '..'),
        path.abspath
    )

except NameError:
    PROJECT_DIR = path.abspath('../')


def create_logger(log_path, log_level=lg.DEBUG):
    """Utility to set up logging object

    Custom Levels for prs_pact_xml:
        5 -- Used for logging messages concerning tokenized data

    Arguments:
        log_path -- location to create logger in package

    Keyword arguments:
        log_level -- log level to report in the file (default: {lg.DEBUG})
    """

    file_name = f'{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'

    log_formatter = lg.Formatter("%(asctime)s [%(levelname)-5.5s] %(module)s -> %(message)s")
    console_formattter = lg.Formatter("[%(levelname)-5.5s] %(module)s -> %(message)s")

    root_logger = lg.getLogger(__name__)
    root_logger.setLevel(log_level)

    file_handler = lg.FileHandler(f'{log_path}/{file_name}')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = lg.StreamHandler()
    console_handler.setLevel(lg.INFO)
    console_handler.setFormatter(console_formattter)
    root_logger.addHandler(console_handler)

    root_logger.info("Setup logging.")

    return root_logger


def init(PROJECT_DIR):
    """Initialization method to create a global variable dictionary

    As a matter of convenience, all globals can be stated here and then refered
    across differnt modules instead of having to reproduce logic for things like
    the project directory or timestamps

    Arguments:
        PROJECT_DIR -- root path for the project as predetermined by the module
    """
    global conf

    conf = {'PROJECT_DIR': PROJECT_DIR,
            'INPUT_DIR': path.join(PROJECT_DIR, "sample-xml"),
            'OUTPUT_DIR': path.join(PROJECT_DIR, "output"),
            'SQL_DIR': path.join(PROJECT_DIR, "sql"),
            'CONF': path.join(PROJECT_DIR, "conf"),
            'XSD_FILE': path.join(PROJECT_DIR, "conf",
                                  "CpiPolicyAdminServiceObjects.xsd"),
            'LOG_DIR': path.join(PROJECT_DIR, "log"),
            'TEST': path.join(PROJECT_DIR, "src", "tests"),
            'VTS_SAMPLE': {"tokengroup": "CB01", "tokentemplate": "CBDIGIT01", "data": "111"},
            'JOB_BATCH': float(datetime.now().strftime("%Y%m%d.%H%M%S"))}


init(PROJECT_DIR)
