"""Collection of utility methods

Utilities that are used at multiple stages throughout the package are stored
here for easy accesss in new modules or test scripts.

Methods originally found in advanced-analytics/prs-xml repo

"""

import base64
import csv
import os
import re
import sys
import logging as lg
from os import walk
from toolz import update_in
from json import dumps
from contextlib import contextmanager

import yaml
from py_chubb_xml.util import if_string_remove_crnl


def progress_bar(progress: float, width: int = 20):
    """Code commandeered from Brian Khuu answer to Stack Exchange question"""

    status = str()
    if isinstance(progress, int):
        progress = float(progress)

    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"

    if progress < 0:
        progress = 0
        status = "Halt...\r\n"

    if progress >= 1:
        progress = 1
        status = "Done...\r\n"

    block = int(round(width * progress))
    block_length = f'{"#" * block + "-" * (width - block)}'
    text = f'\rProgress: [{block_length}] {round(progress * 100, 4)}%\t {status}'
    sys.stdout.write(text)
    sys.stdout.flush()


# from prs-xml/src/app/utils/fs.py
def yield_files_in_dir(input_dir, limit=None, extension_filter="xml"):
    """Utility, return xml files walked in dir with root/path in tuple
    output."""
    for root, _, files in walk(input_dir):
        for fi in files[0:limit]:
            if fi.endswith(extension_filter):
                yield {"root": root, "fi": fi}


def __get_filename_fields(fi):
    """extract metadata from filename

    As per requirements, extract metadata from a file name matching our
    requirements. Something akin to this:

    <xml_type>_<source_system>_<policy_number>_<source_key>.xml

    This was the case for the sample masterpiece data originally worked on.
    CAAS data has a different layout that shares only the initial metadata.

    {xml_type}_{source_system}_

    Yields a dict with such keys

    """

    fn_fields = ["xml_type", "source_system",
                 "policy_number", "source_key_timestamp"]
    # split on _ or . , the -1 drops the extension
    fn_values = re.split(r'_|\.|:', os.path.basename(fi))[:-1]

    # if fn_values[1] == 'PLA':
    #     # capture the timestamp infomation into policy number
    #     hold = '.'.join(fn_values[2:6])
    #     del fn_values[2:6]

    #     fn_values.insert(2, hold)

    # generic processing of string values
    zip_values = [
        fn_values[0],  # xml_type
        fn_values[1],  # source_system
        '_'.join(fn_values[2:len(fn_values[:-1])]),  # time information
        fn_values[-1]  # source_key_timestamp
    ]

    return dict(zip(fn_fields, zip_values))


def drink_yaml(yaml_file):
    """Load the contents of a yaml configuration file"""

    with open(yaml_file, 'r') as stream:
        return yaml.load(stream)


def write_csv(data, file_name, sep='\x01', write_header=True, mode='w'):
    """write a python list of dictionaries to file

    Take the dictionary ojects in the list of shredded xml and produce a
    delimited file based on the arguments. Does not consider what the
    current directory is.

    Arguments
        data -- python dictionary
        file_name -- name of the file to create
        sep -- delimiter for the written file

    """

    headers = data[0].keys()

    with open(file_name + '.tsv', mode, newline='') as f:
        writer = csv.DictWriter(
            f,
            delimiter=sep,
            quoting=csv.QUOTE_NONE,
            fieldnames=headers,
            quotechar='\x01',
            dialect='unix',
            escapechar='\x01'
        )

        if write_header:
            writer.writeheader()

        for row in data:
            clean_data = {k: if_string_remove_crnl(v) for k, v in row.items()}
            writer.writerow(clean_data)


@contextmanager
def change_dir(directory):
    """Set and forget os.getcwd()

    Switch to directory for specific operations then return to home

    Arguments:
        directory -- [description]
    """

    try:
        current_working_directory = os.getcwd()
        os.chdir(directory)
        yield  # no variable to hold in context

    finally:
        os.chdir(current_working_directory)


def make_json_friendly(keys: tuple, d: dict) -> dict:
    """Takes python objects and makes them into a json string so they can be
    loaded into other systems."""
    return update_in(d, keys, dumps)


def create_additional_logger(log_name, log_path, level=lg.DEBUG):
    """Additional logger for capturing data on tags created by alter_mappings"""

    log_formatter = lg.Formatter("%(asctime)s [%(levelname)-5.5s] -> %(message)s")
    logger = lg.getLogger(log_name)
    logger.setLevel(level)

    file_handler = lg.FileHandler(f'{log_path}/{log_name}')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)

    return logger


def encode_credentials(user, passwd):
    """Create the Base64 string of a basic credential"""

    credential_input = f'{user}:{passwd}'.encode('utf-8')
    encoded_credentials = base64.b64encode(credential_input).decode('utf-8')
    credential_string = f'Basic {encoded_credentials}'

    return credential_string


if __name__ == "__main__":
    pass
