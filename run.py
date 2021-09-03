#!/usr/bin/env python

"""Main file for shredding XML to delimited files

This script runs over a directory of XML data file extracts from the
source systems and provide either a set of delimited files to be ingesed into a
relational database or insert the data into the database itself (to be
determined by project requirements)

Script borrows from existing code base found at advanced-analytics/py-chubb-xml
"""

import argparse
import os
import sys
import time
from datetime import datetime
from getpass import getpass
from json import dumps

import xmltodict
from py_chubb_xml.util import slurp
from py_chubb_xml.parse_xml import denormalize, flatten
from toolz import partition_all, thread_last, curry
from toolz import assoc_in, update_in

from app import conf, create_logger
from app.utils import yield_files_in_dir, __get_filename_fields, progress_bar
from app.utils import change_dir, write_csv, drink_yaml
from app.utils import create_additional_logger, encode_credentials
from parsing import undo_cdata, replace_None, create_force_list_callable
from post import pack_val_string, alter_mappings, update_md5


def main():
    """Shred an entire batch of XML into Delimited Files

    The script shreds an entire directory and provides a directory of
    delimited files (can pack all files into one for ingestion).

    A file is retrieved from the walk generator and stripped of it's xml
    structure using an internal denormalizing function. Once all files
    have been converted into flat(er) python objects, those objects are
    written as delimited files to be passed onto the ingestion team.
    """

    parser = argparse.ArgumentParser(
        description=main.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('-d', '--directory', help='directory containing XML files')
    parser.add_argument('-o', '--output', default=conf['OUTPUT_DIR'],
                        help='full directory to place parsed files')
    parser.add_argument('-i', '--interactive', action='store_true')
    parser.add_argument('-f', '--flat', action='store_true')
    parser.add_argument('-l', '--logging', action='store_false', help='disables logging')
    parser.add_argument('-s', '--separator', default='\x01')
    parser.add_argument('--pack', action='store_true', help='pack metadata in val string')
    parser.add_argument('--one-file', action='store_true')

    args = parser.parse_args()

    args.directory = os.path.realpath(args.directory)

    config = drink_yaml(os.path.join(conf["CONF"], 'xml_parsing.yml'))

    print(f'Parsing files from {args.directory} into {args.output}')

    if args.interactive:

        prompt = None
        while prompt not in ['Y', 'n']:
            prompt = input('Is this correct [Y/n]? ')

            if prompt == 'n':
                print('Aborting process')
                sys.exit(0)

            else:
                print('Please answer "Y" or "n" to start or abort the process\r')

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    log_dt = datetime.now().strftime('%Y%m%d_%H%M%S')

    logger = create_logger(conf["LOG_DIR"])
    tag_logger = create_additional_logger(
        f'additional_tags_{log_dt}.log',
        conf["LOG_DIR"]
    )

    # Ensure module is running at the proper level regardless of location
    os.chdir(conf['PROJECT_DIR'])

    header_list = list()
    xml_log = list()
    error_log = list()

    files = yield_files_in_dir(args.directory)

    alter_this_mapping = curry(alter_mappings)(alterations=config['alterations'])
    force_list_call = create_force_list_callable(config['force_list']['keys'],
                                                 config['force_list']['paths'])
    parse_xml = curry(xmltodict.parse)(attr_prefix='at-', force_list=force_list_call)

    for batch_count, batch in enumerate(partition_all(1000, files)):

        xml_list = list()
        batch_list = list(batch)
        batch_start = time.time()
        logger.info(f'parsing_batch {batch_count}')

        for counter, fi in enumerate(batch_list):
            denorm_start = time.time()

            try:
                xml = slurp(os.path.join(fi['root'], fi['fi']))
                file_fields = __get_filename_fields(fi['fi'])
                dir_name = '_'.join(file_fields.values())

            except Exception as e:
                fi['error'] = e
                error_log.append(fi)
                logger.error(f'Problem loading XML: {e}')
                sys.exit(1)

            if not args.flat:
                pipeline = (xml, parse_xml, undo_cdata, denormalize)
                output_dirname = (f'{datetime.now().strftime("%Y-%m-%d")}_'
                                    f'{file_fields["source_system"]}')
                row_header = f'{args.directory.split("/")[-1]}'

            else:
                pipeline = (xml, parse_xml, undo_cdata, flatten, denormalize)
                output_dirname = (f'{datetime.now().strftime("%Y-%m-%d")}_'
                                    f'{file_fields["source_system"]}_flattened')
                row_header = f'{args.directory.split("/")[-1]}_flattened'

            # Parser Postprocessing
            pipeline = (
                *pipeline,
                (map, lambda x: assoc_in(x, ['file_name'], dir_name)),
                (map, lambda x: assoc_in(x, ['etl_record_updated'], timestamp)),
                (map, lambda x: update_in(x, ['val'], replace_None)),
                (map, lambda x: update_in(x, ['val'], alter_this_mapping)),
                list
            )

            try:
                xml_shred = thread_last(*pipeline)
                tag_logger.debug(f'Additional tags added to {dir_name}')

            except Exception as e:
                fi['error'] = e
                error_log.append(fi)
                logger.error(f'Problem parsing XML: {e}')
                sys.exit(1)

            json_keys = {str(row['json_key']) for row in xml_shred}
            max_values = max([len(row['val']) for row in xml_shred])

            xml_list.append(xml_shred)
            denorm_time = time.time() - denorm_start

            # xml_log is not reset on batch iteration
            xml_log.append({
                'file': dir_name,
                'file_size': os.path.getsize(os.path.join(fi['root'], fi['fi'])) / 1024,
                'parse_time': denorm_time,
                'unique_node_count': len(json_keys),
                'maximum_fields': max_values
            })

            progress_bar((counter + 1) / len(batch_list))

        parse_time = time.time() - batch_start
        logger.info(f'Batch parsed in {time.time() - batch_start:4f} seconds')
        # format fields for ingestion after tokenization
        dumped_xml_list = list()
        for xml_shred in xml_list:

            if args.pack:

                dumped_xml_list.append(thread_last(
                    xml_shred,
                    (map, update_md5),
                    (map, pack_val_string),
                    (map, lambda x: update_in(x, ['val'], dumps)),
                    (map, lambda x: update_in(x, ['json_key'], dumps)),
                    (map, lambda x: update_in(x, ['json_key_minus_one'], dumps)),
                    (map, lambda x: update_in(x, ['md5_list'], dumps)),
                    (map, lambda x: update_in(x, ['md5_list_minus_one'], dumps)),
                    list
                ))

            else:

                dumped_xml_list.append(thread_last(
                    xml_shred,
                    (map, update_md5),
                    (map, lambda x: update_in(x, ['val'], dumps)),
                    (map, lambda x: update_in(x, ['json_key'], dumps)),
                    (map, lambda x: update_in(x, ['json_key_minus_one'], dumps)),
                    (map, lambda x: update_in(x, ['md5_list'], dumps)),
                    (map, lambda x: update_in(x, ['md5_list_minus_one'], dumps)),
                    list
                ))

        # write output to delimited files
        with change_dir(args.output):

            logger.info(f'writing data to {output_dirname}')

            if not os.path.isdir(output_dirname):
                os.mkdir(output_dirname)

            for count, xml_shred in enumerate(dumped_xml_list):

                write_csv(
                    xml_shred,
                    os.path.join(output_dirname, xml_shred[0]['file_name']),
                    sep=args.separator
                )

                if args.one_file:

                    if count == 0 and batch_count == 0:
                        write_csv(
                            xml_shred,
                            f'{file_fields["source_system"]}_full',
                            sep=args.separator
                        )

                    else:
                        write_csv(
                            xml_shred,
                            f'{file_fields["source_system"]}_full',
                            sep=args.separator,
                            write_header=False,
                            mode='a'
                        )

                progress_bar((count + 1) / len(dumped_xml_list))

        header_file = {
            'batch_number': f'{output_dirname}_{batch_count}',
            'files_processed': counter,
            'batch_time': time.time() - batch_start,
            'parse_time': parse_time,
            'tokenize_time': 0
        }
        header_list.append(header_file)

        logger.info(f'Batch data fully processed in {time.time() - batch_start:4f}')

    # Output reports
    with change_dir(args.output):

        logger.info(f'writing reports to {row_header}')
        write_csv(xml_log, row_header, sep=args.separator)
        write_csv(
            header_list,
            os.path.join(output_dirname, 'header'),
            sep=args.separator
        )


        if error_log:

            logger.warning('Some files did not parse. Check Error log in output')
            write_csv(error_log, os.path.join(output_dirname, 'errors'),
                      sep=args.separator)

    logger.info('XML Parsing Process Complete')


if __name__ == '__main__':
    main()
