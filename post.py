"""Module for various postprocessing options after data has been tokenized"""

from py_chubb_xml.util import md5sum
from toolz import count, cons, drop, take
from toolz import assoc, update_in
from toolz import pipe


def pack_val_string(xml_row):
    """Pack the metadata in the other columns into the val dict

    If specified by the user, the other key value pairs (json_key,
    md5_checksum, file_name) will be added into the val dictionary for
    assiting with mapping values to work tables

    Arguments
        xml_row -- Dictionary from list of denormalized xml

    Returns
        xml_row -- Dictionary with the same keys, but a packed val

    """

    for key, value in xml_row.items():

        if key != 'val':
            # convert python lists into comma separated strings
            if key in ['json_key', 'json_key_minus_one', 'md5_list', 'md5_list_minus_one']:

                value = ', '.join(value)

            xml_row['val'][key] = value

    return xml_row


def alter_mappings(value_map, alterations=None):
    """Alter a mapping if it's present in the alterations dictionary"""

    if not alterations:
        alterations = dict()

    # create a copy to assign new keys and avoid mutating original
    mapping = {key: value for key, value in value_map.items()}

    for key, value in value_map.items():

        stems = key.split('_')
        tag = stems[-1]

        if key in alterations.keys():

            sources = alterations[key]['source']
            targets = alterations[key]['target']

            if len(sources) > 1:
                # Concatenate
                # Check for both keys in entire mapping

                source_vals = list()
                for source_key in sources:

                    source_path = stems.copy()
                    source_path[-1] = source_key

                    if '_'.join(source_path) in value_map.keys():
                        source_vals.append(value_map['_'.join(source_path)])

                target_path = stems.copy()
                target_path[-1] = targets[0]
                target_val = ' '.join(source_vals)
                mapping['_'.join(target_path)] = target_val

            elif len(sources) == 1:
                # Split

                splits = alterations[tag]['splits']

                for target, (begin, end) in zip(targets, splits):

                    target_path = stems.copy()
                    target_path[-1] = target
                    mapping['_'.join(target_path)] = value_map[key][begin:end]

    return mapping


def update_md5(processed_row):
    """Update checksum to include filename in hashing process"""

    updated_hash = md5sum(processed_row['md5_checksum'] + processed_row['file_name'])
    if len(processed_row['md5_list_minus_one']) > 0:
        hash_update_pipeline = (
            lambda r: update_in(r, ['md5_list'], lambda f: list(drop(1, f))),
            lambda r: update_in(r, ['md5_list'], lambda f: list(cons(updated_hash, f))),
            lambda r: update_in(r, ['md5_list_minus_one'], lambda f: list(drop(1, f))),
            lambda r: update_in(r, ['md5_list_minus_one'], lambda f: list(cons(updated_hash, f))),
            lambda r: assoc(r, 'md5_checksum', updated_hash)
        )

    else:
        hash_update_pipeline = (
            lambda r: update_in(r, ['md5_list'], lambda f: list(drop(1, f))),
            lambda r: update_in(r, ['md5_list'], lambda f: list(cons(updated_hash, f))),
            lambda r: assoc(r, 'md5_checksum', updated_hash)
        )

    return pipe(processed_row, *hash_update_pipeline)
