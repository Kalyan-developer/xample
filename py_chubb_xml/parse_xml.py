 #!/usr/bin/env python -W ignore::DeprecationWarning
 #
from collections.abc import Mapping
from toolz import assoc, dissoc, first, thread_last
from py_chubb_xml.util import md5sum
from typing import Generator
try:
    # This is faster if availible
    import simplejson as json
except ImportError:
    import json

MAPTYPES = {"str": "text",
            "int": "bigint",
            "bool": "bit",
            "float": "float"}

def flatten(d: dict, parent_key: str="", sep: str="_") -> dict:
    """Given dict d, flatten (recursively) where possible. Recurse into lists
    where required. Note that the flatten sep must be different than the key
    delimiter; I've been using "_" to flatten and ">" to denote key tokens.

    >>> d = {"a": 1,
    ...      "b": 2,
    ...      "c": {"sub-a": "one",
    ...            "sub-b": "two",
    ...            "sub-c": "three"}}

    >>> flatten(d) == {"a": 1,
    ...                "b": 2,
    ...                "c_sub-a": "one",
    ...                "c_sub-b": "two",
    ...                "c_sub-c": "three"}
    True

    """

    items = []
    if not isinstance(d, dict):
        d = {"value": d}

    for k, v in d.items():
        if parent_key != "":
            new_key = "{0}{1}{2}".format(parent_key, sep, k)
        else:
            new_key = k

        if isinstance(v, Mapping):
            if k == "":
                raise ValueError("Keys cannot be empty strings.")
            items.extend(flatten(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # apply itself to each element of the list - that's it!
            items.append((new_key, list(map(lambda x: flatten(x, "", sep=sep), v))))
        else:
            items.append((new_key, v))

    return dict(items)


def denormalize_dicts(d, md5_list: list=[], tree: list=['r']) -> Generator:
    # can't avoid mutable arguments
    """With input dict d, recur yielding a list of dictionaries denormalized
    including lists and sub-dictionaries.

    If (recursive) input is list, enumerate to yield each individual dict
    inside this list (denormalized).

    Does not yield strings as they are to be recursed over after we stabilize
    the relational sets.

    Each dict in the output entry contains the following keys:
    - data_type -> of which is list or dict
    - json_key -> a conjoined string of each element with it's parent elements; in
      alignment with the md5's for each value.
    - md5_list -> an array containing the checksums of the value of this list, as
      well as previous parents md5 checksums.
    - val -> the value of this particular dictionary or list.

    >>> d = {'aaa': {111: 'a'}}

    >>> expected = [{'data_type': 'dict',
                     'json_key': ['r'],
                     'md5_list': ['12cfdc1bad22a25b3502a9237fd57e80'],
                     'val': {'aaa': {111: 'a'}}},
                    {'data_type': 'dict',
                     'json_key': ['r', 'aaa'],
                     'md5_list': ['12cfdc1bad22a25b3502a9237fd57e80',
                      '3d1848556a8ed2534deb301d738d6c8d'],
                     'val': {111: 'a'}},
                    {'data_type': 'text',
                     'json_key': ['r', 'aaa', 111],
                     'md5_list': ['12cfdc1bad22a25b3502a9237fd57e80',
                      '3d1848556a8ed2534deb301d738d6c8d',
                      '6067924ae1b1832abce3d12fe83755a9'],
                     'val': 'a'}]

    >>> result = list(denormalize_dicts(d))
    >>> result == expected
    True

    """

    # main logic
    if True: #d is not None:
        yield_dict = {"data_type": MAPTYPES.get(type(d).__name__, type(d).__name__),
                      "json_key": tree,
                      "md5_list": list(md5_list) + [md5sum(json.dumps(d))],
                      "val": d}

        if isinstance(d, dict):
            yield yield_dict
            for key, val in d.items():
                yield from denormalize_dicts(val, yield_dict["md5_list"], list(tree)+[key])
        elif isinstance(d, (list, tuple, set)):
            if len(d) == 0:
                yield yield_dict

            for val in d:
                yield from denormalize_dicts(val, md5_list, tree)
        else:
            yield yield_dict


def remove_redundant_parts_from_dict(d: dict) -> dict:
    """Removes key/value pairs from d["val"] where the value type is a list

    >>> data = {'val': {'aaa': 1,
    ...                 'bbb': [1, 2, 3],
    ...                 'ccc': 3}}

    >>> expected = {'val': {'aaa': 1,
    ...                     'ccc': 3}}

    >>> remove_lists_from_dict(data) == expected
    True
    """

    d["val"] = {k: v for k, v in d["val"].items() if not isinstance(v, (list, dict))}
    return d


def denormalize(d: dict) -> list:
    """With input dict d, return a list with all the information in d.

    The list return does not include sub-lists and sub-dictionaries. (Removes
    redundant data).

    Uses `dernormalize_dicts` to return process thorugh a generat of input.

    It also further enriches the `denormalize_dicts` output with values useful
    for database querying.

    Output values, for each item in return dict:

    Further associates values on the each record in
    Each dict in the output entry contains the following keys:

    - json_key -> a conjoined string of each element with it's parent elements; in
      alignment with the md5's for each value.
    - md5_list -> an array containing the checksums of the value of this list,
      as well as previous parents md5 checksums.
    - val -> the value of this particular dictionary or list.
    - md5_checksum -> this is the "key" of the entire document. This groups all
      the data for this xml doc.
    - md5_list -> this is each md5 of each value throughout the list.
    - md5_list_minus_one -> this is the md5_list, with the last entry in the
      list dropped. This can be used to "join" the relational set to it's
      parent structure.
    - json_Key_minus_one -> this is the parent stuctures "name".

    See the usage section of the readme for what these values are intended to do.

    >>> d = {'aaa': {111: 'a'}}
    >>> expected = [{'json_key': ['r'],
                     'md5_list': ['12cfdc1bad22a25b3502a9237fd57e80'],
                     'val': {'aaa': {111: 'a'}},
                     'md5_checksum': '12cfdc1bad22a25b3502a9237fd57e80',
                     'md5_list_minus_one': [],
                     'json_key_minus_one': []},
                    {'json_key': ['r', 'aaa'],
                     'md5_list': ['12cfdc1bad22a25b3502a9237fd57e80', '3d1848556a8ed2534deb301d738d6c8d'],
                     'val': {111: 'a'},
                     'md5_checksum': '12cfdc1bad22a25b3502a9237fd57e80',
                     'md5_list_minus_one': ['12cfdc1bad22a25b3502a9237fd57e80'],
                     'json_key_minus_one': ['r']}]

    >> result = denormalize(d)
    >>> result == expected
    True


    """

    try:
        # This should be a generator
        val = denormalize_dicts(d)

        pipe = [val, # this is the start, dict output from xmltodict
                # flatten any nested objects we can
                # filter out redundant types, dicts should have everything
                (filter, lambda x: x["data_type"] in ("OrderedDict", "dict")),
                # remove any sub-lists from the dicts, again, redundant
                (map, remove_redundant_parts_from_dict),

                (map , lambda x: assoc(x  , "md5_checksum"     , first(x["md5_list"]))) ,
                (map , lambda x: dissoc(x , "data_type"))      ,

                # This is for joins to parents
                (map, lambda x: assoc(x, "md5_list_minus_one", x["md5_list"][:-1])),
                (map, lambda x: assoc(x, "json_key_minus_one", x["json_key"][:-1])),

                # also a pk (of the parent)

                # make json friendly
                # remove newlines, just in case.
                # TODO should we do this or is it application specific?
                # (map, lambda x: update_in(x, ["val"], if_string_remove_crnl)),
                list
               ]

        return thread_last(*pipe)
    except Exception as exc:
        # append the MicroMessageDetailId to the existing error message
        #exc.args = tuple(list(exc.args) + ["MicroMessageDetailId: {}".format(record["MicroMessageDetailId"])])
        exc.args = tuple(list(exc.args))
        raise exc
