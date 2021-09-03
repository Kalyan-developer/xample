"""Library for assisting in parsing PRS XML"""

from collections.abc import Mapping

from toolz import valmap


def undo_cdata(parsed_xml, parent_key='', sep='_', cdata='#text'):
    """Correct an annoying but necessary feature of xmltodict's parse method

    This method mimics the flatten method provided by py_chubb_xml but only
    flattens where character data is present

    """

    items = []
    if not isinstance(parsed_xml, dict):
        parsed_xml = {'value': parsed_xml}

    for k, v in parsed_xml.items():

        if parent_key != "":
            new_key = f'{parent_key}{sep}{k}'

            if new_key[-len(sep + cdata):] == sep + cdata:
                new_key = new_key[:-len(sep + cdata)]

        else:
            new_key = k

        if isinstance(v, Mapping):

            if k == "":
                raise ValueError("Keys cannot be empty strings.")

            if cdata in list(v.keys()):
                items.extend(undo_cdata(v, new_key, sep=sep, cdata=cdata).items())

            else:

                sub_items = []
                for ke, va in v.items():

                    if isinstance(va, Mapping):

                        if cdata in va.keys():
                            sub_items.extend((undo_cdata(va, ke, sep=sep, cdata=cdata).items()))

                        else:
                            sub_items.append((ke, undo_cdata(va)))

                    elif isinstance(va, list):
                        sub_items.append((ke, list(map(lambda x: undo_cdata(x, "", sep=sep, cdata=cdata), va))))

                    else:
                        sub_items.append((ke, va))

                items.append((new_key, dict(sub_items)))

        elif isinstance(v, list):
            items.append((new_key, list(map(lambda x: undo_cdata(x, "", sep=sep), v))))

        else:
            items.append((new_key, v))

    return dict(items)


def create_force_list_callable(keys, paths):
    """logic to decide if a list needs to be forced

    A GLOBAL LIST OF keys AND paths IS KEPT TO PREVENT MULTIPLE FILE LOADS

    This method performs logical checks to determine if a tag level needs to be
    forced into a list by xmltodict's parser. The only tricky argument to
    conceptualize is the parenth_path which takes on a list of tuples:
        [('name', attribute dictionary), ...] or
        [('name', None)]
        an attribute dictionary is an OrderedDict (probably based on
        self.dict_constructor) that takes the name and value of the attributes
        at that level

    Any value returned by this function will trigger the parser to force the
    current tag into a list so a boolean True is used for simplicity.

    Arguments
        parent_path -- list of tuples taking on the form
        current_key -- name of the current tag
        value -- any character data currently present at this tag level

    Return
        True -- When logical checks pass
        None -- When no checks pass
    """

    def __inner(parent_path, current_key, value):

        if current_key in keys:

            if current_key in paths.keys():

                for path in paths[current_key]:

                    xpath_parent = [x[0] for x in parent_path]

                    if xpath_parent == path:
                        return True

            else:
                return True

        return False

    return __inner


def replace_None(value_dict):
    """Replace all the occurences of None in dictionary values"""

    return valmap(lambda x: "" if x is None else x, value_dict)