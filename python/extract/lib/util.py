import collections
import datetime

import config as config

def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el

def remove_empty_lists(d):
    '''
        Removes all empty lists from an input dictionary 
        Args:
            d (dict): Input dictionary to process 
        Returns: dict 
    '''
    d_copy = d.copy()
    for k, v in d_copy.items():
        if isinstance(v, dict):
            remove_empty_lists(d[k])
        elif v == []:
            d.pop(k)

def get_lookup(input_data,lookup_field):
    # pull the field from the list of dictionaries 
    lookup = [i.get(lookup_field) for i in input_data]
    # flatten in case there are lists of lists
    lookup = flatten(lookup)
    # get unique items
    lookup = list(set(lookup))
    # remove nulls 
    lookup = [l for l in lookup if l != None]
    return(lookup)

def get_extract_start_date():
    start_date = datetime.datetime.now()
    # apply offset
    start_date += datetime.timedelta(days=config.extract_start_date_offset)
    return(start_date)
