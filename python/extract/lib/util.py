import collections
import datetime

import config as config

def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el

def get_extract_start_date():
    start_date = datetime.datetime.now()
    # apply offset
    start_date += datetime.timedelta(days=config.extract_start_date_offset)
    return(start_date)
