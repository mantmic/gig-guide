import lib.bandcamp as bandcamp
import lib.gcp      as bigquery

import config

from prefect import task
import collections
import os
import time
import datetime

def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el

def get_extracted_artist_names(expiry_period_days = 90):
    '''
    Function to get artist names have already been searched in bandcamp

    Args:
        expriy_period_days (int)
            The number of days before an existing result expires
    Returns:
        dict: Dictionary of all extracted entities

    '''
    table_name = 'bandcamp_artist_search'
    if(bigquery.check_table_exists(table_name)):
        results = {}
        # Get extracted results
        min_extract_ts = datetime.datetime.now() - datetime.timedelta(days=expiry_period_days)
        sql_query = """
        select
            input_artist_name
        from
            {}.{}
        where
            extract_ts > '{}'
        """.format(config.bigquery_dataset_id,table_name,min_extract_ts.isoformat())
        extracted_entities = bigquery.get_query(sql_query)
        # orient into dictionary
        for record in extracted_entities:
            results[record['input_artist_name']] = True
        return(results)
    else:
        return({})


@task
def extract_artist_search(input_data,artist_name_field):
    results = []
    # get urls from input data
    artist_names = [i.get(artist_name_field) for i in input_data]
    # flatten list
    artist_names = flatten(artist_names)
    # get unique artists
    artist_names = list(set(artist_names))

    # get already extracted artists
    extracted_artist_names = get_extracted_artist_names()

    # iterate
    for artist_name in artist_names:
        # do not extract if artist has already been extracted
        if(extracted_artist_names.get(artist_name)):
            continue
        print("Searching for artist %s" % artist_name)
        try:
            results.extend(bandcamp.get_bandcamp_search(artist_name))
        except:
            print("Failed")
    return(results)

@task
def extract_artist_albums(input_data,bandcamp_url_field):
    results = []
    # get urls from input data
    bandcamp_urls = [i.get(bandcamp_url_field) for i in input_data]
    # iterate
    for bandcamp_url in bandcamp_urls:
        try:
            results.extend(bandcamp.get_bandcamp_albums(bandcamp_url))
        except:
            print("Failed")
    return(results)

@task
def extract_album_details(input_data,bandcamp_album_url_field):
    results = []
    # get urls from input data
    bandcamp_album_urls = [i.get(bandcamp_album_url_field) for i in input_data]
    # iterate
    for bandcamp_album_url in bandcamp_album_urls:
        try:
            results.append(bandcamp.get_bandcamp_album_details(bandcamp_album_url))
        except:
            print("Failed")
    return(results)
