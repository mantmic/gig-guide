import lib.unearthed    as unearthed
import lib.util         as util
import lib.gcp          as gcp

import config

from prefect import task
import datetime

def get_extracted_artist_names(expiry_period_days = 90):
    '''
    Function to get artist names have already been searched in bandcamp

    Args:
        expriy_period_days (int)
            The number of days before an existing result expires
    Returns:
        dict: Dictionary of all extracted entities

    '''
    table_name = 'unearthed_artist_search'
    min_file_name = gcp.get_file_path('unearthed','artist_search',datetime.datetime.now() - datetime.timedelta(days=expiry_period_days))
    if(gcp.check_table_exists(table_name)):
        results = {}
        # Get extracted results
        sql_query = """
        select
            search_artist_name
        from
            {}.{}
        where
            _file_name > '{}'
        """.format(config.bigquery_dataset_id,table_name,min_file_name)
        extracted_entities = gcp.get_query(sql_query)
        # orient into dictionary
        for record in extracted_entities:
            results[record['search_artist_name']] = True
        return(results)
    else:
        return({})


@task
def extract_artist_search(input_data,artist_name_field):
    results = []
    # get names from input data
    artist_names = [i.get(artist_name_field) for i in input_data]
    # flatten list
    artist_names = util.flatten(artist_names)
    # get unique artists
    artist_names = list(set(artist_names))
    # get already extracted artists
    extracted_artist_names = get_extracted_artist_names()
    # iterate
    for artist_name in artist_names:
        if(extracted_artist_names.get(artist_name)):
            continue
        print("Searching for artist %s" % artist_name)
        try:
            results.extend(unearthed.search_unearthed(artist_name))
        except Exception as e:
            print("Failed")
            print(e)

    return(results)

@task
def extract_artist_details(input_data,url_field):
    results = []
    # get urls from input data
    urls = [i.get(url_field) for i in input_data]
    # flatten list
    urls = util.flatten(urls)
    # get unique artists
    urls = list(set(urls))
    # iterate
    for url in urls:
        try:
            results.append(unearthed.get_artist_details(url))
        except Exception as e:
            print("Failed")
            print(e)
    return(results)
