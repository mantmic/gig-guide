import lib.geocode  as geocode
import lib.gcp      as bigquery
import config       as config

import datetime
from prefect import task

geocode_result_table = 'geocode_results'

def get_geocoded_addresses(geocode_provider = 'arcgis', expiry_period_days = 90):
    '''
    Function to get addresses that have already been geocoded

    Args:
        geocode_provider (str)
            The name of the geocode provider to check existing results for
        expriy_period_days (int)
            The number of days before an existing result expires
    Returns:
        dict: Dictionary of all extracted addresses

    '''
    if(bigquery.check_table_exists(geocode_result_table)):
        results = {}
        # Get extracted results
        min_extract_ts = datetime.datetime.now() - datetime.timedelta(days=expiry_period_days)
        sql_query = """
        select
            input_address
        from
            {}.{}
        where
            geocode_provider = '{}'
            and extract_ts > '{}'
        """.format(config.bigquery_dataset_id,geocode_result_table,geocode_provider,min_extract_ts.isoformat())
        geocoded_addresses = bigquery.get_query(sql_query)
        # orient into dictionary
        for a in geocoded_addresses:
            results[a['input_address']] = True
        return(results)
    else:
        return({})

def get_geocode(addresses = []):
    # get unique items in list
    unique_addresses = list(set(addresses))
    # initialise results
    results = []
    # get already geocoded adddresses
    geocoded_addresses = get_geocoded_addresses()
    for address in unique_addresses:
        # if address already exists, skip
        if(geocoded_addresses.get(address)):
            pass
        else:
            print("Geocoding address %s" % address)
            try:
                g = geocode.get_geocode(address)
                results.append(g)
            except:
                print("Failed to get geocode")
    return(results)

@task
def extract_geocode(input_data,address_field):
    # get addresses from input data
    addresses = [i.get(address_field) for i in input_data]
    return(get_geocode(addresses))
