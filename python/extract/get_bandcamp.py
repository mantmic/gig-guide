import lib.bandcamp as bandcamp
from prefect import task
import collections
import os
import time

# extract these many pages before sleeping
page_batch_size = int(os.getenv('PAGE_BATCH_SIZE', 7))

# sleep these many seconds between pages
page_batch_sleep_time = int(os.getenv('PAGE_BATCH_SLEEP_TIME', 30))

def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el

@task
def extract_artist_search(input_data,artist_name_field):
    page_batch_index = 0
    results = []
    # get urls from input data
    artist_names = [i.get(artist_name_field) for i in input_data]
    # flatten list
    artist_names = flatten(artist_names)
    # get unique artists
    artist_names = list(set(artist_names))
    # iterate
    for artist_name in artist_names:
        print("Searching for artist %s" % artist_name)
        try:
            results.extend(bandcamp.get_bandcamp_search(artist_name))
        except:
            print("Failed")
            print("Sleeping %s seconds" % page_batch_sleep_time)
        page_batch_index +=1
        if page_batch_index % page_batch_size == 0:
            print("Sleeping %s seconds" % page_batch_sleep_time)
            time.sleep(page_batch_sleep_time)
    return(results)

@task
def extract_artist_albums(input_data,bandcamp_url_field):
    page_batch_index = 0
    results = []
    # get urls from input data
    bandcamp_urls = [i.get(bandcamp_url_field) for i in input_data]
    # iterate
    for bandcamp_url in bandcamp_urls:
        print("Scraping url %s" % bandcamp_url)
        try:
            results.extend(bandcamp.get_bandcamp_albums(bandcamp_url))
        except:
            print("Failed")
            print("Sleeping %s seconds" % page_batch_sleep_time)
        page_batch_index +=1
        if page_batch_index % page_batch_size == 0:
            print("Sleeping %s seconds" % page_batch_sleep_time)
            time.sleep(page_batch_sleep_time)
    return(results)

@task
def extract_album_details(input_data,bandcamp_album_url_field):
    page_batch_index = 0
    results = []
    # get urls from input data
    bandcamp_album_urls = [i.get(bandcamp_album_url_field) for i in input_data]
    # iterate
    for bandcamp_album_url in bandcamp_album_urls:
        print("Scraping url %s" % bandcamp_album_url)
        try:
            results.append(bandcamp.get_bandcamp_album_details(bandcamp_album_url))
        except:
            print("Failed")
            print("Sleeping %s seconds" % page_batch_sleep_time)
        page_batch_index +=1
        if page_batch_index % page_batch_size == 0:
            print("Sleeping %s seconds" % page_batch_sleep_time)
            time.sleep(page_batch_sleep_time)
    return(results)
