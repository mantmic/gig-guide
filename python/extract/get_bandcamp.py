import lib.bandcamp as bandcamp
from prefect import task
import collections
import os
import time


def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
            yield from flatten(el)
        else:
            yield el

@task
def extract_artist_search(input_data,artist_name_field):
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
