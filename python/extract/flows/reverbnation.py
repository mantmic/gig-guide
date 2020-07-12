from prefect import Parameter, Flow
from prefect import task
from prefect.engine.executors import DaskExecutor
import datetime

import lib.gcp              as gcp
import config               as config

# import extract scripts
import tasks.get_reverbnation     as get_reverbnation
import tasks.get_geocode          as get_geocode
import tasks.get_bandcamp         as get_bandcamp
import tasks.get_unearthed        as get_unearthed
import tasks.get_spotify          as get_spotify 
import tasks.get_google           as get_google
import tasks.common

# common task to load json data into bigquery

load_json_data = tasks.common.load_json_data

with Flow("Reverbnation ELT") as flow:

    """

    Gigs scraped from the brag

    """
    # extract gigs
    venue_shows = get_reverbnation.extract_venue_shows()
    load_json_data(venue_shows,'venue_shows','reverbnation')

    # search arists on google 
    artist_google_search = get_google.extract_google_search(venue_shows,'venue_artists')
    load_json_data(artist_google_search,'search','google')

    # geocode artist google events 
    artist_google_search_event_geocode = get_geocode.extract_geocode(artist_google_search,'event_venue_locations')
    load_json_data(artist_google_search_event_geocode,'results','geocode')

    # search arists on spotify 
    artist_spotify = get_spotify.extract_artist_search(venue_shows,'venue_artists')
    load_json_data(artist_spotify,'artist_search','spotify')

    # get artist top spotify tracks 
    spotify_top_tracks = get_spotify.extract_artist_top_tracks(artist_spotify,'result_artist_ids')
    load_json_data(spotify_top_tracks,'artist_top_tracks','spotify')

    # search for artists on JJJ unearthed
    unearthed_artist_search = get_unearthed.extract_artist_search(venue_shows,'venue_artists')
    load_json_data(unearthed_artist_search,'artist_search','unearthed')
    
    # get details from artist urls
    unearthed_artist_details = get_unearthed.extract_artist_details(unearthed_artist_search,'unearthed_artist_url')
    load_json_data(unearthed_artist_details,'artist_details','unearthed')

    # search for thebrag artists on bandcamp
    bandcamp_artist_details = get_bandcamp.extract_artist_search(venue_shows,'venue_artists')
    load_json_data(bandcamp_artist_details,'artist_search','bandcamp')

    # get albums for bandcamp artists
    bandcamp_artist_albums = get_bandcamp.extract_artist_albums(bandcamp_artist_details,'bandcamp_url')
    load_json_data(bandcamp_artist_albums,'artist_albums','bandcamp')

    # get album details for those albums
    bandcamp_artist_album_details = get_bandcamp.extract_album_details(bandcamp_artist_albums,'bandcamp_album_url')
    load_json_data(bandcamp_artist_album_details,'album_details','bandcamp')

    # geocode shows detailed on bandcamp pages
    bandcamp_artist_geocoded = get_geocode.extract_geocode(bandcamp_artist_album_details,'show_locations')
    load_json_data(bandcamp_artist_geocoded,'results','geocode')
    
    # geocode venues
    geocoded_addresses = get_geocode.extract_geocode(venue_shows,'venue_location_full')
    load_json_data(geocoded_addresses,'results','geocode')

