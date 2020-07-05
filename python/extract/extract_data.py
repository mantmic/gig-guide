from prefect import Parameter, Flow
from prefect import task
from prefect.engine.executors import DaskExecutor
import datetime

import lib.gcp              as gcp
import config               as config

# import extract scripts
import tasks.get_datamelbourne    as get_datamelbourne
import tasks.get_thebrag          as get_thebrag
import tasks.get_geocode          as get_geocode
import tasks.get_moshtix          as get_moshtix
import tasks.get_bandcamp         as get_bandcamp
import tasks.get_unearthed        as get_unearthed
import tasks.get_spotify          as get_spotify 
import tasks.get_google           as get_google

# evaluate the extract timestamp for all files
extract_ts = datetime.datetime.now().isoformat().replace(':','').replace('.','')
# common task to load json data into bigquery
@task
def load_json_data(data,table,source_system):
    if len(data) > 0:
        # create directory path
        directory = '/'.join([source_system,table])
        file_prefix = '_'.join([table,datetime.datetime.now().isoformat().replace(':','').replace('.','')])
        job_result = gcp.load_json_data(data,directory,file_prefix)
        return(job_result)
    else:
        print("No data")
        return


def main():
    with Flow("ELT") as flow:

        """

        Gigs scraped from the brag

        """
        # extract gigs
        thebrag_gigs = get_thebrag.extract_gigs()
        load_json_data(thebrag_gigs,'gigs','thebrag')

        # search arists on google 
        thebrag_artist_google_search = get_google.extract_google_search(thebrag_gigs,'gig_artist_list')
        load_json_data(thebrag_artist_google_search,'search','google')

        # geocode artist google events 
        thebrag_artist_google_search_event_geocode = get_geocode.extract_geocode(thebrag_artist_google_search,'event_venue_locations')
        load_json_data(thebrag_artist_google_search_event_geocode,'results','geocode')

        # search arists on spotify 
        thebrag_artist_spotify = get_spotify.extract_artist_search(thebrag_gigs,'gig_artist_list')
        load_json_data(thebrag_artist_spotify,'artist_search','spotify')

        # get artist top spotify tracks 
        thebrag_artist_spotify = get_spotify.extract_artist_top_tracks(thebrag_artist_spotify,'result_artist_ids')
        load_json_data(thebrag_artist_spotify,'artist_top_tracks','spotify')

        # search for artists on JJJ unearthed
        thebrag_unearthed_artist_search = get_unearthed.extract_artist_search(thebrag_gigs,'gig_artist_list')
        load_json_data(thebrag_unearthed_artist_search,'artist_search','unearthed')
        
        # get details from artist urls
        thebrag_unearthed_artist_details = get_unearthed.extract_artist_details(thebrag_unearthed_artist_search,'unearthed_artist_url')
        load_json_data(thebrag_unearthed_artist_details,'artist_details','unearthed')

        # search for thebrag artists on bandcamp
        thebrag_bandcamp_artist_details = get_bandcamp.extract_artist_search(thebrag_gigs,'gig_artist_list')
        load_json_data(thebrag_bandcamp_artist_details,'artist_search','bandcamp')

        # get albums for bandcamp artists
        thebrag_bandcamp_artist_albums = get_bandcamp.extract_artist_albums(thebrag_bandcamp_artist_details,'bandcamp_url')
        load_json_data(thebrag_bandcamp_artist_albums,'artist_albums','bandcamp')

        # get album details for those albums
        thebrag_bandcamp_artist_album_details = get_bandcamp.extract_album_details(thebrag_bandcamp_artist_albums,'bandcamp_album_url')
        load_json_data(thebrag_bandcamp_artist_album_details,'album_details','bandcamp')

        # geocode shows detailed on bandcamp pages
        thebrag_bandcamp_artist_geocoded = get_geocode.extract_geocode(thebrag_bandcamp_artist_album_details,'show_locations')
        load_json_data(thebrag_bandcamp_artist_geocoded,'results','geocode')

        # extract gig_details
        thebrag_gig_details = get_thebrag.extract_gig_details(thebrag_gigs)
        load_json_data(thebrag_gig_details,'gig_details','thebrag')
        
        # geocode gig detail addresses
        thebrag_geocoded_addresses = get_geocode.extract_geocode(thebrag_gig_details,'gig_location_address')
        load_json_data(thebrag_geocoded_addresses,'results','geocode')
        
        # get additional gig details from links
        thebrag_moshtix_gig_details = get_moshtix.extract_gig_details(thebrag_gig_details,'gig_ticket_url')
        load_json_data(thebrag_moshtix_gig_details,'gig_details','moshtix')

    state = flow.run()
    #flow.run(executor=DaskExecutor())

    assert state.is_successful()


if __name__ == "__main__":
    main()
