from prefect import Parameter, Flow
from prefect import task
from prefect.engine.executors import DaskExecutor


import lib.bigquery         as bigquery
import config               as config

# import extract scripts
import get_datamelbourne    as get_datamelbourne
import get_thebrag          as get_thebrag
import get_geocode          as get_geocode
import get_moshtix          as get_moshtix
import get_bandcamp         as get_bandcamp
# common task to load json data into bigquery
@task
def load_json_data(data,table):
    job_result = bigquery.load_json_data(data,table)
    return(job_result)


def main():
    with Flow("ELT") as flow:

        """

        Gigs scraped from the brag

        """
        # extract gigs
        thebrag_gigs = get_thebrag.extract_gigs()
        load_json_data(thebrag_gigs,'thebrag_gigs')
        # search for thebrag artists on bandcamp
        thebrag_bandcamp_artist_details = get_bandcamp.extract_artist_search(thebrag_gigs,'gig_artist_list')
        load_json_data(thebrag_bandcamp_artist_details,'bandcamp_artist_search')

        # get albums for bandcamp artists
        thebrag_bandcamp_artist_albums = get_bandcamp.extract_artist_albums(thebrag_bandcamp_artist_details,'bandcamp_url')
        load_json_data(thebrag_bandcamp_artist_albums,'bandcamp_artist_albums')

        # get album details for those albums
        thebrag_bandcamp_artist_album_details = get_bandcamp.extract_album_details(thebrag_bandcamp_artist_albums,'bandcamp_album_url')
        load_json_data(thebrag_bandcamp_artist_album_details,'bandcamp_album_details')

        # extract gig_details
        thebrag_gig_details = get_thebrag.extract_gig_details(thebrag_gigs)
        load_json_data(thebrag_gig_details,'thebrag_gig_details')
        # geocode gig detail addresses
        thebrag_geocoded_addresses = get_geocode.extract_geocode(thebrag_gig_details,'gig_location_address')
        load_json_data(thebrag_geocoded_addresses,config.geocode_result_table)
        # get additional gig details from links
        thebrag_moshtix_gig_details = get_moshtix.extract_gig_details(thebrag_gig_details,'gig_ticket_url')
        load_json_data(thebrag_moshtix_gig_details,'moshtix_gig_details')

        # datamelbourne tasks
        #datamelbourne_music_venue = get_datamelbourne.extract_music_venue()
        #load_json_data(datamelbourne_music_venue,'datamelbourne_music_venue')
        # geocode melbourne music

        #

    state = flow.run()
    #flow.run(executor=DaskExecutor())

    assert state.is_successful()


if __name__ == "__main__":
    main()
