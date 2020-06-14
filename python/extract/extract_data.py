from prefect import Parameter, Flow
from prefect import task
import lib.bigquery         as bigquery
import config               as config

# import extract scripts
import get_datamelbourne    as get_datamelbourne
import get_thebrag          as get_thebrag
import get_geocode          as get_geocode
import get_moshtix          as get_moshtix
# common task to load json data into bigquery
@task
def load_json_data(data,table):
    job_result = bigquery.load_json_data(data,table)
    return(job_result)


with Flow("ELT") as flow:
    # thebrag tasks
    # extract gigs
    thebrag_gigs = get_thebrag.extract_gigs()
    load_json_data(thebrag_gigs,'thebrag_gigs')

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


state = flow.run()

assert state.is_successful()
