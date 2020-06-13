from prefect import Parameter, Flow

import get_datamelbourne as get_datamelbourne
import get_thebrag as get_thebrag
from prefect import task
import lib.bigquery as bigquery

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
    # extract get gig_details
    thebrag_gig_details = get_thebrag.extract_gig_details(thebrag_gigs)
    load_json_data(thebrag_gig_details,'thebrag_gig_details')

    # datamelbourne tasks
    datamelbourne_music_venue = get_datamelbourne.extract_music_venue()
    load_json_data(datamelbourne_music_venue,'datamelbourne_music_venue')


state = flow.run()

assert state.is_successful()
