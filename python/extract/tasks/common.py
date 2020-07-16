from prefect import task

import datetime

import lib.gcp              as gcp
import config               as config


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
