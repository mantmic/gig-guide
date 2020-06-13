import requests
import datetime
from prefect import task

@task
def extract_music_venue() -> object:
    """
    Extracts music venues from datamelbourne
    """
    # set the extract ts
    extract_ts = datetime.datetime.now().isoformat()
    url = "https://data.melbourne.vic.gov.au/resource/mgqj-necz.json"
    res = requests.get(url)
    data = res.json()
    # add extract_ts to data
    for item in data:
        item.update({"extract_ts":extract_ts})
    return(data)
