from prefect import task
from urllib.parse import urlparse

import lib.oztix as oztix

@task
def extract_gig_details(input_data,gig_url_field):
    results = []
    # get urls from input data
    gig_urls = [i.get(gig_url_field) for i in input_data]
    # iterate
    for url in gig_urls:
        # validate that url is a moshtix url, then scrape
        parsed_url = urlparse(url)
        if(parsed_url.netloc == 'tickets.oztix.com.au'):
            try:
                results.append(oztix.extract_ticket_details(url))
            except:
                print("Failed")
    return(results)
