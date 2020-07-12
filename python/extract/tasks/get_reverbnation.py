from prefect import task

import lib.reverbnation as reverbnation


@task
def extract_venue_shows():
    # get the venue urls to extract
    venue_urls = reverbnation.get_venue_urls()
    # initialise results
    results = []

    for venue_url in venue_urls:
        print("Getting %s" % venue_url)
        try:
            venue = reverbnation.extract_venue(venue_url)
            results.append(venue)
        except:
            print("failed")

    return(results)
