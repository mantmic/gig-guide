import lib.thebrag as thebrag
import datetime
import os
from prefect import task

# extract these many days
extract_days = int(os.getenv('GIG_EXTRACT_DAYS', 1))

# extract each of these cities
cities = ['sydney','melbourne']

#set start date to today
start_date = datetime.datetime.now()

@task
def extract_gigs():
    results = []
    # iterate by city
    for city in cities:
        print("Extracting city %s" % city)
        # iterate by date
        for i in range(extract_days):
            date = start_date + datetime.timedelta(days = i)
            print("Extracting date %s" % date.isoformat())
            try:
                gigs = thebrag.get_gigs(date,city)
                results.extend(gigs)
            except:
                print("Failed")
    return(results)

@task
def extract_gig_details(gigs):
    results = []
    # iterate by city
    for gig in gigs:
        try:
            print("Getting gig details for url %s" % gig.get('gig_url'))
            gig_details = thebrag.get_gig_details(gig.get('gig_url'))
            results.append(gig_details)
        except:
            print("Failed")
    return(results)
