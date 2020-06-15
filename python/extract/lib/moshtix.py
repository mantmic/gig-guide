import datetime
import lib.scrape as scrape
import requests
from urllib.parse import urlparse

# set the extract ts
extract_ts = datetime.datetime.now().isoformat()

def get_gig_details(url):
    '''
    Function to get the details from a moshtix gig url

    Args:
        url: Url to the moshtix gig
    Return:
        object: Gig details
    '''

    parsed_url = urlparse(url)

    soup = scrape.get_soup(url)

    if(soup == None):
        return({})

    gig_datetime = soup.find('div',id='event-summary-date').text

    gig_venue_name = soup.find('span',class_='event-venue-name').text

    gig_venue_url = ''.join([parsed_url.netloc,soup.find('a',id='event-summary-venue').get('href')])

    gig_details = {
        "url":url,
        "extract_ts":extract_ts,
        "gig_datetime":gig_datetime,
        "gig_venue_name":gig_venue_name
    }

    # get ticket prices if they exist
    ticket_type_objects = soup.findAll('ul',class_='event-ticket-type-list')
    if(len(ticket_type_objects) > 0):
        ticket_types = []
        for t in ticket_type_objects:
            ticket_types.append({
                "ticket_type_name":t.find('span',class_='ticket-type-name').text,
                "ticket_price":t.find('div',class_='ticket-type-costs').text
            })
        gig_details['ticket_types'] = ticket_types

    return(gig_details)
