import datetime
from bs4 import BeautifulSoup
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
    print("extracting url %s" % url)

    parsed_url = urlparse(url)

    res = requests.get(url)
    soup = BeautifulSoup(res.text,'html.parser')

    # get ticket prices
    ticket_types = []

    ticket_type_objects = soup.findAll('ul',class_='event-ticket-type-list')

    for t in ticket_type_objects:
        ticket_types.append({
            "ticket_type_name":t.find('span',class_='ticket-type-name').text,
            "ticket_price":t.find('div',class_='ticket-type-costs').text
        })

    gig_datetime = soup.find('div',id='event-summary-date').text

    gig_venue_name = soup.find('span',class_='event-venue-name').text

    gig_venue_url = ''.join([parsed_url.netloc,soup.find('a',id='event-summary-venue').get('href')])

    return({
        "url":url,
        "extract_ts":extract_ts,
        "ticket_types":ticket_types,
        "gig_datetime":gig_datetime,
        "gig_venue_name":gig_venue_name
    })
