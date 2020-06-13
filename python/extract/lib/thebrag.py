import datetime
from bs4 import BeautifulSoup
import requests

def get_gigs(date = datetime.datetime.now(),city = 'melbourne'):
    # set the extract ts
    extract_ts = datetime.datetime.now().isoformat()
    #format url
    url = "https://thebrag.com/gigs/?title=&artist=&gig_date={}+{}+{}&city={}&search=Search".format(date.day,date.strftime('%b'),date.year,city)
    print("extracting url %s" % url)
    res = requests.get(url)
    soup = BeautifulSoup(res.text,'html.parser')

    # get each gig
    gigs = soup.findAll("div",class_="gig-title")
    results = []

    for gig in gigs:
        gig_name = gig.text
        gig_url = gig.findChild('a').get('href')
        gig_artist = gig.findNext('div',class_='gig-artist').text
        gig_location = gig.findNext('div',class_='gig-location').text
        gig_location_url = gig.findNext('div',class_='gig-location').findChild('a').get('href')
        # append record
        results.append({
            'gig_date':date.isoformat(),
            'gig_city':city,
            'gig_name':gig_name,
            'gig_url':gig_url,
            'gig_artist':gig_artist,
            'gig_location':gig_location,
            'gig_location_url':gig_location_url,
            'extract_ts':extract_ts
        })
    #return results
    return(results)

# get details for a gig url
def get_gig_details(url):
    extract_ts = datetime.datetime.now().isoformat()
    res = requests.get(url)
    soup = BeautifulSoup(res.text,'html.parser')

    # replace breaks with spaces to avoid strings being squished
    for br in soup.find_all('br'):
        br.replace_with(br.text + " ")

    # get location address
    gig_location_address = soup.find('th',text='Where').findNext('td').text

    # get ticket url
    gig_ticket_url = soup.find('th',text='Ticket Information').findNext('a').get('href')

    # get gig artists
    gig_artist = soup.find('th',text='Artists').findNext('td').text
    # return object
    return({
        "gig_url":url,
        "gig_location_address":gig_location_address,
        "gig_ticket_url":gig_ticket_url,
        "gig_artist":gig_ticket_url,
        'extract_ts':extract_ts
    })
