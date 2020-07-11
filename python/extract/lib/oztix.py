import lib.scrape as scrape 
import urllib 

# Create the refinement list 
oztix_categories = ['Music','Rock','Australian Artists','POP','Alternative','Indie','Metal / Hard Rock','Punk','Acoustic','Blues','Folk','Heavy Metal','Country','Hip Hop']

refinement_parameters = {}
for i in range(len(oztix_categories)):
    refinement_parameters['refinementList[Categories][{}]'.format(i)] = '{}'.format(oztix_categories[i])


def extract_outlet_ticket_details(url):
    soup = scrape.get_soup(url)

    result = {"oztix_ticket_url":url}

    # get event image 
    event_header = soup.find('div',class_='event-header')
    result['event_image_url'] = event_header.find('img').get('src')
    result['event_name'] = event_header.find('img').get('alt')

    result['event_presenter'] = soup.find('div',class_ = 'presented-by').text

    # get event deatils
    event_details = soup.find('div','event-details')
    try:
        result['event_location'] = event_details.findAll('div')[0].find('span').text
        result['event_datetime'] = event_details.findAll('div')[1].find('span').text
    except:
        print("Failed to get event details")

    # get ticket information 
    event_ticket = soup.find('div',class_='tickets')
    # for now just the first ticket 
    try:
        ticket_name = event_ticket.find('div',class_='ticket-name').text
        ticket_price = event_ticket.find('div',class_='ticket-price').text
        ticket_description = event_ticket.find('div',class_='ticket-description').text
        ticket_tag = event_ticket.find('div',class_='ticket-tag').get('text') if event_ticket.find('div',class_='ticket-tag') != None else ''
        result['event_tickets'] = [{'ticket_name':ticket_name,'ticket_price':ticket_price,'ticket_description':ticket_description,'ticket_tag':ticket_tag}]
    except:
        print("Failed to get ticket details")

    return(result)

def extract_event_ticket_details(url):
    soup = scrape.get_soup(url)

    result = {"oztix_ticket_url":url}

    # get event image 
    content_section = soup.find('div',id='content')
    venue_info = content_section.find('div',class_='venueInfo')
    #result['event_image_url'] = event_header.find('img').get('src')
    result['event_name'] = venue_info.find('h2').text

    result['event_presenter'] = venue_info.find('h3').text

    # get event deatils
    location_p = venue_info.find('a').findPrevious('p')
    try:
        result['event_venue_name'] = location_p.find('b').find(text=True).replace('\n','').replace('\r','')
        result['event_location'] = (str(location_p).split('<br/>')[1].replace('\n','').replace('\r','')).strip()
        result['event_datetime'] = location_p.findPrevious('p').text
    except:
        print("Failed to get event details")

    # get ticket information 
    event_ticket = soup.find('h3',text='Reserve Tickets').findParent('div')
    # for now just the first ticket 
    try:
        ticket_name = event_ticket.findAll('td')[0].text
        ticket_price = event_ticket.findAll('td')[1].text
        result['event_tickets'] = [{'ticket_name':ticket_name,'ticket_price':ticket_price}]
    except:
        print("Failed to get ticket details")

    return(result)

def extract_ticket_details(url):
    if '/outlet/' in url:
        return(extract_outlet_ticket_details(url))
    else:
        return(extract_event_ticket_details(url))
    return({})

url = 'https://tickets.oztix.com.au/?Event=113808&utm_source=Oztix&utm_medium=Website&utm_content=EventGuide'


def get_oztix_eventguide_url(page=1):
    return("https://www.oztix.com.au/eventguide/?{}".format(urllib.parse.urlencode({**{"page":page},**refinement_parameters})))


def get_eventguide_article_details(event):
    return({ 
        "event_url":event.find('a').get('href'),
        "event_name":event.find('span',class_='product-name').text,
        "event_city":event.find('span',class_='product-city').text,
        "event_venue":event.find('span',class_='product-type').text,
        "event_location":', '.join([event.find('span',class_='product-type').text,event.find('span',class_='product-city').text]),
        "event_image":event.find('img').get('src'),
        "event_day_of_month":event.find('div',class_='product-date-container').find('span', class_='date').text,
        "event_month":event.find('div',class_='product-date-container').find('span', class_='month').text
    })

def extract_eventguide_page(page = 1):
    url = get_oztix_eventguide_url(page)
    soup = scrape.get_soup(url,use_selenium=True)
    events = [get_eventguide_article_details(event) for event in soup.findAll('article',class_='hit')]
    event_urls = [e.get('event_url') for e in events] 
    return({
        "page":1,
        "events":events,
        "event_urls":event_urls
    })

def extract_eventguide(start_page = 1,end_page=20):
    results = []
    for i in range(start_page,end_page + 1):
        results.append(extract_eventguide_page(i))
    return(results)
