import lib.scrape as scrape 


def extract_ticket_details(url):
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