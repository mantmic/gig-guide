import lib.scrape as scrape 
from urllib import parse


def extract_google_search(query):
    # initialise response
    result = {'input_query':query}

    url = 'https://www.google.com/search?q={}'.format(parse.quote(query))

    USER_AGENT = "Mozilla/5.0 (X11; Linux i686; rv:77.0) Gecko/20100101 Firefox/77.0"
    headers = {"user-agent" : USER_AGENT}

    soup = scrape.get_soup(url,headers,use_selenium=True)

    sidebar_section = soup.find('h1',text='Complementary results').findNext('div')
    # get artist name     
    for span in sidebar_section.findAll('span'):
        if span.text != '':
            result['artist_name'] = span.text
            break

    # get bio, bio link
    try:
        bio_div = sidebar_section.find('h2',text='Description').findNext('div')
        result['bio_text'] = bio_div.find('span').text
        result['bio_link_url'] = bio_div.find('a').get('href')
    except:
        print("bio failed")

    # get other info like genres and stuff 
    other_info = []

    try:
        other_info_section = sidebar_section.find('h2',text='Description').findNext('div')
        #get sections of key values 
        other_info_divs = other_info_section.findAll('div',class_='mod')

        for div in other_info_divs:
            links = div.findAll('a')
            other_info_key = links[0].text.lower()
            other_info_key = other_info_key.replace(' ','_')
            other_info_values = [l.text for l in links[1:]]
            other_info.append({
                'description':other_info_key,
                'value':other_info_values
            })
    except:
        print("other info failed")
    result['other_info'] = other_info

    # get music platforms
    music_platform_links = []
    try:
        music_platform_table = soup.find('div',text='Available on').findNext('div').findAll('td')
        for td in music_platform_table:
            music_platform_links.append({
                'url':td.find('a').get('href'),
                'description':td.find('span',class_='hl').text
            })
    except:
        print("music platform faild")
    
    result['music_platform_links'] = music_platform_links

    # get events 
    events = []

    try:
        events_div = sidebar_section.find('a',text='Events').findNext('div',role='list')

        for div in events_div.findAll('div',recursive=False):
            events.append({
                'event_venue_name':div.find('span').text,
                'event_location':div.find('div',class_='title').text,
                'event_venue_location':' '.join([div.find('span').text,div.find('div',class_='title').text]),
                'event_description_full':div.find('a').get_text(' ')
            })
    except:
        print("events failed")

    result['events'] = events
    # also put the event locations on the result object for easier geocoding 
    result['event_venue_locations'] = [e.get('event_venue_location') for e in events]
    # get social media links 
    social_media_links = []
    try:
        social_media_objects = soup.find('div',text='Profiles').findNext('div').findChildren('div',recursive=False)
        for smo in social_media_objects:
            social_media_links.append({
                'url':smo.find('a').get('href'),
                'description':smo.text
            })
    except:
        print("social media failed")

    result['social_media_links'] = social_media_links

    # get videos 
    videos = []
    video_order = 1
    try:
        video_cards = soup.findAll('g-inner-card')

        for card in video_cards:
            videos.append({
                'url':card.find('a').get('href'),
                'video_order':video_order,
            })
            video_order += 1
    except:
        print("videos failed")
    result['videos'] = videos 

    # get search results 
    search_results = []
    search_result_order = 1
    try:
        search_result_divs = soup.findAll('div',class_='r')
        for div in search_result_divs:
            search_results.append({
                'url':div.find('a').get('href'),
                'search_result_order':search_result_order
            })
            search_result_order += 1
    except:
        print("search results failed")

    result['search_results'] = search_results
    return(result)
