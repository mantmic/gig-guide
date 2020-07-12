import lib.scrape as scrape 


'''
# stopgap method for attaining venues manually 
# run the following and manually navigate to the Gig Finder venue page 
sesh = scrape.InteractiveScrapeSession('https://www.reverbnation.com/control_room/artist/6968045/gig_finder_recommended')
venue_urls = []

# when the page loads and you see a list of venues, run the following to extract the venues seen on the page and append to venue_urls 
# keep navigating through each page running the below code to get all venues 

soup = sesh.get_soup()
for span in soup.findAll('span',itemprop='location'):
    venue_urls.append(span.find('a').get('href').replace('?',''))

# Get the end of each url (unique)
venue_ids = list(set([v.split('/')[-1] for v in venue_urls]))
venue_ids = [v for v in venue_ids if not(v.isnumeric())]
venue_urls = ['/'.join(['https://www.reverbnation.com/venue',v]) for v in venue_ids]
'''

def get_venue_urls():
    ## TO DO, find a way to automatically extract this 
    sydney_venue = ['https://www.reverbnation.com/venue/thecambridgehotelnewcastle', 'https://www.reverbnation.com/venue/barpetitenewcastle', 'https://www.reverbnation.com/venue/thebridgehotel', 'https://www.reverbnation.com/venue/themerton', 'https://www.reverbnation.com/venue/easternlounge', 'https://www.reverbnation.com/venue/riverstoneschofieldsmemorialclub', 'https://www.reverbnation.com/venue/hardrockcafesydney', 'https://www.reverbnation.com/venue/frankiespizza', 'https://www.reverbnation.com/venue/empirebaytavern', 'https://www.reverbnation.com/venue/cameliagrovehotel', 'https://www.reverbnation.com/venue/stocktonbowlingclub', 'https://www.reverbnation.com/venue/lazyboneslounge', 'https://www.reverbnation.com/venue/cardiffrslclub', 'https://www.reverbnation.com/venue/thestagandhunterhotel', 'https://www.reverbnation.com/venue/thegreatnorthernhotelnewcastle', 'https://www.reverbnation.com/venue/georgeivinn', 'https://www.reverbnation.com/venue/theconcourse', 'https://www.reverbnation.com/venue/petershambowlingclub']
    return(sydney_venue)

def extract_venue(url):
    '''
        Function to extract venue information and venue shows from reverbnation 
    '''
    soup = scrape.get_soup(url,use_selenium=True)
    res = {'venue_reverbnation_url':url}
    # get venue information 
    venue_name = soup.find('h1',class_="profile_user_name").text.replace('\n','').strip()
    venue_location = soup.find('span',class_='profile_location').text
    venue_location_full = ' '.join([venue_name,venue_location])
    venue_address = ' '.join(s.text for s in soup.find('p',itemtype='http://schema.org/PostalAddress').findAll('span'))
    venue_image_url = soup.find('div',class_='profile_left_column').find('img').get('src')
    
    res['venue_name'] = venue_name
    res['venue_location'] = venue_location
    res['venue_location_full'] = venue_location_full
    res['venue_address'] = venue_address 
    res['venue_image_url'] = venue_image_url
    
    venue_social_links = []
    # get venue social links 
    try:
        for li in soup.find('ul',id='profile_website_items').findAll('li'):
            venue_social_links.append({
                'social_link_type':li.find('a').get('title').lower(),
                'social_link_url':li.find('a').get('href')
            })
        res['venue_social_links'] = venue_social_links
    except:
        print("Failed to get social links")
    # get show information
    shows_container = soup.find('ul',id='shows_container')

    shows = []
    venue_artists = []
    show_order = 1
    try:

        for show in shows_container.findAll('li',recursive=False):
            show_reverbnation_url = show.find('meta',itemprop='url').get('content')
            show_name = show.find('meta',itemprop='description').get('content')
            show_datetime = show.find('meta',itemprop='startDate').get('content')
            # get ticket link 
            show_ticket_url = show.find('a',text='Tickets').get('href')
            # get show artists
            show_artists = []
            for li in show.find('ul').findAll('li'):
                # sometimes the artist has the musicgroup schema, othertimes it's more simple 
                if(li.find('span',itemtype="http://schema.org/MusicGroup")):
                    # the artist has a reverbnation account
                    show_artists.append({
                        'artist_name':li.find('span',itemtype="http://schema.org/MusicGroup").find('a').text,
                        'artist_image_url':li.find('img').get('src'),
                        'artist_reverbnation_url':''.join(['https://reverbnation.com',li.find('span',itemtype="http://schema.org/MusicGroup").find('a').get('href')]).replace('?','')
                    })
                    venue_artists.append(li.find('span',itemtype="http://schema.org/MusicGroup").find('a').text)
                else:
                    # the artist does not have a reverbnation account
                    show_artists.append({
                        'artist_name':li.find('span',class_='fb_artist_name').text,
                        'artist_image_url':li.find('img').get('src')
                    })
                    venue_artists.append(li.find('span',class_='fb_artist_name').text)
                # end artists
            # append the show
            shows.append({
                'show_reverbnation_url':show_reverbnation_url,
                'show_name':show_name,
                'show_datetime':show_datetime,
                'show_ticket_url':show_ticket_url,
                'show_artists':show_artists,
                'show_order':show_order
            })
            show_order += 1
        res['shows'] = shows
        res['venue_artists'] = venue_artists
    except:
        print("failed to get shows")
    
    return(res)
