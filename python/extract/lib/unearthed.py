# search for artists on JJJ unearthed
import requests
import urllib

import lib.scrape as scrape


UNEARTHED_BASE_URL = 'https://www.triplejunearthed.com'



def search_unearthed(artist_name):
    '''
    Function to get JJJ unearthed search results for a given artist name

    Args:
        artist_name (str): Artist name to search JJJ

    '''
    search_url = '/'.join([UNEARTHED_BASE_URL,'search/site',urllib.parse.quote(artist_name)])

    soup = scrape.get_soup(search_url)

    search_results = soup.findAll('li', class_='search-result')

    search_result_order = 1

    #initialise results
    results = []

    for search_result in search_results:
        # get the artist link from the image url
        unearthed_artist_url = search_result.find('a').get('href')
        # if the full url isn't in the href, add it in
        if UNEARTHED_BASE_URL not in unearthed_artist_url:
            unearthed_artist_url = ''.join([UNEARTHED_BASE_URL,unearthed_artist_url])
        # determine result type
        search_result_type = search_result.find('p').text

        # get genres
        genres = [g.text for g in search_result.find('span', class_='genre').findAll('a')]
        result_object = {
            'search_artist_name':artist_name,
            'unearthed_artist_url':unearthed_artist_url,
            'search_result_order':search_result_order,
            'search_result_type':search_result_type,
            'genres':genres
        }
        #increment result order
        search_result_order += 1

        ###  optional fields
        # get tags
        tag_div = search_result.find('div',class_="field field-name-field-unearthed-tags-union field-type-taxonomy-term-reference field-label-inline clearfix")
        if(tag_div != None):
            result_object['tags'] = [t.text for t in tag_div.findAll('a')]

        # if it's a track, get the track id, generate a track play link
        if(search_result_type == 'Track'):
            play_controls = search_result.find('div',class_='play_controls')
            try:
                result_object['track_id'] = play_controls.find('a').get('href').split('/')[-1]
            except:
                pass

        results.append(result_object)
    return(results)


def get_artist_details(url):
    '''
    Function to get information from an artist page on JJJ unearthed
    '''
    soup = scrape.get_soup(url)
    artist_details = {'unearthed_artist_url':url}
    artist_details['artist_name'] = soup.find('h1',id='unearthed-profile-title').text

    # get location
    try:
        artist_details['location'] = soup.findAll('span',class_='location')[-1].text
    except:
        pass

    # get genres
    artist_details['genres'] = [g.text for g in soup.findAll('span',class_='genre')]

    # get tracks
    try:
        artist_details['track_ids'] = [t.get('href').split('/')[-1] for t in soup.findAll('a',class_='play_now_large open_jukebox')]
    except:
        pass
    # get tags
    try:
        artist_details['tags'] = [t.text for t in soup.find('div',class_='panel tags').findAll('a')]
    except:
        pass
    # get band members if exists
    try:
        artist_details['band_members'] = soup.find('h3',text='band members').findNext('p').text.split('\r\n')
    except:
        pass
    # get website
    try:
        artist_details['website'] = soup.find('h3',text='Website').findNext('p').find('a').get('href')
    except:
        pass

    # get social links
    socials = []
    try:
        for social_link in soup.find('ul',class_='social').findAll('li'):
            socials.append({
                'link_type':social_link.get('class')[0],
                'link_url':social_link.find('a').get('href')
            })
        artist_details['socials'] = socials
    except:
        pass
    # get artist bio
    try:
        artist_details['artist_bio'] = soup.find('h3',text='Bio').findNext('p').text
    except:
        pass
    # get youtube links
    artist_details['youtube_links'] = [l.get('src') for l in soup.findAll('iframe',id='ytplayer')]
    return(artist_details)
