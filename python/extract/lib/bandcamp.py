import requests
from urllib import request, parse
from bs4 import Comment
import re
import datetime
import lib.scrape as scrape


def get_bandcamp_search(artist_name):
    '''
    Function to search bandcamp for artists that match
    '''
    extract_ts = datetime.datetime.now().isoformat()
    results = []

    search_url = "".join(["https://bandcamp.com/search?q=",parse.quote(artist_name)])

    soup = scrape.get_soup(search_url)

    if(soup == None):
        return(results)

    band_search_results = soup.findAll('li', class_='searchresult band')

    search_rank = 1

    for band_result in band_search_results:
        results.append({
            "input_artist_name":artist_name,
            "bandcamp_url":band_result.find('div', class_='itemurl').text.replace('\n',''),
            "bandcamp_artist_name":band_result.find('div', class_='heading').text,
            "search_rank":search_rank,
            "extract_ts":extract_ts
        })
        search_rank += 1
    return(results)



def get_bandcamp_albums(url):
    extract_ts = datetime.datetime.now().isoformat()
    results = []

    parsed_url = parse.urlparse(url)

    soup = scrape.get_soup(url)

    if(soup == None):
        return(results)

    # initialise album order
    album_order = 1

    # find the album links
    album_link_objects = soup.findAll('li', class_="music-grid-item square first-four")

    # TODO handle label pages that have links to artists
    if(len(album_link_objects) > 0):
        for album_link in album_link_objects:
            # determine whether album_url is relative path or absolute
            bandcamp_album_url = album_link.find('a').get('href')
            if("bandcamp.com" not in bandcamp_album_url):
                # if bandcamp.com isn't in url then it's a relative path
                bandcamp_album_url = "".join(["https://",parsed_url.netloc,bandcamp_album_url])
            results.append({
                "bandcamp_url":url,
                "bandcamp_album_url":bandcamp_album_url,
                "album_name":album_link.find('p',class_='title').text,
                "album_order":album_order,
                "extract_ts":extract_ts
            })
            album_order += 1
    else:
        # if there are no album links this page is an album, link page to the page as the album link
        results.append({
            "bandcamp_url":url,
            "bandcamp_album_url":url,
            "album_name":soup.find('h2',class_='trackTitle').text,
            "album_order":album_order,
            "extract_ts":extract_ts
        })
    return(results)


def get_bandcamp_album_details(url):
    '''
    Function to get details for an artist album/track on bandcamp

    Args:
        url (str): A url to the artists album/track on bandcamp
    '''
    extract_ts = datetime.datetime.now().isoformat()

    parsed_url = parse.urlparse(url)

    soup = scrape.get_soup(url)

    if(soup == None):
        return({})

    # get album / track id
    page_comments = soup.findAll(text=lambda text:isinstance(text, Comment))

    album_type = None
    album_id = None

    # get the album id and album type
    for c in page_comments:
        this_comment = c.extract()
        if "album id" in this_comment:
            album_type='album'
            album_id=re.sub('[^0-9]','', this_comment)
            break
        elif "track id" in this_comment:
            album_type='track'
            album_id=re.sub('[^0-9]','', this_comment)

    # use album id and track id to generate an embeddedplayer link
    embedded_player_link = "https://bandcamp.com/EmbeddedPlayer/{}={}/size=small/bgcol=ffffff/linkcol=0687f5/transparent=true/".format(album_type,album_id)
    response = {
        "bandcamp_arist_name":soup.find('span',itemprop='byArtist').text,
        "bandcamp_album_url":url,
        "bandcamp_album_type":album_type,
        "bandcamp_album_id":album_id,
        "bandcamp_embedded_player_link":embedded_player_link,
        "bandcamp_artist_location":soup.find('span',class_="location secondaryText").text,
        "extract_ts":extract_ts
    }
    # get non mandatory entries
    # bio
    try:
        response['bio'] = soup.find('p',id='bio-text').text
    except:
        pass
    # band links
    try:
        response['band_links'] = [{'link_text':a.text,'link_url':a.get('href')} for a in soup.find('ol',id='band-links').findAll('a')]
    except:
        pass
    # band shows
    try:
        response['band_showography'] = [{
            'show_date':li.find('div',class_='showDate').text,
            'show_url':li.find('a').get('href'),
            'show_venue':li.find('div',class_='showVenue').text,
            'show_location':li.find('div',class_='showLoc').text,
            'show_location_full':', '.join([li.find('div',class_='showVenue').text,li.find('div',class_='showLoc').text])
        } for li in soup.find('div',id='showography').findAll('li')]
        # create seperate object for passing into geocoding functions
        response['show_locations'] = [l.get('show_location_full') for l in response['band_showography']]
    except:
        pass
    return(response)
