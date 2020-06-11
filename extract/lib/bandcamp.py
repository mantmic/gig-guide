from urllib import request, parse
from bs4 import BeautifulSoup, Comment
import re
from fuzzywuzzy import process, fuzz

#perform a query for the artist name on bandcamp
def BandcampGetArtistLink(artistName):
    min_match = 90
    bandcamp_url = "https://bandcamp.com"
    page = request.urlopen(bandcamp_url + "/search?q=" + parse.quote(artistName))
    soup = BeautifulSoup(page, "html5lib")
    #get the search response
    results = soup.find_all('div', {"class":"result-info"})
    #filter so we only get arist responses
    results = [x for x in results if "artist" in x.find("div",{"class":"itemtype"}).text.lower()]
    results = [x for x in results if "artist" in x.find("div",{"class":"itemtype"}).text.lower()]

    artist_results = []

    for i in range(len(results)):
        	this_name = results[i].find('div', {"class":"heading"}).text.strip()
        	this_link = results[i].find('div', {"class":"itemurl"}).text.strip()
        	artist_results.append({"name":this_name, "link":this_link})
    #get best match
    best_match = process.extractOne(artistName,map(lambda x: x.get("name"),artist_results), scorer=fuzz.token_sort_ratio)
    #check if it's close enough
    if best_match is None:
        return
    if best_match[1] > min_match:
        artist_match = [x for x in artist_results if best_match[0] == x.get("name")][0]
        return(artist_match.get("link"))
    else:
        return

def BandcampGetArtistAlbumLinks(artistLink):
    page = request.urlopen(artistLink)
    soup = BeautifulSoup(page, "html5lib")
    albums = soup.find_all('li', {"class":"music-grid-item square first-four "})
    return(list(map(lambda x: x.find("a").get("href"),albums)))

def BandcampGetTrackLink(albumPage):
	this_page = request.urlopen(albumPage)
	s = BeautifulSoup(this_page, "html5lib")
	page_comments = s.findAll(text=lambda text:isinstance(text, Comment))
	for comments in page_comments:
		this_comment = comments.extract()
		if "album id" in this_comment:
			return({"link":albumPage,"id":re.sub('[^0-9]','', this_comment),"type":"album"})
		elif "track id" in this_comment:
			return({"link":albumPage,"id":re.sub('[^0-9]','', this_comment),"type":"track"})

def BandcampBandSearch(artistName):
    artist_url = BandcampGetArtistLink(artistName)
    #get some links to the music
    trackLinks = []
    if artist_url != None:
        #check if there is a music link on the home page
        #otherwise find a link to album or ep
        pageTrack = BandcampGetTrackLink(artist_url)
        if pageTrack != None:
            trackLinks.append(pageTrack)
        else:
            #get the albums
            albumLinks = BandcampGetArtistAlbumLinks(artist_url)
            #loop through all the albums
            for i in range(len(albumLinks)):
                	album_link = artist_url + albumLinks[i]
                	trackLinks.append(BandcampGetTrackLink(album_link))
    return({
        "bandcampLink":artist_url,
        "bandcampTracks":trackLinks
    })
