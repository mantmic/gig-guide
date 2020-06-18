# search for artists on JJJ unearthed
import requests
import urllib3

import lib.scrape as scrape

UNEARTHED_BASE_URL = 'https://www.triplejunearthed.com/'

artist_name = 'Caitlin Harnett & The Pony Boys'


search_url = '/'.join([UNEARTHED_BASE_URL,'search/site',urllib.parse.quote(artist_name)])

soup = scrape.get_soup(search_url)

search_results = soup.findAll('li', class_='search-result')

search_result_order = 1

search_result = search_results[0]

# get the artist link from the image url
unearthed_artist_url = search_result.find('a').get('href')
# determine result type
search_result_type = search_result.find('p').text

# get tags and genres

# if it's a track, get the track id, generate a track play link

search_result.find('a')
