import lib.scrape as scrape 

query = 'jaala'

url = 'https://www.google.com/search?q={}'.format(query)

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0"
headers = {"user-agent" : USER_AGENT}

soup = scrape.get_soup(url,headers)

# get bio, bio link


# get music platforms
music_platform_table = soup.find('div',text='Available on').findNext('div').findAll('td')
music_platform_links = []
for td in music_platform_table:
    music_platform_links.append({
        'url':td.find('a').get('href'),
        'description':td.find('span',class_='hl').text
    })

# get events 

# get social media links 
social_media_objects = soup.find('div',text='Profiles').findNext('div').findChildren('div',recursive=False)
social_media_links = []
for smo in social_media_objects:
    social_media_links.append({
        'url':smo.find('a').get('href'),
        'description':smo.text
    })

# get videos 