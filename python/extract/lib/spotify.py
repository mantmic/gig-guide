import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())


def get_artist_search(artist_name):
    query=artist_name
    results = spotify.search(query, limit=10, offset=0, type='artist', market=None)
    return({
        'input_search_artist':artist_name,
        'search_results':results
    })

def get_artist_top_tracks(artist_id):
    results = spotify.artist_top_tracks(artist_id, country='AU')
    return({
        'input_artist_id':artist_id,
        'top_tracks':results
    })