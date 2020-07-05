import lib.spotify as spotify
import lib.util    as util

from prefect import task


@task
def extract_artist_search(input_data,artist_name_field):
    results = []
    # get from input data
    artist_names = util.get_lookup(input_data,artist_name_field)
    # iterate
    for artist_name in artist_names:
        try:
            artist_search = spotify.get_artist_search(artist_name)
            # append the artist ids for easier lookup
            artist_search['result_artist_ids'] = [r.get('id') for r in artist_search['search_results']['artists']['items']]
            results.append(artist_search)
        except:
            print("Failed")
    return(results)

@task
def extract_artist_top_tracks(input_data,artist_id_field):
    results = []
    # get from input data
    artist_ids = util.get_lookup(input_data,artist_id_field)
    # iterate
    for artist_id in artist_ids:
        try:
            top_tracks = spotify.get_artist_top_tracks(artist_id)
            results.append(top_tracks)
        except:
            print("Failed")
    return(results)

