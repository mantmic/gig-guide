import geocoder
import datetime

def get_geocode(input_address):
    g = geocoder.google(input_address)
    print(g.json)
    print(g)
    return({
        'input_address':input_address,
        'geocode_provider':g.provider,
        'extract_ts':datetime.datetime.now().isoformat(),
        #'geojson_result':g.geojson,
        'json_result':g.json
    })
