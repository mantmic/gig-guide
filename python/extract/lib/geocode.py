import geocoder
import datetime

def get_geocode(input_address):
    g = geocoder.google(input_address)
    extract_keys = ['address','city','country','lat','lng','bbox']
    json_result = {key:g.json.get(key) for key in extract_keys}
    return({
        'input_address':input_address,
        'geocode_provider':g.provider,
        'extract_ts':datetime.datetime.now().isoformat(),
        #'geojson_result':g.geojson,
        'json_result':json_result
    })
