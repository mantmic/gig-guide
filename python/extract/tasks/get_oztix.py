import lib.oztix as oztix

url  = "https://tickets.oztix.com.au/outlet/event/8f799cd6-996d-4d4c-8b72-4747f8e0d622?utm_source=Whatslively&utm_medium=Website"

print(oztix.extract_ticket_details(url))

url  = "https://tickets.oztix.com.au/outlet/event/ba7a01bb-eb94-456b-8673-519189a3d125?utm_source=Whatslively&utm_medium=Website"

print(oztix.extract_ticket_details(url))

url = 'https://tickets.oztix.com.au/outlet/event/801825b4-491b-42f1-be20-08d9d6e85b25?utm_source=Whatslively&utm_medium=Website'

print(oztix.extract_ticket_details(url))
