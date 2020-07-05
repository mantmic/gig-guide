import lib.google   as google 
import lib.util     as util

from prefect import task

@task
def extract_google_search(input_data,query_field):
    results = []
    # get urls from input data
    queries = [i.get(query_field) for i in input_data]
    # flatten list
    queries = util.flatten(queries)
    # get unique artists
    queries = list(set(queries))
    # iterate
    for query in queries:
        try:
            results.append(google.extract_google_search(query))
        except Exception as e:
            print("Failed")
            print(e)

    return(results)
