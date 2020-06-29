import requests
from bs4 import BeautifulSoup
import time
import os

retry_max_count = int(os.getenv('REQUEST_RETRY_COUNT', 3))
retry_sleep_time = int(os.getenv('REQUEST_FAILURE_SLEEP_TIME', 35))


def get_soup(url, headers = {}):
    retry_count = 0
    print("Scraping url %s" % url)
    success = False
    res = None
    while success == False and retry_count < retry_max_count:
        try:
            res = requests.get(url,headers=headers)
            res = BeautifulSoup(res.text,'html.parser')
            success = True
        except:
            if (retry_count < retry_max_count):
                retry_count += 1
                print("{} of {} failed to extract url".format(retry_count,retry_max_count))
                print("Sleeping %s seconds" % retry_sleep_time)
                time.sleep(retry_sleep_time)
            else:
                print("Failed to scrape url %s" % url)
                return
    return(res)
