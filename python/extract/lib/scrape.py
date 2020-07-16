import requests
from bs4 import BeautifulSoup
import time
import os



from selenium import webdriver 
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC 
from selenium.common.exceptions import TimeoutException

retry_max_count = int(os.getenv('REQUEST_RETRY_COUNT', 3))
retry_sleep_time = int(os.getenv('REQUEST_FAILURE_SLEEP_TIME', 35))
chromedriver_path = os.getenv('CHROMEDRIVER_PATH')

# setup selenium
selenium_option = webdriver.ChromeOptions()
selenium_option.add_argument(" — incognito")
selenium_option.add_argument('--headless')

class InteractiveScrapeSession:
    '''
        Works on MacOS
    '''
    def __init__(self, url):
        options = webdriver.ChromeOptions()
        #options.add_argument("user-data-dir=/Users/{}/Library/Application\ Support/Google/Chrome/Default".format(os.getenv('USER')));
        self._selenium_browser = webdriver.Chrome(executable_path=chromedriver_path, options=options)
        self._selenium_browser.get(url)

    def get_soup(self):
        html_source = self._selenium_browser.find_element_by_tag_name('html').get_attribute('innerHTML')
        res = BeautifulSoup(html_source,'html.parser')
        return(res)

def get_soup(url, headers = {}, use_selenium=False):
    retry_count = 0
    print("Scraping url %s" % url)
    success = False
    res = None
    while success == False and retry_count < retry_max_count:
        try:
            if(use_selenium):
                selenium_browser = webdriver.Chrome(executable_path=chromedriver_path, options=selenium_option)
                selenium_browser.get(url)
                html_source = selenium_browser.find_element_by_tag_name('html').get_attribute('innerHTML')
                res = BeautifulSoup(html_source,'html.parser')
            else:
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
