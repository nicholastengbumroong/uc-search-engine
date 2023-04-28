"""
Your application should read a file of seed  URLs.
The application should also input the number of pages to crawl and the number of levels (hops (i.e. hyperlinks) away from the seed URLs).
Optionally, you can filter the domains that your crawler should access, e.g. .edu or .gov only.
All crawled pages (html files) should be stored in a folder.
Python-based Scrapy will be the default crawler used in the discussion sessions. 
You can use other languages or libraries, like Java jsoup, but support for these will be limited.
"""
from bs4 import BeautifulSoup
import requests
import json
# import sys
# import string


urls = []
queue = []
visited_urls = []

#pages_crawled = 0

with open('seeds.txt', 'r') as seeds:
    for seed in seeds:
        urls.append(str(seed.strip()))
        
def data_dump(data):
    with open('data.json', 'a') as outfile:
        json.dump(data, outfile)
        outfile.write('\n')
        
        
def crawler(url):
    #global pages_crawled
    
    if(url in visited_urls): #we have visited this url 
        return
    
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, 'lxml') 
    body = soup.body.text
    
    data = {"url": url, "body": body} # add to data
    data_dump(data) # dump the data
    
    visited_urls.append(url) 
    
    links = soup.find_all('a')
    for link in links:
        href = link.get('href')
        # check to see if url is part of same domain
        if not href.startswith('/') or '://' in href:
            continue
        full_url = url + href
        queue.append(full_url)
            
# crawl starting with given seed pages #
for url in urls:
     crawler(url)
# crawl the rest of the queue #
while queue:
    url = queue.pop(0)
    crawler(url)

    """
    - Still need to make it stop crawling after a certain period of time/or data limit
    - Need to account for number of pages to crawl and number of hops
    - There might be some other smaller stuff missed or that can be improved 
    This is a very rough draft of our crawler
    """