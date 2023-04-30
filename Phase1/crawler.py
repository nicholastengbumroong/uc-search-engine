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
import os.path
import collections

seed_urls = []
queue = collections.deque()
visited_urls = {}
outfile = open('data.json', 'a')
maxFileSize = 100
# pages_crawled = 0

with open('seeds.txt', 'r') as seeds:
    for seed in seeds:
        seed_urls.append(str(seed.strip()))
        
def crawl(url):
    #global pages_crawled
    
    if(url in visited_urls):
        return
    
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, 'lxml') 
    body = soup.body.text
    
    data = {"url": url, "body": body}
    json.dump(data, outfile)
    outfile.write('\n')

    visited_urls[url] = 0
    
    links = soup.find_all('a')
    for link in links:
        href = link.get('href')
        # check to see if url is part of same domain
        if href is None or not href.startswith('/') and '://' in href:
            continue
        full_url = url + href
        queue.append(full_url)

def crawler():
    outfile = open('data.json', 'a')
    # crawl starting with given seed pages
    for url in seed_urls:
        crawl(url)
    # crawl the rest of the queue 
    outFileSize_MB = os.fstat(outfile.fileno()).st_size / 1048576
    countinueCrawl = len(queue) > 0 and outFileSize_MB <= 1
    while (countinueCrawl):
        print(outFileSize_MB)
        url = queue.pop()
        crawl(url)

    """
    - Still need to make it stop crawling after a certain period of time/or data limit
    - Need to account for number of pages to crawl and number of hops
    - nneds to handle duplicate pages
    - There might be some other smaller stuff missed or that can be improved 
    This is a very rough draft of our crawler
    """
    outfile.close()

crawler()
print(os.path.getsize('data.json'))