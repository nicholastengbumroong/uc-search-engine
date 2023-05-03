"""
Your application should read a file of seed  URLs.
The application should also input the number of pages to crawl and the number of levels (hops (i.e. hyperlinks) away from the seed URLs).
Optionally, you can filter the domains that your crawler should access, e.g. .edu or .gov only.
All crawled pages (html files) should be stored in a folder.
Python-based Scrapy will be the default crawler used in the discussion sessions. 
You can use other languages or libraries, like Java jsoup, but support for these will be limited.
"""
from multiprocessing import Pool, Manager, Array, Queue, Value, Lock
from bs4 import BeautifulSoup
from ctypes import c_int
import requests
import json
import sys
import math
import tldextract 
import os

seed_urls = []
MAX_FILE_SIZE = 20       # in MB 
workers = 4
maxHops = 6
# maxPages = math.ceil(100/workers)
# totalPagesCrawled = Value(c_int)
MB = 0

with open('seeds.txt', 'r') as seeds:
    for seed in seeds:
        seed_urls.append(str(seed.strip()))
    
def sameDomainOrEdu(url, seed_url):
    domain = tldextract.extract(url).domain
    suffix = tldextract.extract(url).suffix
    seed_domain = tldextract.extract(seed_url).domain
    seed_suffix = tldextract.extract(seed_url).suffix
    if suffix == "edu" or (domain == seed_domain and suffix == seed_suffix and not url.endswith(".html")):
        return True
    return False
     
def crawl(url, queuePool: Array, visited_urls, outfile) -> None:
    global totalPagesCrawled
    if(url in visited_urls):
        return

    #print('fetching url', url)
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, 'lxml') 
    body = soup.body.text

    data = {"url": url, "body": body}
    json.dump(data, outfile)
    # with totalPagesCrawled.get_lock():
    #     totalPagesCrawled.value += 1
    #print('crawled : ', totalPagesCrawled.value)
    outfile.write('\n')
    visited_urls[url] = 0
    
    links = soup.find_all('a')
    for link in links:
        href = link.get('href')
        # check to see if url is part of same domain or an edu page 
        if href is None:
            continue
        if href.startswith('http'): # check if href is its own link
            if sameDomainOrEdu(href, url):  # check to see if link is an edu page 
                full_url = href
                #print(full_url)
            else:
                continue
        else:                       # else it is part of same domain
            full_url = url + href
            # print(full_url)
            if not sameDomainOrEdu(full_url, url):
                continue 
        assignedQueueIndex = 0
        for char in full_url:
            assignedQueueIndex += ord(char)
        assignedQueueIndex = assignedQueueIndex % workers
        queuePool[assignedQueueIndex].put(full_url) 


def crawler(id: int, queuePool: Array) -> None: 
    crawler_limit = MAX_FILE_SIZE / workers
    curr_file_size = 0 
    visited_urls = {}
    assignedQueue: Queue  = queuePool[id]
    filename = 'data/data_' + str(id) + '.json' 
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    outfile = open(filename, 'w')
    # crawl starting with given seed pages
    for url in seed_urls:
        crawl(url, queuePool, visited_urls, outfile)
     
    while (curr_file_size < crawler_limit):
        curr_file_size += os.path.getsize(filename) / (1024*1024.0)
        print("Crawler", id, ": ", '%0.2f' % curr_file_size, ' MB')

        url = assignedQueue.get()
        #print('inner fetched url', url)
        crawl(url, queuePool, visited_urls, outfile)
    outfile.close()

if __name__ == '__main__':
    with Pool(processes=workers) as pool:
        with Manager() as manager:
            queuePoolArgs = []
            queuePool = [manager.Queue() for i in range(workers)]
            poolArguments = [(i, queuePool) for i in range(workers)]
            pool.starmap(crawler, poolArguments)

"""
- Still need to make it stop crawling after a certain period of time/or data limit
- Need to account for number of pages to crawl and number of hops
- needs to handle duplicate pages
- There might be some other smaller stuff missed or that can be improved 
This is a very rough draft of our crawler
"""