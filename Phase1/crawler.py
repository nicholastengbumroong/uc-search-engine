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
import hashlib

# python crawler.py seeds.txt 500 100000 data 8

input_seed_file = sys.argv[1]
input_max_size = sys.argv[2]
input_max_hops = sys.argv[3]
input_output_dir = sys.argv[4]
input_num_workers = sys.argv[5]

NUM_WORKERS = int(input_num_workers)
MAX_HOPS = int(input_max_hops)
TARGET_SIZE = float(input_max_size)        # in MB
# MAX_FILE_SIZE = TARGET_SIZE / NUM_WORKERS  # in MB  

seed_urls = []


with open(input_seed_file, 'r') as seeds:
    for seed in seeds:
        seed_urls.append(str(seed.strip()))

def hasEndingHtmlTag(html_text):
    n = len(html_text)
    endingHtmlTag = '</html>'
    endingHtmlTagLength = len(endingHtmlTag)
    i = endingHtmlTagLength
    while(i <= n):
        matches = 0
        for j in range(endingHtmlTagLength):
            if(html_text[n - i + j] == endingHtmlTag[j]):
                matches += 1
        if (matches == endingHtmlTagLength):
            return True
        i += 1
    return False

def getUrlHtml(url):
    user_agent = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
    url_headers = requests.head(url, headers=user_agent).headers
    allowedContentTypes = ['text/plain', 'text/html']
    hasAllowedContentType = False
    for contentType in allowedContentTypes:
        if(contentType in url_headers['Content-Type']):
            hasAllowedContentType = True
    
    if(not hasAllowedContentType):
        # print(url, url_headers['Content-Type'], '❌ not in')
        return ""
    
    html_text = requests.get(url, headers=user_agent).text
    if(not hasEndingHtmlTag(html_text)):
        # print(url, url_headers['Content-Type'], '❌')
        return ""

    # print(url, url_headers['Content-Type'], '✅')
    return html_text

def crawl(url, queuePool: Array, visited_urls, outfile, numHops, shared_fingerprints) -> None:
    if(url in visited_urls):
        return
    try:
        html_text = getUrlHtml(url)
        if(html_text == ""):
            return
        
        soup = BeautifulSoup(html_text, 'lxml') 
        body = soup.body.text
        
        simHash = hashDoc(body, shared_fingerprints)
        if(simHash == -1):
            visited_urls[url] = 0
            return
        
        data = {"url": url, "body": body}
        json.dump(data, outfile)
        outfile.write(',\n')
        visited_urls[url] = 0
        
        full_url = None
        numHops += 1
        links = soup.find_all('a')
        
        for link in links:
            href = link.get('href')
            domain = tldextract.extract(url).domain
            
            full_url = ""
            
            if href is None:
                continue
            
            if href.startswith('http') or href.startswith('//'):
                if tldextract.extract(href).suffix == "edu":
                    full_url = href
                else:
                    continue
                
                if full_url.startswith('//'):
                    full_url = 'https:' + full_url

            elif href.startswith('/'):
                baseUrl = url[0: url.find(domain) + len(domain)] + "." + tldextract.extract(url).suffix
                full_url = baseUrl + href
            else:
                continue

            assignedQueueIndex = 0
            for char in full_url:
                assignedQueueIndex += ord(char)
            assignedQueueIndex = assignedQueueIndex % NUM_WORKERS
            queuePool[assignedQueueIndex].put((full_url, numHops)) 
            
    except Exception as e:
        #print(e)
        return

def crawler(id: int, queuePool: Array, shared_total_size: float, lock, shared_fingerprints) -> None: 
    curr_file_size = 0 
    visited_urls = {}
    assignedQueue: Queue  = queuePool[id]
    filename = input_output_dir + '/data_' + str(id) + '.json' 
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    outfile = open(filename, 'w')
    outfile.write('[\n')
    # crawl starting with given seed pages
    for url in seed_urls:
        numHops = 0
        crawl(url, queuePool, visited_urls, outfile, numHops, shared_fingerprints)
    
    while (shared_total_size.value <= TARGET_SIZE):
        getQueueTuple = assignedQueue.get()
        url = getQueueTuple[0]
        numHops = getQueueTuple[1]
        # print("url: ", url, "hops :", numHops)
        if(numHops < MAX_HOPS):
            crawl(url, queuePool, visited_urls, outfile, numHops, shared_fingerprints)

        temp_file_size = os.path.getsize(filename) / (1024*1024.0)
        new_data_size = temp_file_size - curr_file_size
        curr_file_size = temp_file_size
        with lock:
            shared_total_size.value += new_data_size
            #print(filename[5:], ":", '%0.2f' % curr_file_size, 'MB', '    Total Size: ', '%0.2f' % shared_total_size.value, 'MB')

        if (shared_total_size.value >= TARGET_SIZE): break 
    outfile.write(']')
    outfile.close()

def hashDoc(textBody, shared_fingerprints):
    wordList = textBody.split() #split the string input into a list of words
    
    finalFingerPrint = ""
    for word in wordList: #go through each word in doc
        hashedWord = hashlib.sha1(word.encode("utf-8")).hexdigest() #hash each word
        finalFingerPrint += hashedWord #concatenate hash of each word
    finalFingerPrint = hashlib.sha1(finalFingerPrint.encode("utf-8")).hexdigest() #rehash the concatenation
    finalFingerPrint = format(int(finalFingerPrint, 16), '064b')

    if(finalFingerPrint not in shared_fingerprints): 
        shared_fingerprints[finalFingerPrint] = 0
    else:
        return -1
    return finalFingerPrint


if __name__ == '__main__':
    with Pool(processes=NUM_WORKERS) as pool:
        with Manager() as manager:
            queuePool = [manager.Queue() for i in range(NUM_WORKERS)]
            shared_total_size = manager.Value('f', 0.0)
            lock = manager.Lock() 
            shared_fingerprints = manager.dict()    
            poolArguments = [(i, queuePool, shared_total_size, lock, shared_fingerprints) for i in range(NUM_WORKERS)]
            pool.starmap(crawler, poolArguments)
