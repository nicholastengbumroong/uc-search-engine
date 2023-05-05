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
    
def sameDomainOrEdu(url, seed_url):
    domain = tldextract.extract(url).domain
    suffix = tldextract.extract(url).suffix
    seed_domain = tldextract.extract(seed_url).domain
    seed_suffix = tldextract.extract(seed_url).suffix
    if (suffix == "edu" or (domain == seed_domain and suffix == seed_suffix)):
        return True
    return False

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
            return
        
        data = {"url": url, "body": body}
        json.dump(data, outfile)
        outfile.write('\n')
        visited_urls[url] = 0
        
        
        numHops += 1
        links = soup.find_all('a')
        for link in links:
            href = link.get('href')
            # check to see if url is part of same domain or an edu page 
            if href is None:
                continue
            if href.startswith('http'): # check if href is its own link
                if sameDomainOrEdu(href, url):  # check to see if link is an edu page 
                    full_url = href
                else:
                    continue
            else:                       # else it is part of same domain
                full_url = url + href
                if not sameDomainOrEdu(full_url, url):
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
            print(filename[5:], ":", '%0.2f' % curr_file_size, 'MB', '    Total Size: ', '%0.2f' % shared_total_size.value, 'MB')

        if (shared_total_size.value >= TARGET_SIZE): break 

    outfile.close()

def hashDoc(textBody, shared_fingerprints):
    wordList = textBody.split() #split the string input into a list of words
    bWordList = [] #word list but after hashing into binary
    hashAlg = 2 # 1 = in class hashing algorithm from slides, 2 = SHA-1 hashing algorithm

    if(hashAlg == 1):
        for word in wordList: #for each word in the list
            wordSum = 0
            for char in word: #for each character in the word 
                wordSum += ord(char) #get the ascii value of the character and sum it across the word
            binWordSum = format(wordSum, '064b') #turn wordSum into binary format
            bWordList.append(binWordSum) 
        finalFingerPrint = ""
        for i in range(64): #iterates through the column of each word hashed in binary
            colSum = 0
            for j in range(len(bWordList)): #adds -1 or 1 depending on whether it is a 1 or 0 for that word
                if bWordList[j][i] == '1':
                    colSum += 1
                else:
                    colSum += -1

            if(colSum > 0):
                finalFingerPrint += ('1')
            else:
                finalFingerPrint += ('0')
        if(dupeCheck(finalFingerPrint, shared_fingerprints) != -1): 
            shared_fingerprints.append(finalFingerPrint)
            return finalFingerPrint
        else:
            return -1
    
    if(hashAlg == 2):
        finalFingerPrint = ""
        for word in wordList: #go through each word in doc
            hashedWord = hashlib.sha1(word.encode("utf-8")).hexdigest() #hash each word
            finalFingerPrint += hashedWord #concatenate hash of each word
        finalFingerPrint = hashlib.sha1(finalFingerPrint.encode("utf-8")).hexdigest() #rehash the concatenation
        finalFingerPrint = format(int(finalFingerPrint, 16), '064b')

    if(dupeCheck(finalFingerPrint, shared_fingerprints) != -1): 
        shared_fingerprints.append(finalFingerPrint)
    else:
        return -1
    return finalFingerPrint

def dupeCheck(simHash, shared_fingerprints):
    matchingBits = 0
    for currHash in shared_fingerprints: #for every hash in the array
        matchingBits = 0
        for bit in range(64): #go through all 64 bits
            if currHash[bit] == simHash[bit]: #if they match, increment matching bits
                matchingBits += 1
        if(matchingBits >= 54): #if more than 54/64 bits match, return -1 to indicate its a dupe
            return -1
    shared_fingerprints.append(simHash)
    return simHash

if __name__ == '__main__':
    with Pool(processes=NUM_WORKERS) as pool:
        with Manager() as manager:
            queuePool = [manager.Queue() for i in range(NUM_WORKERS)]
            shared_total_size = manager.Value('f', 0.0)
            lock = manager.Lock() 
            shared_fingerprints = manager.list()    
            poolArguments = [(i, queuePool, shared_total_size, lock, shared_fingerprints) for i in range(NUM_WORKERS)]
            pool.starmap(crawler, poolArguments)
