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

seed_urls = []
currentFingerprints = []
workers = 8
maxHops = 6
TARGET_SIZE = 500        # in MB
MAX_FILE_SIZE = TARGET_SIZE / workers  # in MB  


with open('seeds.txt', 'r') as seeds:
    for seed in seeds:
        seed_urls.append(str(seed.strip()))
    
def sameDomainOrEdu(url, seed_url):
    domain = tldextract.extract(url).domain
    suffix = tldextract.extract(url).suffix
    seed_domain = tldextract.extract(seed_url).domain
    seed_suffix = tldextract.extract(seed_url).suffix
    if ((suffix == "edu" or (domain == seed_domain and suffix == seed_suffix)) and not url.endswith(".html") and not url.endswith(".pdf")):
        return True
    return False
     
def crawl(url, queuePool: Array, visited_urls, outfile) -> None:
    if(url in visited_urls):
        return
    try:
        #print('fetching url', url)
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, 'lxml') 
        body = soup.body.text
        simHash = hashDoc(body)
        if(simHash == -1): #possibly gonna be used to check dupes like this
            return
        

        data = {"url": url, "body": body}
        json.dump(data, outfile)
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
    except Exception as e:
        print(e)

def crawler(id: int, queuePool: Array, shared_total_size: float, lock) -> None: 
    curr_file_size = 0 
    file_cnt = 0 

    visited_urls = {}
    assignedQueue: Queue  = queuePool[id]
    filename = 'data/data_' + str(id) + '_' + str(file_cnt) + '.json' 
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    outfile = open(filename, 'w')
    # crawl starting with given seed pages
    for url in seed_urls:
        crawl(url, queuePool, visited_urls, outfile)
    
    while (shared_total_size.value <= TARGET_SIZE):
        filename = 'data/data_' + str(id) + '_' + str(file_cnt) + '.json' 
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        outfile = open(filename, 'w')

        while (curr_file_size < MAX_FILE_SIZE):
            #hashDoc("nba basketball tournament us countries")
            url = assignedQueue.get()
            crawl(url, queuePool, visited_urls, outfile)

            temp_file_size = os.path.getsize(filename) / (1024*1024.0)
            new_data_size = temp_file_size - curr_file_size
            curr_file_size = temp_file_size
            with lock:
                shared_total_size.value += new_data_size
                # print(filename[5:], ":", '%0.2f' % curr_file_size, 'MB', '    Total Size: ', '%0.2f' % shared_total_size.value, 'MB')

            if (shared_total_size.value >= TARGET_SIZE): break 

        curr_file_size = 0
        outfile.close()
        file_cnt += 1

if __name__ == '__main__':
    with Pool(processes=workers) as pool:
        with Manager() as manager:
            queuePool = [manager.Queue() for i in range(workers)]
            shared_total_size = manager.Value('f', 0.0)
            lock = manager.Lock() 
            poolArguments = [(i, queuePool, shared_total_size, lock) for i in range(workers)]
            pool.starmap(crawler, poolArguments)

"""
- Still need to make it stop crawling after a certain period of time/or data limit
- Need to account for number of pages to crawl and number of hops
- needs to handle duplicate pages
- There might be some other smaller stuff missed or that can be improved 
This is a very rough draft of our crawler
"""

def hashDoc(textBody):
    wordList = textBody.split() #split the string input into a list of words
    bWordList = [] #word list but after hashing into binary
    hashAlg = 2 # 1 = in class hashing algorithm from slides, 2 = SHA-1 hashing algorithm

    if(hashAlg == 1):
        for word in wordList: #for each word in the list
            wordSum = 0
            for char in word: #for each character in the word 
                #print(char)
                wordSum += ord(char) #get the ascii value of the character and sum it across the word
            #print(wordSum)
            #wordSum = wordSum % 65536 # no mod for 64 bits
            binWordSum = format(wordSum, '064b') #turn wordSum into binary format
            bWordList.append(binWordSum) 
            #print(binWordSum)
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
        if(dupeCheck(finalFingerPrint) != -1): 
            currentFingerprints.append(finalFingerPrint)
            return finalFingerPrint
        else:
            return -1
    
    if(hashAlg == 2):
        finalFingerPrint = ""
        for word in wordList: #go through each word in doc
            hashedWord = hashlib.sha1(word.encode("utf-8")).hexdigest() #hash each word
            #print('value: ', hashedWord)
            finalFingerPrint += hashedWord #concatenate hash of each word
        finalFingerPrint = hashlib.sha1(finalFingerPrint.encode("utf-8")).hexdigest() #rehash the concatenation
        finalFingerPrint = format(int(finalFingerPrint, 16), '064b')
        #print('value: ', finalFingerPrint)

    if(dupeCheck(finalFingerPrint) != -1): 
        currentFingerprints.append(finalFingerPrint)
    else:
        #print('dupe: ', finalFingerPrint)
#        fileSimHash = open("testSimHash", 'a')
    #    print('final:', finalFingerPrint)
    #print(textBody.strip())
#        fileSimHash.write('final: ')
#        fileSimHash.write(finalFingerPrint)
#        fileSimHash.write(' dupe text: ')
#        json.dump(textBody, fileSimHash)
#        fileSimHash.write('\n')
#        fileSimHash.write('\n')
#        fileSimHash.write('\n')
    #fileSimHash.write(textBody)
    #fileSimHash.write('\n')
#    fileSimHash.close()
        return -1
#    fileSimHash = open("testSimHash", 'a')
#    print('final:', finalFingerPrint)
    #print(textBody.strip())
#    fileSimHash.write('final: ')
#    fileSimHash.write(finalFingerPrint)
#    fileSimHash.write(' ')
#    json.dump(textBody, fileSimHash)
#    fileSimHash.write('\n')
#    fileSimHash.write('\n')
#    fileSimHash.write('\n')
    #fileSimHash.write(textBody)
    #fileSimHash.write('\n')
#    fileSimHash.close()
    return finalFingerPrint

def dupeCheck(simHash):
    matchingBits = 0
    for currHash in currentFingerprints: #for every hash in the array
        matchingBits = 0
        for bit in range(64): #go through all 64 bits
            if currHash[bit] == simHash[bit]: #if they match, increment matching bits
                matchingBits += 1
        if(matchingBits >= 54): #if more than 54/64 bits match, return -1 to indicate its a dupe
            #print('simhash: ', simHash, 'already there: ', currHash, 'matchingbits: ', matchingBits)
            return -1
    currentFingerprints.append(simHash)
    return simHash