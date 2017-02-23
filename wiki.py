import xml.etree.ElementTree as etree
from multiprocessing import Process, Queue, JoinableQueue, Pool
import os, sys
from corenlp_pywrap import pywrap
from pymongo import MongoClient
import _pickle
import re
from time import sleep
import random
import requests
import json

ns = "{http://www.mediawiki.org/xml/export-0.10/}"
lines = 945682732
_MEDIA_CAT = """
  [Ii]mage|[Cc]ategory      # English
 |[Aa]rchivo                # Spanish
 |[Ff]ile                   # English, Italian
 |[CcKk]at[ée]gor[íi][ea]   # Dutch, German, French, Italian, Spanish, Polish, Latin
 |[Bb]estand                # Dutch
 |[Bb]ild                   # German
 |[Ff]icher                 # French
 |[Pp]lik                   # Polish
 |[Ff]asciculus             # Latin
"""

_UNWANTED = re.compile(r"""
  (:?
    \{\{ .*? \}\}                           # templates
  | \| .*? \n                               # left behind from templates
  | \}\}                                    # left behind from templates
  | <!-- .*? -->
  | <div .*?> .*? </div>
  | <math> .*? </math>
  | <nowiki> .*? </nowiki>
  | <ref .*?> .*? </ref>
  | <ref .*?/>
  | <span .*?> .*? </span>
  | \[\[ (:?%s): (\[\[.*?\]\]|.)*? \]\]
  | \[\[ [a-z]{2,}:.*? \]\]                 # interwiki links
  | =+                                      # headers
  | \{\| .*? \|\}
  | \[\[ (:? [^]]+ \|)?
  | \]\]
  | '{2,}
  )
""" % _MEDIA_CAT,
re.DOTALL | re.MULTILINE | re.VERBOSE)

def get_server_response(text, ip):
    url = 'http://172.17.0.{}:9000/?properties={{"annotators": "tokenize,ssplit", "outputFormat": "json"}}'.format(ip)
    if len(text) > 100000 or len(text) < 15:
        return {'sentences': []}
    try:
        r = requests.post(url, text).text
    except requests.exceptions.ConnectionError:
        r = None
    retry = 0
    while retry < 100:
        if r == "Could not handle incoming annotation" or not r:
            print(r)
            sleep(3)
            retry += 1
            try:
                r = requests.post(url, text).text
            except requests.exceptions.ConnectionError:
                print("server failing")
                sleep(7)
        else:
            break
    try:
        j = json.loads(r, encoding='utf-8')
    except ValueError:
        print(r)
        return {'sentences': []}
    return j
def text_only(text):
    return _UNWANTED.sub("", text)
def init_worker():
    full_annotator_list = ["tokenize", "ssplit"]
    global ip, c
    ip = random.choice([2, 3, 4])
    c = MongoClient('10.0.1.40')
def process_page(elem):
    text = elem.findall(".//revision/text")[0].text
    title = elem.findall(".//title")[0].text
    if not text:
        return
    if text.startswith("#REDIRECT") or text.startswith('#redirect'):
        return
    paras = filter(lambda x: len(x) > 25, map(text_only,
       re.split(r'==.*?==|\n[\n]+', text)))
    doc = {}
    doc['title'] = title
    doc['paras'] = []
    for para in paras:
        para = para.encode('latin-1', 'ignore').decode('latin-1')
        if re.match(r'[\S]+', para):
            sentences = None
            retry = 0
            while not sentences:
                sentences = get_server_response(para, ip)['sentences']
            if not sentences:
                continue
            endOfLast = 0
            doc['paras'].append([])
            for sent in sentences:
                last_word = sent['tokens'][-1]
                sentence_text = para[endOfLast:last_word['characterOffsetEnd']]
                endOfLast = last_word['characterOffsetEnd']
                tokens = list(map(lambda x: x['word'], sent['tokens']))
                doc['paras'][-1].append({'text': sentence_text, 'tokens':
                    tokens})
    if doc['paras']:
        return doc

def process(inds):
    index, nextIndex = inds
    with open('wiki.xml', 'rb') as f:
        f.seek(index)
        if not nextIndex:
            data = f.read().decode('utf-8')
        else:
            data = f.read(nextIndex - index).decode('utf-8')
        elem = etree.fromstring(data)
        if elem:
            doc = process_page(elem)
        else:
            doc = None
    if doc:
        c['corpora']['wiki'].insert_one(doc)
    return doc

if __name__ == '__main__':
    with open('indices.pkl', 'rb') as f:
        indices = _pickle.load(f)
    pool = Pool(10, init_worker)
    zip_indice = list(zip(indices, indices[1:] + [None]))
    print("Pool Started")
    pool.map(process, zip_indice)
    pool.close()
    pool.join()
