import logging, sys
logging.disable(sys.maxsize)

import lucene
import os
import json
from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, NIOFSDirectory
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, StringField
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader
from org.apache.lucene.search import IndexSearcher, BoostQuery, Query
from org.apache.lucene.search.similarities import BM25Similarity


documents = []
crawled_data_path = "/home/cs172/CS172-Project/Phase2/crawled_data"
for file in os.listdir(crawled_data_path):
    if file.endswith("json"):
        print(file)
        with open(os.path.join(crawled_data_path, file)) as crawled_data_file:
            crawled_data_json = json.load(crawled_data_file)
            documents += crawled_data_json
print(len(documents))

def create_index(dir):
    if not os.path.exists(dir):
        os.mkdir(dir)
    store = SimpleFSDirectory(Paths.get(dir))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(store, config)

    metaType = FieldType()
    metaType.setStored(True)
    metaType.setTokenized(False)

    contextType = FieldType()
    contextType.setStored(True)
    contextType.setTokenized(True)
    contextType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

    for document in documents:
        title = document['title']
        url = document['url']
        body = document['body']

        doc = Document()
        doc.add(Field('Title', str(title), metaType))
        doc.add(Field('Context', str(body), contextType))
        doc.add(Field('Url', str(url), StringField.TYPE_STORED))
        writer.addDocument(doc)
    writer.close()

lucene.initVM(vmargs=['-Djava.awt.headless=true'])
create_index('lucene_index/')
