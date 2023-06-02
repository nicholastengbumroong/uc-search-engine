import logging, sys
logging.disable(sys.maxsize)

import lucene
import os
from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, NIOFSDirectory
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader
from org.apache.lucene.search import IndexSearcher, BoostQuery, Query
from org.apache.lucene.search.similarities import BM25Similarity

sample_doc = [
    {
        'title' : 'A',
        'context' : 'lucene is a useful tool for searching and information retrieval'
        },
    {
        'title' : 'B',
        'context' : 'Bert is a deep learning transformer model for encoding textual data'
    },
    {
        'title' : 'C',
        'context' : 'Django is a python web framework for building backend web APIs'
    }
]


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

    for sample in sample_doc:
        title = sample['title']
        context = sample['context']

        doc = Document()
        doc.add(Field('Title', str(title), metaType))
        doc.add(Field('Context', str(context), contextType))
        writer.addDocument(doc)
    writer.close()

lucene.initVM(vmargs=['-Djava.awt.headless=true'])
create_index('sample_lucene_index/')
