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

from flask import Flask, request, render_template
app = Flask(__name__)

@app.route("/")
def home():
    return render_template('input.html')  #home page html here

def retrieve(storedir, query):
    searchDir = NIOFSDirectory(Paths.get(storedir))
    searcher = IndexSearcher(DirectoryReader.open(searchDir))

    parser = QueryParser('Context', StandardAnalyzer())
    parsed_query = parser.parse(query)

    topDocs = searcher.search(parsed_query, 10).scoreDocs
    topkdocs = []
    for hit in topDocs:
        doc = searcher.doc(hit.doc)
        topkdocs.append({
            "score": hit.score,
            "url": doc.get("Url"),
            "title": doc.get("Title"),
            "context": doc.get("Context")[0:360]
        })

    print(topkdocs)
    return topkdocs

@app.route("/submit", methods = ['POST', 'GET'])
def search():
    if request.method == 'GET':
        return render_template('input.html')
    if request.method == 'POST':
        form = request.form
        query = form['query']

        # prevents crash if query is empty
        if not query:
            return render_template('output.html', lucene_output = [], query = query)     

        print(f"query: {query}") #for testing
        lucene.getVMEnv().attachCurrentThread()
        results = retrieve('lucene_index/', str(query))
        print(results) #for testing

        return render_template('output.html', lucene_output = results, query = query)


lucene.initVM(vmargs=['-Djava.awt.headless=true'])

if __name__ == "__main__":
    app.run(debug=True)
