## This project is a modified version of Lucene: https://github.com/apache/lucene


### Implemented Intra-Query parallelism algorithm

Each full-text query can be processed by multiple threads. Search latency is reduced by about 45%.

The idea is indeed intuitive. In Lucene, indexes are stored in multiple segment files, and during a search query, it searches through these files to find matches. Therefore, to enhance efficiency, we can employ multiple threads to concurrently process the search operation.

By utilizing multiple threads, each thread can handle a subset of the segment files simultaneously, allowing for parallel processing of the search query. 

### Added a new method in IndexReader to read documents with multiple threads
```java
public List<Document> docs(int... docIDs) throws IOException {
    return reader.documents(docIDs);
}
```


### Added a new class `SearchWorker` in the package `search`

### Modified the classes 
* `TopScoreDocCollector`
* `HitQueue`
* `TopDocsCollector`
* `MaxScoreAccumulator`
* `TopFieldCollector`
* `BaseCompositeReader`
* `IndexReader`
* `IndexSearcher`
