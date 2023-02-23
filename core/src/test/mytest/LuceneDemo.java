package mytest;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

public class LuceneDemo {
    private IndexSearcher isearcher;
    private IndexWriter indexWriter;
    private final static String INDEX_PATH = "src/main/resources/index";

    public void createIndex() {
        //1.指定索引文件的存储位置，索引具体的表现形式就是一组有规则的文件
        try {
            getIndexWriter();
            indexWriter.deleteAll();

            //4.获取索引源(原始数据)
            List<Image> images = List.of(new Image(1, "12334", "niubi galaxy", 1.5),
                    new Image(1, "5678", "gaga honghong niubi ", 1.5));
            //5.遍历jobInfoList，每次遍历创建一个Document对象
            for (Image image : images) {
                addNewDocument(image, false);
            }
            indexWriter.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addNewDocument(Image image, boolean flush) {
        getIndexWriter();
        Document document = new Document();
        //创建Field对象
        document.add(new StoredField("id", image.getId()));
        //切分词、索引、存储
        document.add(new TextField("caption", image.getCaption(), Field.Store.YES));
        //将文档追加到索引库中
        try {
            indexWriter.addDocument(document);
            if (flush) indexWriter.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateDocument(Image image) {
        deleteDocument(image);
        addNewDocument(image, true);
    }

    public void deleteDocument(Image image) {
        getIndexWriter();
        try {
            indexWriter.deleteDocuments(new Term("id", String.valueOf(image.getId())));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public List<Document> search(String description) {
        if (isearcher == null) {
            getIndexSearcher();
        }
        List<Document> doclist = new ArrayList<>();

        try {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (String s : description.split(" ")) {
                builder.add(new TermQuery(new Term("caption", s)), BooleanClause.Occur.SHOULD)
                        .build();
            }

            TopDocs docs = isearcher.search(builder.build(), 10);
            ScoreDoc[] scoreDocs = docs.scoreDocs;
            for (ScoreDoc scoreDoc : scoreDocs) {
                int doc = scoreDoc.doc;
                doclist.add(isearcher.doc(doc));
            }
            return doclist;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void getIndexSearcher() {
        if (isearcher == null) {
            try {
                Path path = Paths.get(INDEX_PATH);
                Directory directory = FSDirectory.open(path);
                DirectoryReader ireader = DirectoryReader.open(directory);
                isearcher = new IndexSearcher(ireader, Executors.newFixedThreadPool(2));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void getIndexWriter() {
        if (indexWriter == null) {
            try {
                Path path = Paths.get(INDEX_PATH);
                Directory directory = FSDirectory.open(path);
                //2.配置版本及其分词器
                IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
                //3.创建IndexWriter对象，作用就是创建索引
                indexWriter = new IndexWriter(directory, config);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
       LuceneDemo d = new LuceneDemo();
        //d.createIndex();
        System.out.println(d.search("niubi lihai"));
    }
}
