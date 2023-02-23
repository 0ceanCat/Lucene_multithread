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

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.Executors;

public class LuceneSpeedTest {
    private IndexSearcher isearcher;
    private IndexWriter indexWriter;
    private final static String INDEX_PATH = "src/main/resources/index_test";

    public List<Document> search(String filename) {
        if (isearcher == null) {
            getIndexSearcher();
        }
        //List<List<String>> doclists = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line = br.readLine();
            long start = System.currentTimeMillis();
            int i = 0;
            while (true){
                i++;
                if (line == null) break;
               // List<String> doclist = new ArrayList<>();
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (String s : line.split(" ")) {
                    builder.add(new TermQuery(new Term("review_body", s)), BooleanClause.Occur.SHOULD)
                            .build();
                }

                TopDocs docs = isearcher.search(builder.build(), 100);
                ScoreDoc[] scoreDocs = docs.scoreDocs;
                try{
                    for (ScoreDoc scoreDoc : scoreDocs) {
                        int doc = scoreDoc.doc;
                        Document doc1 = isearcher.doc(doc);
                    //    doclist.add(doc1.get("review_body"));
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("error " + i);
                    return null;
                }

                //doclists.add(doclist);
                line = br.readLine();
            };
            System.out.println(System.currentTimeMillis() - start);

            /*FileWriter fw = new FileWriter("results.txt");
            for (List<String> doclist : doclists) {
                fw.write(doclist.toString());
                fw.write("\n");
            }
            fw.close();*/

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

    public void readFile(String filename) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        br.readLine();
        getIndexWriter();
        while (true){
            String line = br.readLine();
            if (line == null) break;
            String[] content = line.split("\t");
            Document document = new Document();
            document.add(new StoredField("id", content[2]));
            document.add(new TextField("review_body", content[13], Field.Store.YES));
            //将文档追加到索引库中
            indexWriter.addDocument(document);
        }
        indexWriter.commit();
    }
    public static void main(String[] args) throws IOException {
        long s = System.currentTimeMillis();
        //String file = "D:\\Universidade\\tese\\lucene_test_dataset\\amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv";
        String file = "D:\\Universidade\\tese\\lucene_test_dataset\\queries.txt";
        LuceneSpeedTest d = new LuceneSpeedTest();
        d.search(file);
        System.out.println("done");
        //d.readFile(file);
        //System.out.println(System.currentTimeMillis() - s);


        // 553 ms without threads
        // 278 ms with threads
    }
}
