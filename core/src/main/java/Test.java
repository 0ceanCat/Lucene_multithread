import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.PriorityQueue;

public class Test extends PriorityQueue<ScoreDoc> {
    public Test(int maxSize) {
        super(
                maxSize,
                () -> {
                        // Always set the doc Id to MAX_VALUE so that it won't be favored by
                        // lessThan. This generally should not happen since if score is not NEG_INF,
                        // TopScoreDocCollector will always add the object to the queue.
                        return new ScoreDoc(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);

                });
    }

    @Override
    protected final boolean lessThan(ScoreDoc hitA, ScoreDoc hitB) {
        if (hitA.score == hitB.score) {
            return hitA.doc > hitB.doc;
        } else {
            return hitA.score < hitB.score;
        }
    }

    public static void main(String[] args) {
        Test t = new Test(16);
        ScoreDoc scoreDoc = new ScoreDoc(1, 0.5f);
        ScoreDoc scoreDoc2 = new ScoreDoc(2, 0.4f);
        t.add(scoreDoc);
        t.add(scoreDoc2);
        System.out.println();
    }
}
