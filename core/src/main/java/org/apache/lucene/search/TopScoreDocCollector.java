/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MaxScoreAccumulator.DocAndScore;
import org.apache.lucene.util.PriorityQueue;

/**
 * A {@link Collector} implementation that collects the top-scoring hits, returning them as a {@link
 * TopDocs}. This is used by {@link IndexSearcher} to implement {@link TopDocs}-based search. Hits
 * are sorted by score descending and then (when the scores are tied) docID ascending. When you
 * create an instance of this collector you should know in advance whether documents are going to be
 * collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and {@link Float#NEGATIVE_INFINITY} are not valid
 * scores. This collector will not properly collect hits with such scores.
 */
public abstract class TopScoreDocCollector extends TopDocsCollector<ScoreDoc> {

    /**
     * Scorable leaf collector
     */
    public abstract static class ScorerLeafCollector implements LeafCollector {
        protected ScorerLeafCollector() {
        }

        protected ScorerLeafCollector(int docBase,
                                      float minCompetitiveScore,
                                      ScoreDoc pqTop,
                                      TotalHits.Relation totalHitsRelation) {
            this.docBase = docBase;
            this.pqTop = pqTop;
            this.minCompetitiveScore = minCompetitiveScore;
            this.totalHitsRelation = totalHitsRelation;
            this.initDoc = true;
            this.returned = false;
        }

        protected Scorable scorer;
        protected int docBase;
        protected boolean returned;
        protected float minCompetitiveScore;
        protected ScoreDoc pqTop;
        protected ScoreDoc addedback;
        protected boolean initDoc;
        protected TotalHits.Relation totalHitsRelation;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
        }
    }

    private static class SimpleTopScoreDocCollector extends TopScoreDocCollector {

        SimpleTopScoreDocCollector(
                int numHits, HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
            super(numHits, hitsThresholdChecker, minScoreAcc, true);
            updated = new HitQueue(numHits,false);
            toBeUpdated = pq;
        }

        PriorityQueue<ScoreDoc> updated;
        PriorityQueue<ScoreDoc> toBeUpdated;

        final ReentrantLock toBeUpdatedLock = new ReentrantLock();
        final ReentrantLock updatedLock = new ReentrantLock();

        protected ScoreDoc updateTop(Supplier<ScoreDoc> defaultV) {
            try {
                toBeUpdatedLock.lock();
                if (toBeUpdated.size() == 0){
                    swap();
                }

                if (updated.size() == 0 || updated.top().compareTo(toBeUpdated.top()) >= 0){
                    return toBeUpdated.pop();
                }
            }finally {
                toBeUpdatedLock.unlock();
            }
            return popUpdated();
        }
        protected void add(ScoreDoc t) {
            try{
                updatedLock.lock();
                if (updated.size() >= numHits){
                    swap();
                }
                updated.add(t);
            }finally {
                updatedLock.unlock();
            }
        }

        protected ScoreDoc popUpdated() {
            try{
                updatedLock.lock();
                return updated.pop();
            }finally {
                updatedLock.unlock();
            }
        }

        protected void addback(ScoreDoc t) {
            try{
                toBeUpdatedLock.lock();
                toBeUpdated.add(t);
            }finally {
                toBeUpdatedLock.unlock();
            }
        }

        synchronized void swap(){
            if (toBeUpdated.size() > updated.size()) return;
            PriorityQueue<ScoreDoc> temp = toBeUpdated;
            toBeUpdated = updated;
            updated = temp;
        }

        protected int topDocsSize() {
            // In case pq was populated with sentinel values, there might be less
            // results than pq.size(). Therefore return all results until either
            // pq.size() or totalHits.
            return Math.min(totalHits.get(), updated.size() + toBeUpdated.size());
        }

        public TopDocs topDocs(int start, int howMany) {

            // In case pq was populated with sentinel values, there might be less
            // results than pq.size(). Therefore return all results until either
            // pq.size() or totalHits.

            int size = topDocsSize();

            if (howMany < 0) {
                throw new IllegalArgumentException(
                        "Number of hits requested must be greater than 0 but value was " + howMany);
            }

            if (start < 0) {
                throw new IllegalArgumentException(
                        "Expected value of starting position is between 0 and " + size + ", got " + start);
            }

            if (start >= size || howMany == 0) {
                return newTopDocs(null, start);
            }

            // We know that start < pqsize, so just fix howMany.
            howMany = Math.min(size - start, howMany);
            ScoreDoc[] results = new ScoreDoc[howMany];

            // pq's pop() returns the 'least' element in the queue, therefore need
            // to discard the first ones, until we reach the requested range.
            // Note that this loop will usually not be executed, since the common usage
            // should be that the caller asks for the last howMany results. However it's
            // needed here for completeness.
            for (int i = updated.size() + toBeUpdated.size() - start - howMany; i > 0; i--) {
                if (updated.size() > 0 && toBeUpdated.size() > 0){
                    if (updated.top().compareTo(toBeUpdated.top()) < 0){
                        updated.pop();
                    }else {
                        toBeUpdated.pop();
                    }
                } else if (updated.size() > 0) {
                    updated.pop();
                }else {
                    toBeUpdated.pop();
                }
            }

            // Get the requested results from pq.
            populateResults(results, howMany);

            return newTopDocs(results, start);
        }

        protected void populateResults(ScoreDoc[] results, int howMany) {
            for (int i = howMany - 1; i >= 0; i--) {
                if (updated.size() > 0 && toBeUpdated.size() > 0){
                    if (updated.top().compareTo(toBeUpdated.top()) < 0){
                        results[i] = updated.pop();
                    }else {
                        results[i] =toBeUpdated.pop();
                    }
                } else if (updated.size() > 0) {
                    results[i] = updated.pop();
                }else {
                    results[i] =toBeUpdated.pop();
                }
            }
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            // reset the minimum competitive score
            //docBase = context.docBase;
            //minCompetitiveScore = 0f;
            return new ScorerLeafCollector(context.docBase, 0f, null, totalHitsRelation) {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    super.setScorer(scorer);
                    if (minScoreAcc == null) {
                        updateMinCompetitiveScore(this);
                    } else {
                        updateGlobalMinCompetitiveScore(this);
                    }
                }

                @Override
                public void collect(int doc) throws IOException {
                    pqTop = updateTop(supplier);
                    float score = scorer.score();
                    //System.out.println(doc);
                    // This collector relies on the fact that scorers produce positive values:
                    assert score >= 0; // NOTE: false for NaN

                    int hits = totalHits.incrementAndGet();
                    hitsThresholdChecker.incrementHitCount();

                    if (minScoreAcc != null && (hits & minScoreAcc.modInterval) == 0) {
                        updateGlobalMinCompetitiveScore(this);
                    }

                    if (score <= pqTop.score) {
                        if (this.totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                            // we just reached totalHitsThreshold, we can start setting the min
                            // competitive score now
                            updateMinCompetitiveScore(this);
                        }
                        // Since docs are returned in-order (i.e., increasing doc Id), a document
                        // with equal score to pqTop.score cannot compete since HitQueue favors
                        // documents with lower doc Ids. Therefore reject those docs too.
                        addback(pqTop);
                        return;
                    }
                    pqTop.doc = doc + this.docBase;
                    pqTop.score = score;
                    add(pqTop);
                    returned = false;
                    updateMinCompetitiveScore(this);
                }
            };
        }
    }

    private static class PagingTopScoreDocCollector extends TopScoreDocCollector {

        private final ScoreDoc after;
        private int collectedHits;

        PagingTopScoreDocCollector(
                int numHits,
                ScoreDoc after,
                HitsThresholdChecker hitsThresholdChecker,
                MaxScoreAccumulator minScoreAcc) {
            super(numHits, hitsThresholdChecker, minScoreAcc);
            this.after = after;
            this.collectedHits = 0;
        }

        @Override
        protected int topDocsSize() {
            return Math.min(collectedHits, pq.size());
        }

        @Override
        protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
            return results == null
                    ? new TopDocs(new TotalHits(totalHits.get(), totalHitsRelation), new ScoreDoc[0])
                    : new TopDocs(new TotalHits(totalHits.get(), totalHitsRelation), results);
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            docBase = context.docBase;
            final int afterDoc = after.doc - context.docBase;
            minCompetitiveScore = 0f;

            return new ScorerLeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    super.setScorer(scorer);
                    if (minScoreAcc == null) {
                        updateMinCompetitiveScore(scorer);
                    } else {
                        updateGlobalMinCompetitiveScore(scorer);
                    }
                }

                @Override
                public void collect(int doc) throws IOException {
                    float score = scorer.score();

                    // This collector relies on the fact that scorers produce positive values:
                    assert score >= 0; // NOTE: false for NaN

                    totalHits.incrementAndGet();
                    hitsThresholdChecker.incrementHitCount();

                    if (minScoreAcc != null && (totalHits.get() & minScoreAcc.modInterval) == 0) {
                        updateGlobalMinCompetitiveScore(scorer);
                    }

                    if (score > after.score || (score == after.score && doc <= afterDoc)) {
                        // hit was collected on a previous page
                        if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                            // we just reached totalHitsThreshold, we can start setting the min
                            // competitive score now
                            updateMinCompetitiveScore(scorer);
                        }
                        return;
                    }

                    if (score <= pqTop.score) {
                        if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                            // we just reached totalHitsThreshold, we can start setting the min
                            // competitive score now
                            updateMinCompetitiveScore(scorer);
                        }

                        // Since docs are returned in-order (i.e., increasing doc Id), a document
                        // with equal score to pqTop.score cannot compete since HitQueue favors
                        // documents with lower doc Ids. Therefore reject those docs too.
                        return;
                    }
                    collectedHits++;
                    pqTop.doc = doc + docBase;
                    pqTop.score = score;
                    pqTop = pq.updateTop();
                    updateMinCompetitiveScore(scorer);
                }
            };
        }
    }

    /**
     * Creates a new {@link TopScoreDocCollector} given the number of hits to collect and the number
     * of hits to count accurately.
     *
     * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly {@code
     * totalHitsThreshold} then this value is accurate. On the other hand, if the {@link
     * TopDocs#totalHits} value is greater than {@code totalHitsThreshold} then its value is a lower
     * bound of the hit count. A value of {@link Integer#MAX_VALUE} will make the hit count accurate
     * but will also likely make query processing slower.
     *
     * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
     * <code>numHits</code>, and fill the array with sentinel objects.
     */
    public static TopScoreDocCollector create(int numHits, int totalHitsThreshold) {
        return create(numHits, null, totalHitsThreshold);
    }

    /**
     * Creates a new {@link TopScoreDocCollector} given the number of hits to collect, the bottom of
     * the previous page, and the number of hits to count accurately.
     *
     * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly {@code
     * totalHitsThreshold} then this value is accurate. On the other hand, if the {@link
     * TopDocs#totalHits} value is greater than {@code totalHitsThreshold} then its value is a lower
     * bound of the hit count. A value of {@link Integer#MAX_VALUE} will make the hit count accurate
     * but will also likely make query processing slower.
     *
     * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
     * <code>numHits</code>, and fill the array with sentinel objects.
     */
    public static TopScoreDocCollector create(int numHits, ScoreDoc after, int totalHitsThreshold) {
        return create(
                numHits, after, HitsThresholdChecker.create(Math.max(totalHitsThreshold, numHits)), null);
    }

    static TopScoreDocCollector create(
            int numHits,
            ScoreDoc after,
            HitsThresholdChecker hitsThresholdChecker,
            MaxScoreAccumulator minScoreAcc) {

        if (numHits <= 0) {
            throw new IllegalArgumentException(
                    "numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
        }

        if (hitsThresholdChecker == null) {
            throw new IllegalArgumentException("hitsThresholdChecker must be non null");
        }

        if (after == null) {
            return new SimpleTopScoreDocCollector(numHits, hitsThresholdChecker, minScoreAcc);
        } else {
            return new PagingTopScoreDocCollector(numHits, after, hitsThresholdChecker, minScoreAcc);
        }
    }

    /**
     * Create a CollectorManager which uses a shared hit counter to maintain number of hits and a
     * shared {@link MaxScoreAccumulator} to propagate the minimum score accross segments
     */
    public static CollectorManager<TopScoreDocCollector, TopDocs> createSharedManager(
            int numHits, ScoreDoc after, int totalHitsThreshold) {

        int totalHitsMax = Math.max(totalHitsThreshold, numHits);
        return new CollectorManager<>() {

            private final HitsThresholdChecker hitsThresholdChecker =
                    HitsThresholdChecker.createShared(totalHitsMax);
            private final MaxScoreAccumulator minScoreAcc =
                    totalHitsMax == Integer.MAX_VALUE ? null : new MaxScoreAccumulator();

            @Override
            public TopScoreDocCollector newCollector() throws IOException {
                return TopScoreDocCollector.create(numHits, after, hitsThresholdChecker, minScoreAcc);
            }

            @Override
            public TopDocs reduce(Collection<TopScoreDocCollector> collectors) throws IOException {
                final TopDocs[] topDocs = new TopDocs[collectors.size()];
                int i = 0;
                for (TopScoreDocCollector collector : collectors) {
                    topDocs[i++] = collector.topDocs();
                }
                return TopDocs.merge(0, numHits, topDocs);
            }
        };
    }

    int docBase;
    ScoreDoc pqTop;
    final HitsThresholdChecker hitsThresholdChecker;
    final MaxScoreAccumulator minScoreAcc;
    final int numHits;
    float minCompetitiveScore;
    final Supplier<ScoreDoc> supplier = () -> new ScoreDoc(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);

    // prevents instantiation
    TopScoreDocCollector(
            int numHits, HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
        super(numHits, new HitQueue(numHits, true));
        assert hitsThresholdChecker != null;

        // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
        // that at this point top() is already initialized.
        pqTop = pq.top();
        this.hitsThresholdChecker = hitsThresholdChecker;
        this.minScoreAcc = minScoreAcc;
        this.numHits = numHits;
    }

    TopScoreDocCollector(
            int numHits, HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc, boolean prePopulate) {
        super(numHits, new HitQueue(numHits, prePopulate));
        assert hitsThresholdChecker != null;

        // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
        // that at this point top() is already initialized.
        pqTop = null;
        this.hitsThresholdChecker = hitsThresholdChecker;
        this.minScoreAcc = minScoreAcc;
        this.numHits = numHits;
    }

    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
        if (results == null) {
            return EMPTY_TOPDOCS;
        }

        return new TopDocs(new TotalHits(totalHits.get(), totalHitsRelation), results);
    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.scoreMode();
    }

    protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
        assert minScoreAcc != null;
        DocAndScore maxMinScore = minScoreAcc.get();
        if (maxMinScore != null) {
            // since we tie-break on doc id and collect in doc id order we can require
            // the next float if the global minimum score is set on a document id that is
            // smaller than the ids in the current leaf
            float score =
                    docBase >= maxMinScore.docBase ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
            if (score > minCompetitiveScore) {
                assert hitsThresholdChecker.isThresholdReached();
                scorer.setMinCompetitiveScore(score);
                minCompetitiveScore = score;
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
            }
        }
    }

    protected void updateGlobalMinCompetitiveScore(ScorerLeafCollector collector) throws IOException {
        assert minScoreAcc != null;
        DocAndScore maxMinScore = minScoreAcc.get();
        if (maxMinScore != null) {
            // since we tie-break on doc id and collect in doc id order we can require
            // the next float if the global minimum score is set on a document id that is
            // smaller than the ids in the current leaf
            float score =
                    collector.docBase >= maxMinScore.docBase ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
            if (score > collector.minCompetitiveScore) {
                assert hitsThresholdChecker.isThresholdReached();
                collector.scorer.setMinCompetitiveScore(score);
                collector.minCompetitiveScore = score;
                collector.totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
            }
        }
    }

    protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
        if (hitsThresholdChecker.isThresholdReached()
                && pqTop != null
                && pqTop.score != Float.NEGATIVE_INFINITY) { // -Infinity is the score of sentinels
            // since we tie-break on doc id and collect in doc id order, we can require
            // the next float
            float localMinScore = Math.nextUp(pqTop.score);
            if (localMinScore > minCompetitiveScore) {
                scorer.setMinCompetitiveScore(localMinScore);
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                minCompetitiveScore = localMinScore;
                if (minScoreAcc != null) {
                    // we don't use the next float but we register the document
                    // id so that other leaves can require it if they are after
                    // the current maximum
                    minScoreAcc.accumulate(docBase, pqTop.score);
                }
            }
        }
    }

    protected void updateMinCompetitiveScore(ScorerLeafCollector collector) throws IOException {
        if (hitsThresholdChecker.isThresholdReached()
                && collector.pqTop != null
                && collector.pqTop.score != Float.MIN_VALUE) { // -Infinity is the score of sentinels
            // since we tie-break on doc id and collect in doc id order, we can require
            // the next float
            float localMinScore = Math.nextUp(collector.pqTop.score);
            if (localMinScore > collector.minCompetitiveScore) {
                collector.scorer.setMinCompetitiveScore(localMinScore);
                collector.totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                collector.minCompetitiveScore = localMinScore;
                if (minScoreAcc != null) {
                    // we don't use the next float but we register the document
                    // id so that other leaves can require it if they are after
                    // the current maximum
                    minScoreAcc.accumulate(collector.docBase, collector.pqTop.score);
                }
            }
        }
    }
}
