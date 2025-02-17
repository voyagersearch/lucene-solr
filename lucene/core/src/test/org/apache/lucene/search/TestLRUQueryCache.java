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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RamUsageTester;
import org.apache.lucene.util.TestUtil;

public class TestLRUQueryCache extends LuceneTestCase {

  private static final QueryCachingPolicy NEVER_CACHE = new QueryCachingPolicy() {

    @Override
    public void onUse(Query query) {}

    @Override
    public boolean shouldCache(Query query, LeafReaderContext context) throws IOException {
      return false;
    }

  };

  public void testFilterRamBytesUsed() {
    final Query simpleQuery = new TermQuery(new Term("some_field", "some_term"));
    final long actualRamBytesUsed = RamUsageTester.sizeOf(simpleQuery);
    final long ramBytesUsed = LRUQueryCache.QUERY_DEFAULT_RAM_BYTES_USED;
    // we cannot assert exactly that the constant is correct since actual
    // memory usage depends on JVM implementations and settings (eg. UseCompressedOops)
    assertEquals(actualRamBytesUsed, ramBytesUsed, actualRamBytesUsed / 2);
  }

  public void testConcurrency() throws Throwable {
    final LRUQueryCache queryCache = new LRUQueryCache(1 + random().nextInt(20), 1 + random().nextInt(10000), context -> random().nextBoolean());
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final SearcherFactory searcherFactory = new SearcherFactory() {
      @Override
      public IndexSearcher newSearcher(IndexReader reader, IndexReader previous) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCachingPolicy(MAYBE_CACHE_POLICY);
        searcher.setQueryCache(queryCache);
        return searcher;
      }
    };
    final boolean applyDeletes = random().nextBoolean();
    final SearcherManager mgr = new SearcherManager(w.w, applyDeletes, false, searcherFactory);
    final AtomicBoolean indexing = new AtomicBoolean(true);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final int numDocs = atLeast(10000);
    Thread[] threads = new Thread[3];
    threads[0] = new Thread() {
      public void run() {
        Document doc = new Document();
        StringField f = new StringField("color", "", Store.NO);
        doc.add(f);
        for (int i = 0; indexing.get() && i < numDocs; ++i) {
          f.setStringValue(RandomPicks.randomFrom(random(), new String[] {"blue", "red", "yellow"}));
          try {
            w.addDocument(doc);
            if ((i & 63) == 0) {
              mgr.maybeRefresh();
              if (rarely()) {
                queryCache.clear();
              }
              if (rarely()) {
                final String color = RandomPicks.randomFrom(random(), new String[] {"blue", "red", "yellow"});
                w.deleteDocuments(new Term("color", color));
              }
            }
          } catch (Throwable t) {
            error.compareAndSet(null, t);
            break;
          }
        }
        indexing.set(false);
      }
    };
    for (int i = 1; i < threads.length; ++i) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          while (indexing.get()) {
            try {
              final IndexSearcher searcher = mgr.acquire();
              try {
                final String value = RandomPicks.randomFrom(random(), new String[] {"blue", "red", "yellow", "green"});
                final Query q = new TermQuery(new Term("color", value));
                TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(q, collector); // will use the cache
                final int totalHits1 = collector.getTotalHits();
                final int totalHits2 = searcher.search(q, 1).totalHits; // will not use the cache because of scores
                assertEquals(totalHits2, totalHits1);
              } finally {
                mgr.release(searcher);
              }
            } catch (Throwable t) {
              error.compareAndSet(null, t);
            }
          }
        }
      };
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    if (error.get() != null) {
      throw error.get();
    }
    queryCache.assertConsistent();
    mgr.close();
    w.close();
    dir.close();
    queryCache.assertConsistent();
  }

  public void testLRUEviction() throws Exception {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    StringField f = new StringField("color", "blue", Store.NO);
    doc.add(f);
    w.addDocument(doc);
    f.setStringValue("red");
    w.addDocument(doc);
    f.setStringValue("green");
    w.addDocument(doc);
    final DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    final LRUQueryCache queryCache = new LRUQueryCache(2, 100000, context -> true);

    final Query blue = new TermQuery(new Term("color", "blue"));
    final Query red = new TermQuery(new Term("color", "red"));
    final Query green = new TermQuery(new Term("color", "green"));

    assertEquals(Collections.emptyList(), queryCache.cachedQueries());

    searcher.setQueryCache(queryCache);
    // the filter is not cached on any segment: no changes
    searcher.setQueryCachingPolicy(NEVER_CACHE);
    searcher.search(new ConstantScoreQuery(green), 1);
    assertEquals(Collections.emptyList(), queryCache.cachedQueries());

    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);
    searcher.search(new ConstantScoreQuery(red), 1);
    assertEquals(Collections.singletonList(red), queryCache.cachedQueries());

    searcher.search(new ConstantScoreQuery(green), 1);
    assertEquals(Arrays.asList(red, green), queryCache.cachedQueries());

    searcher.search(new ConstantScoreQuery(red), 1);
    assertEquals(Arrays.asList(green, red), queryCache.cachedQueries());

    searcher.search(new ConstantScoreQuery(blue), 1);
    assertEquals(Arrays.asList(red, blue), queryCache.cachedQueries());

    searcher.search(new ConstantScoreQuery(blue), 1);
    assertEquals(Arrays.asList(red, blue), queryCache.cachedQueries());

    searcher.search(new ConstantScoreQuery(green), 1);
    assertEquals(Arrays.asList(blue, green), queryCache.cachedQueries());

    searcher.setQueryCachingPolicy(NEVER_CACHE);
    searcher.search(new ConstantScoreQuery(red), 1);
    assertEquals(Arrays.asList(blue, green), queryCache.cachedQueries());

    reader.close();
    w.close();
    dir.close();
  }

  public void testClearFilter() throws IOException {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    final int numDocs = atLeast(10);
    for (int i = 0; i < numDocs; ++i) {
      f.setStringValue(random().nextBoolean() ? "red" : "blue");
      w.addDocument(doc);
    }
    final DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = newSearcher(reader);

    final Query query1 = new TermQuery(new Term("color", "blue"));
    // different instance yet equal
    final Query query2 = new TermQuery(new Term("color", "blue"));

    final LRUQueryCache queryCache = new LRUQueryCache(Integer.MAX_VALUE, Long.MAX_VALUE, context -> true);
    searcher.setQueryCache(queryCache);
    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);

    searcher.search(new BoostQuery(new ConstantScoreQuery(query1), random().nextFloat()), 1);
    assertEquals(1, queryCache.cachedQueries().size());

    queryCache.clearQuery(query2);

    assertTrue(queryCache.cachedQueries().isEmpty());
    queryCache.assertConsistent();

    reader.close();
    w.close();
    dir.close();
  }

  // This test makes sure that by making the same assumptions as LRUQueryCache, RAMUsageTester
  // computes the same memory usage.
  public void testRamBytesUsedAgreesWithRamUsageTester() throws IOException {
    final LRUQueryCache queryCache = new LRUQueryCache(1 + random().nextInt(5), 1 + random().nextInt(10000), context -> random().nextBoolean());
    // an accumulator that only sums up memory usage of referenced filters and doc id sets
    final RamUsageTester.Accumulator acc = new RamUsageTester.Accumulator() {
      @Override
      public long accumulateObject(Object o, long shallowSize, Map<Field,Object> fieldValues, Collection<Object> queue) {
        if (o instanceof DocIdSet) {
          return ((DocIdSet) o).ramBytesUsed();
        }
        if (o instanceof Query) {
          return queryCache.ramBytesUsed((Query) o);
        }
        if (o instanceof IndexReader || o.getClass().getSimpleName().equals("SegmentCoreReaders")) {
          // do not take readers or core cache keys into account
          return 0;
        }
        if (o instanceof Map) {
          Map<?,?> map = (Map<?,?>) o;
          queue.addAll(map.keySet());
          queue.addAll(map.values());
          final long sizePerEntry = o instanceof LinkedHashMap
              ? LRUQueryCache.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY
              : LRUQueryCache.HASHTABLE_RAM_BYTES_PER_ENTRY;
          return sizePerEntry * map.size();
        }
        // follow links to other objects, but ignore their memory usage
        super.accumulateObject(o, shallowSize, fieldValues, queue);
        return  0;
      }
      @Override
      public long accumulateArray(Object array, long shallowSize, List<Object> values, Collection<Object> queue) {
        // follow links to other objects, but ignore their memory usage
        super.accumulateArray(array, shallowSize, values, queue);
        return 0;
      }
    };

    Directory dir = newDirectory();
    // serial merges so that segments do not get closed while we are measuring ram usage
    // with RamUsageTester
    IndexWriterConfig iwc = newIndexWriterConfig().setMergeScheduler(new SerialMergeScheduler());
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    final List<String> colors = Arrays.asList("blue", "red", "green", "yellow");

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    final int iters = atLeast(5);
    for (int iter = 0; iter < iters; ++iter) {
      final int numDocs = atLeast(10);
      for (int i = 0; i < numDocs; ++i) {
        f.setStringValue(RandomPicks.randomFrom(random(), colors));
        w.addDocument(doc);
      }
      try (final DirectoryReader reader = w.getReader()) {
        final IndexSearcher searcher = newSearcher(reader);
        searcher.setQueryCache(queryCache);
        searcher.setQueryCachingPolicy(MAYBE_CACHE_POLICY);
        for (int i = 0; i < 3; ++i) {
          final Query query = new TermQuery(new Term("color", RandomPicks.randomFrom(random(), colors)));
          searcher.search(new ConstantScoreQuery(query), 1);
        }
      }
      queryCache.assertConsistent();
      assertEquals(RamUsageTester.sizeOf(queryCache, acc), queryCache.ramBytesUsed());
    }

    w.close();
    dir.close();
  }

  /** A query that doesn't match anything */
  private static class DummyQuery extends Query {

    private static int COUNTER = 0;
    private final int id;

    DummyQuery() {
      id = COUNTER++;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      return new ConstantScoreWeight(this) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          return null;
        }
      };
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DummyQuery == false) {
        return false;
      }
      return id == ((DummyQuery) obj).id;
    }

    @Override
    public int hashCode() {
      return id;
    }

    @Override
    public String toString(String field) {
      return "DummyQuery";
    }

  }

  // Test what happens when the cache contains only filters and doc id sets
  // that require very little memory. In that case most of the memory is taken
  // by the cache itself, not cache entries, and we want to make sure that
  // memory usage is not grossly underestimated.
  public void testRamBytesUsedConstantEntryOverhead() throws IOException {
    final LRUQueryCache queryCache = new LRUQueryCache(1000000, 10000000, context -> true);

    final RamUsageTester.Accumulator acc = new RamUsageTester.Accumulator() {
      @Override
      public long accumulateObject(Object o, long shallowSize, Map<Field,Object> fieldValues, Collection<Object> queue) {
        if (o instanceof DocIdSet) {
          return ((DocIdSet) o).ramBytesUsed();
        }
        if (o instanceof Query) {
          return queryCache.ramBytesUsed((Query) o);
        }
        if (o.getClass().getSimpleName().equals("SegmentCoreReaders")) {
          // do not follow references to core cache keys
          return 0;
        }
        return super.accumulateObject(o, shallowSize, fieldValues, queue);
      }
    };

    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      w.addDocument(doc);
    }
    final DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(queryCache);
    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);

    final int numQueries = atLeast(1000);
    for (int i = 0; i < numQueries; ++i) {
      final Query query = new DummyQuery();
      searcher.search(new ConstantScoreQuery(query), 1);
    }
    assertTrue(queryCache.getCacheCount() > 0);

    final long actualRamBytesUsed = RamUsageTester.sizeOf(queryCache, acc);
    final long expectedRamBytesUsed = queryCache.ramBytesUsed();
    // error < 30%
    assertEquals(actualRamBytesUsed, expectedRamBytesUsed, 30 * actualRamBytesUsed / 100);

    reader.close();
    w.close();
    dir.close();
  }

  public void testOnUse() throws IOException {
    final LRUQueryCache queryCache = new LRUQueryCache(1 + random().nextInt(5), 1 + random().nextInt(1000), context -> random().nextBoolean());

    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    final int numDocs = atLeast(10);
    for (int i = 0; i < numDocs; ++i) {
      f.setStringValue(RandomPicks.randomFrom(random(), Arrays.asList("red", "blue", "green", "yellow")));
      w.addDocument(doc);
      if (random().nextBoolean()) {
        w.getReader().close();
      }
    }
    final DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);

    final Map<Query, Integer> actualCounts = new HashMap<>();
    final Map<Query, Integer> expectedCounts = new HashMap<>();

    final QueryCachingPolicy countingPolicy = new QueryCachingPolicy() {

      @Override
      public boolean shouldCache(Query query, LeafReaderContext context) throws IOException {
        return random().nextBoolean();
      }

      @Override
      public void onUse(Query query) {
        expectedCounts.put(query, 1 + expectedCounts.getOrDefault(query, 0));
      }
    };

    Query[] queries = new Query[10 + random().nextInt(10)];
    for (int i = 0; i < queries.length; ++i) {
      queries[i] = new BoostQuery(new TermQuery(new Term("color", RandomPicks.randomFrom(random(), Arrays.asList("red", "blue", "green", "yellow")))), random().nextFloat());
    }

    searcher.setQueryCache(queryCache);
    searcher.setQueryCachingPolicy(countingPolicy);
    for (int i = 0; i < 20; ++i) {
      final int idx = random().nextInt(queries.length);
      searcher.search(new ConstantScoreQuery(queries[idx]), 1);
      Query cacheKey = queries[idx];
      while (cacheKey instanceof BoostQuery) {
        cacheKey = ((BoostQuery) cacheKey).getQuery();
      }
      actualCounts.put(cacheKey, 1 + actualCounts.getOrDefault(cacheKey, 0));
    }

    assertEquals(actualCounts, expectedCounts);

    reader.close();
    w.close();
    dir.close();
  }

  public void testStats() throws IOException {
    final LRUQueryCache queryCache = new LRUQueryCache(1, 10000000, context -> true);

    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final List<String> colors = Arrays.asList("blue", "red", "green", "yellow");

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    for (int i = 0; i < 10; ++i) {
      f.setStringValue(RandomPicks.randomFrom(random(), colors));
      w.addDocument(doc);
      if (random().nextBoolean()) {
        w.getReader().close();
      }
    }

    final DirectoryReader reader = w.getReader();
    final int segmentCount = reader.leaves().size();
    final IndexSearcher searcher = new IndexSearcher(reader);
    final Query query = new TermQuery(new Term("color", "red"));
    final Query query2 = new TermQuery(new Term("color", "blue"));

    searcher.setQueryCache(queryCache);
    // first pass, lookups without caching that all miss
    searcher.setQueryCachingPolicy(NEVER_CACHE);
    for (int i = 0; i < 10; ++i) {
      searcher.search(new ConstantScoreQuery(query), 1);
    }
    assertEquals(10 * segmentCount, queryCache.getTotalCount());
    assertEquals(0, queryCache.getHitCount());
    assertEquals(10 * segmentCount, queryCache.getMissCount());
    assertEquals(0, queryCache.getCacheCount());
    assertEquals(0, queryCache.getEvictionCount());
    assertEquals(0, queryCache.getCacheSize());

    // second pass, lookups + caching, only the first one is a miss
    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);
    for (int i = 0; i < 10; ++i) {
      searcher.search(new ConstantScoreQuery(query), 1);
    }
    assertEquals(20 * segmentCount, queryCache.getTotalCount());
    assertEquals(9 * segmentCount, queryCache.getHitCount());
    assertEquals(11 * segmentCount, queryCache.getMissCount());
    assertEquals(1 * segmentCount, queryCache.getCacheCount());
    assertEquals(0, queryCache.getEvictionCount());
    assertEquals(1 * segmentCount, queryCache.getCacheSize());

    // third pass lookups without caching, we only have hits
    searcher.setQueryCachingPolicy(NEVER_CACHE);
    for (int i = 0; i < 10; ++i) {
      searcher.search(new ConstantScoreQuery(query), 1);
    }
    assertEquals(30 * segmentCount, queryCache.getTotalCount());
    assertEquals(19 * segmentCount, queryCache.getHitCount());
    assertEquals(11 * segmentCount, queryCache.getMissCount());
    assertEquals(1 * segmentCount, queryCache.getCacheCount());
    assertEquals(0, queryCache.getEvictionCount());
    assertEquals(1 * segmentCount, queryCache.getCacheSize());

    // fourth pass with a different filter which will trigger evictions since the size is 1
    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);
    for (int i = 0; i < 10; ++i) {
      searcher.search(new ConstantScoreQuery(query2), 1);
    }
    assertEquals(40 * segmentCount, queryCache.getTotalCount());
    assertEquals(28 * segmentCount, queryCache.getHitCount());
    assertEquals(12 * segmentCount, queryCache.getMissCount());
    assertEquals(2 * segmentCount, queryCache.getCacheCount());
    assertEquals(1 * segmentCount, queryCache.getEvictionCount());
    assertEquals(1 * segmentCount, queryCache.getCacheSize());

    // now close, causing evictions due to the closing of segment cores
    reader.close();
    w.close();
    assertEquals(40 * segmentCount, queryCache.getTotalCount());
    assertEquals(28 * segmentCount, queryCache.getHitCount());
    assertEquals(12 * segmentCount, queryCache.getMissCount());
    assertEquals(2 * segmentCount, queryCache.getCacheCount());
    assertEquals(2 * segmentCount, queryCache.getEvictionCount());
    assertEquals(0, queryCache.getCacheSize());

    dir.close();
  }

  public void testFineGrainedStats() throws IOException {
    Directory dir1 = newDirectory();
    final RandomIndexWriter w1 = new RandomIndexWriter(random(), dir1);
    Directory dir2 = newDirectory();
    final RandomIndexWriter w2 = new RandomIndexWriter(random(), dir2);

    final List<String> colors = Arrays.asList("blue", "red", "green", "yellow");

    Document doc = new Document();
    StringField f = new StringField("color", "", Store.NO);
    doc.add(f);
    for (RandomIndexWriter w : Arrays.asList(w1, w2)) {
      for (int i = 0; i < 10; ++i) {
        f.setStringValue(RandomPicks.randomFrom(random(), colors));
        w.addDocument(doc);
        if (random().nextBoolean()) {
          w.getReader().close();
        }
      }
    }

    final DirectoryReader reader1 = w1.getReader();
    final int segmentCount1 = reader1.leaves().size();
    final IndexSearcher searcher1 = new IndexSearcher(reader1);

    final DirectoryReader reader2 = w2.getReader();
    final int segmentCount2 = reader2.leaves().size();
    final IndexSearcher searcher2 = new IndexSearcher(reader2);

    final Map<Object, Integer> indexId = new HashMap<>();
    for (LeafReaderContext ctx : reader1.leaves()) {
      indexId.put(ctx.reader().getCoreCacheKey(), 1);
    }
    for (LeafReaderContext ctx : reader2.leaves()) {
      indexId.put(ctx.reader().getCoreCacheKey(), 2);
    }

    final AtomicLong hitCount1 = new AtomicLong();
    final AtomicLong hitCount2 = new AtomicLong();
    final AtomicLong missCount1 = new AtomicLong();
    final AtomicLong missCount2 = new AtomicLong();

    final AtomicLong ramBytesUsage = new AtomicLong();
    final AtomicLong cacheSize = new AtomicLong();

    final LRUQueryCache queryCache = new LRUQueryCache(2, 10000000, context -> true) {
      @Override
      protected void onHit(Object readerCoreKey, Query query) {
        super.onHit(readerCoreKey, query);
        switch(indexId.get(readerCoreKey).intValue()) {
          case 1:
            hitCount1.incrementAndGet();
            break;
          case 2:
            hitCount2.incrementAndGet();
            break;
          default:
            throw new AssertionError();
        }
      }

      @Override
      protected void onMiss(Object readerCoreKey, Query query) {
        super.onMiss(readerCoreKey, query);
        switch(indexId.get(readerCoreKey).intValue()) {
          case 1:
            missCount1.incrementAndGet();
            break;
          case 2:
            missCount2.incrementAndGet();
            break;
          default:
            throw new AssertionError();
        }
      }

      @Override
      protected void onQueryCache(Query query, long ramBytesUsed) {
        super.onQueryCache(query, ramBytesUsed);
        ramBytesUsage.addAndGet(ramBytesUsed);
      }

      @Override
      protected void onQueryEviction(Query query, long ramBytesUsed) {
        super.onQueryEviction(query, ramBytesUsed);
        ramBytesUsage.addAndGet(-ramBytesUsed);
      }

      @Override
      protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
        super.onDocIdSetCache(readerCoreKey, ramBytesUsed);
        ramBytesUsage.addAndGet(ramBytesUsed);
        cacheSize.incrementAndGet();
      }

      @Override
      protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
        super.onDocIdSetEviction(readerCoreKey, numEntries, sumRamBytesUsed);
        ramBytesUsage.addAndGet(-sumRamBytesUsed);
        cacheSize.addAndGet(-numEntries);
      }

      @Override
      protected void onClear() {
        super.onClear();
        ramBytesUsage.set(0);
        cacheSize.set(0);
      }
    };

    final Query query = new TermQuery(new Term("color", "red"));
    final Query query2 = new TermQuery(new Term("color", "blue"));
    final Query query3 = new TermQuery(new Term("color", "green"));

    for (IndexSearcher searcher : Arrays.asList(searcher1, searcher2)) {
      searcher.setQueryCache(queryCache);
      searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);
    }

    // search on searcher1
    for (int i = 0; i < 10; ++i) {
      searcher1.search(new ConstantScoreQuery(query), 1);
    }
    assertEquals(9 * segmentCount1, hitCount1.longValue());
    assertEquals(0, hitCount2.longValue());
    assertEquals(segmentCount1, missCount1.longValue());
    assertEquals(0, missCount2.longValue());

    // then on searcher2
    for (int i = 0; i < 20; ++i) {
      searcher2.search(new ConstantScoreQuery(query2), 1);
    }
    assertEquals(9 * segmentCount1, hitCount1.longValue());
    assertEquals(19 * segmentCount2, hitCount2.longValue());
    assertEquals(segmentCount1, missCount1.longValue());
    assertEquals(segmentCount2, missCount2.longValue());

    // now on searcher1 again to trigger evictions
    for (int i = 0; i < 30; ++i) {
      searcher1.search(new ConstantScoreQuery(query3), 1);
    }
    assertEquals(segmentCount1, queryCache.getEvictionCount());
    assertEquals(38 * segmentCount1, hitCount1.longValue());
    assertEquals(19 * segmentCount2, hitCount2.longValue());
    assertEquals(2 * segmentCount1, missCount1.longValue());
    assertEquals(segmentCount2, missCount2.longValue());

    // check that the recomputed stats are the same as those reported by the cache
    assertEquals(queryCache.ramBytesUsed(), (segmentCount1 + segmentCount2) * LRUQueryCache.HASHTABLE_RAM_BYTES_PER_ENTRY + ramBytesUsage.longValue());
    assertEquals(queryCache.getCacheSize(), cacheSize.longValue());

    reader1.close();
    reader2.close();
    w1.close();
    w2.close();

    assertEquals(queryCache.ramBytesUsed(), ramBytesUsage.longValue());
    assertEquals(0, cacheSize.longValue());

    queryCache.clear();
    assertEquals(0, ramBytesUsage.longValue());
    assertEquals(0, cacheSize.longValue());

    dir1.close();
    dir2.close();
  }

  public void testUseRewrittenQueryAsCacheKey() throws IOException {
    final Query expectedCacheKey = new TermQuery(new Term("foo", "bar"));
    final BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new BoostQuery(expectedCacheKey, 42f), Occur.MUST);

    final LRUQueryCache queryCache = new LRUQueryCache(1000000, 10000000, context -> random().nextBoolean());
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Store.YES));
    w.addDocument(doc);
    w.commit();
    final IndexReader reader = w.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    w.close();

    final QueryCachingPolicy policy = new QueryCachingPolicy() {

      @Override
      public boolean shouldCache(Query query, LeafReaderContext context) throws IOException {
        assertEquals(expectedCacheKey, query);
        return true;
      }

      @Override
      public void onUse(Query query) {
        assertEquals(expectedCacheKey, query);
      }
    };

    searcher.setQueryCache(queryCache);
    searcher.setQueryCachingPolicy(policy);
    searcher.search(query.build(), new TotalHitCountCollector());

    reader.close();
    dir.close();
  }

  public void testBooleanQueryCachesSubClauses() throws IOException {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Store.YES));
    doc.add(new StringField("foo", "quux", Store.YES));
    w.addDocument(doc);
    w.commit();
    final IndexReader reader = w.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    w.close();

    final LRUQueryCache queryCache = new LRUQueryCache(1000000, 10000000, context -> true);
    searcher.setQueryCache(queryCache);
    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    TermQuery should = new TermQuery(new Term("foo", "baz"));
    TermQuery must = new TermQuery(new Term("foo", "bar"));
    TermQuery filter = new TermQuery(new Term("foo", "quux"));
    TermQuery mustNot = new TermQuery(new Term("foo", "foo"));
    bq.add(should, Occur.SHOULD);
    bq.add(must, Occur.MUST);
    bq.add(filter, Occur.FILTER);
    bq.add(mustNot, Occur.MUST_NOT);

    // same bq but with FILTER instead of MUST
    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(should, Occur.SHOULD);
    bq2.add(must, Occur.FILTER);
    bq2.add(filter, Occur.FILTER);
    bq2.add(mustNot, Occur.MUST_NOT);

    assertEquals(Collections.emptySet(), new HashSet<>(queryCache.cachedQueries()));
    searcher.search(bq.build(), 1);
    assertEquals(new HashSet<>(Arrays.asList(filter, mustNot)), new HashSet<>(queryCache.cachedQueries()));

    queryCache.clear();
    assertEquals(Collections.emptySet(), new HashSet<>(queryCache.cachedQueries()));
    searcher.search(new ConstantScoreQuery(bq.build()), 1);
    assertEquals(new HashSet<>(Arrays.asList(bq2.build(), should, must, filter, mustNot)), new HashSet<>(queryCache.cachedQueries()));

    reader.close();
    dir.close();
  }

  private static Term randomTerm() {
    final String term = RandomPicks.randomFrom(random(), Arrays.asList("foo", "bar", "baz"));
    return new Term("foo", term);
  }

  private static Query buildRandomQuery(int level) {
    if (level == 10) {
      // at most 10 levels
      return new MatchAllDocsQuery();
    }
    switch (random().nextInt(6)) {
      case 0:
        return new TermQuery(randomTerm());
      case 1:
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        final int numClauses = TestUtil.nextInt(random(), 1, 3);
        int numShould = 0;
        for (int i = 0; i < numClauses; ++i) {
          final Occur occur = RandomPicks.randomFrom(random(), Occur.values());
          bq.add(buildRandomQuery(level + 1), occur);
          if (occur == Occur.SHOULD) {
            numShould++;
          }
        }
        bq.setMinimumNumberShouldMatch(TestUtil.nextInt(random(), 0, numShould));
        return bq.build();
      case 2:
        Term t1 = randomTerm();
        Term t2 = randomTerm();
        PhraseQuery pq = new PhraseQuery(random().nextInt(2), t1.field(), t1.bytes(), t2.bytes());
        return pq;
      case 3:
        return new MatchAllDocsQuery();
      case 4:
        return new ConstantScoreQuery(buildRandomQuery(level + 1));
      case 5:
        List<Query> disjuncts = new ArrayList<>();
        final int numQueries = TestUtil.nextInt(random(), 1, 3);
        for (int i = 0; i < numQueries; ++i) {
          disjuncts.add(buildRandomQuery(level + 1));
        }
        return new DisjunctionMaxQuery(disjuncts, random().nextFloat());
      default:
        throw new AssertionError();
    }
  }

  public void testRandom() throws IOException {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    TextField f = new TextField("foo", "foo", Store.NO);
    doc.add(f);
    w.addDocument(doc);
    IndexReader reader = w.getReader();

    final int maxSize;
    final long maxRamBytesUsed;
    final int iters;

    if (TEST_NIGHTLY) {
      maxSize = TestUtil.nextInt(random(), 1, 10000);
      maxRamBytesUsed = TestUtil.nextLong(random(), 1, 5000000);
      iters = atLeast(20000);
    } else {
      maxSize = TestUtil.nextInt(random(), 1, 1000);
      maxRamBytesUsed = TestUtil.nextLong(random(), 1, 500000);
      iters = atLeast(2000);
    }

    final LRUQueryCache queryCache = new LRUQueryCache(maxSize, maxRamBytesUsed, context -> random().nextBoolean());
    IndexSearcher uncachedSearcher = null;
    IndexSearcher cachedSearcher = null;

    for (int i = 0; i < iters; ++i) {
      if (i == 0 || random().nextInt(100) == 1) {
        reader.close();
        f.setStringValue(RandomPicks.randomFrom(random(), Arrays.asList("foo", "bar", "bar baz")));
        w.addDocument(doc);
        if (random().nextBoolean()) {
          w.deleteDocuments(buildRandomQuery(0));
        }
        reader = w.getReader();
        uncachedSearcher = newSearcher(reader);
        uncachedSearcher.setQueryCache(null);
        cachedSearcher = newSearcher(reader);
        cachedSearcher.setQueryCache(queryCache);
        cachedSearcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);
      }
      final Query q = buildRandomQuery(0);
      assertEquals(uncachedSearcher.count(q), cachedSearcher.count(q));
      if (rarely()) {
        queryCache.assertConsistent();
      }
    }
    queryCache.assertConsistent();
    w.close();
    reader.close();
    dir.close();
    queryCache.assertConsistent();
  }

  private static class BadQuery extends Query {

    int[] i = new int[] {42}; // an array so that clone keeps the reference

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      return new ConstantScoreWeight(this) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          return null;
        }
      };
    }

    @Override
    public String toString(String field) {
      return "BadQuery";
    }

    @Override
    public int hashCode() {
      return super.hashCode() ^ i[0];
    }

  }

  public void testDetectMutatedQueries() throws IOException {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    IndexReader reader = w.getReader();

    // size of 1 so that 2nd query evicts from the cache
    final LRUQueryCache queryCache = new LRUQueryCache(1, 10000, context -> true);
    final IndexSearcher searcher = newSearcher(reader);
    searcher.setQueryCache(queryCache);
    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);

    BadQuery query = new BadQuery();
    searcher.count(query);
    query.i[0] += 1; // change the hashCode!

    try {
      // trigger an eviction
      searcher.search(new MatchAllDocsQuery(), new TotalHitCountCollector());
      fail();
    } catch (ConcurrentModificationException e) {
      // expected
    } catch (RuntimeException e) {
      // expected: wrapped when executor is in use
      Throwable cause = e.getCause();
      assertTrue(cause instanceof ExecutionException);
      assertTrue(cause.getCause() instanceof ConcurrentModificationException);
    }

    IOUtils.close(w, reader, dir);
  }

  public void testRefuseToCacheTooLargeEntries() throws IOException {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 100; ++i) {
      w.addDocument(new Document());
    }
    IndexReader reader = w.getReader();

    // size of 1 byte
    final LRUQueryCache queryCache = new LRUQueryCache(1, 1, context -> random().nextBoolean());
    final IndexSearcher searcher = newSearcher(reader);
    searcher.setQueryCache(queryCache);
    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);

    searcher.count(new MatchAllDocsQuery());
    assertEquals(0, queryCache.getCacheCount());
    assertEquals(0, queryCache.getEvictionCount());

    reader.close();
    w.close();
    dir.close();
  }

  /**
   * Tests CachingWrapperWeight.scorer() propagation of {@link QueryCachingPolicy#onUse(Query)} when the first segment
   * is skipped.
   *
   * #f:foo #f:bar causes all frequencies to increment
   * #f:bar #f:foo does not increment the frequency for f:foo
   */
  public void testOnUseWithRandomFirstSegmentSkipping() throws IOException {
    try (final Directory directory = newDirectory()) {
      try (final RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        Document doc = new Document();
        doc.add(new StringField("f", "bar", Store.NO));
        indexWriter.addDocument(doc);
        if (random().nextBoolean()) {
          indexWriter.getReader().close();
        }
        doc = new Document();
        doc.add(new StringField("f", "foo", Store.NO));
        doc.add(new StringField("f", "bar", Store.NO));
        indexWriter.addDocument(doc);
        indexWriter.commit();
      }
      try (final IndexReader indexReader = DirectoryReader.open(directory)) {
        final FrequencyCountingPolicy policy = new FrequencyCountingPolicy();
        final IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setQueryCache(new LRUQueryCache(100, 10240, context -> random().nextBoolean()));
        indexSearcher.setQueryCachingPolicy(policy);
        final Query foo = new TermQuery(new Term("f", "foo"));
        final Query bar = new TermQuery(new Term("f", "bar"));
        final BooleanQuery.Builder query = new BooleanQuery.Builder();
        if (random().nextBoolean()) {
          query.add(foo, Occur.FILTER);
          query.add(bar, Occur.FILTER);
        } else {
          query.add(bar, Occur.FILTER);
          query.add(foo, Occur.FILTER);
        }
        indexSearcher.count(query.build());
        assertEquals(1, policy.frequency(query.build()));
        assertEquals(1, policy.frequency(foo));
        assertEquals(1, policy.frequency(bar));
      }
    }
  }

  private static class FrequencyCountingPolicy implements QueryCachingPolicy {
    private final Map<Query,AtomicInteger> counts = new HashMap<>();

    public int frequency(final Query query) {
      AtomicInteger count;
      synchronized (counts) {
        count = counts.get(query);
      }
      return count != null ? count.get() : 0;
    }

    @Override
    public void onUse(final Query query) {
      AtomicInteger count;
      synchronized (counts) {
        count = counts.get(query);
        if (count == null) {
          count = new AtomicInteger();
          counts.put(query, count);
        }
      }
      count.incrementAndGet();
    }

    @Override
    public boolean shouldCache(Query query, LeafReaderContext context) throws IOException {
      return true;
    }
  }

  private static class WeightWrapper extends Weight {

    private final Weight in;
    private final AtomicBoolean scorerCalled;
    private final AtomicBoolean bulkScorerCalled;

    protected WeightWrapper(Weight in, AtomicBoolean scorerCalled, AtomicBoolean bulkScorerCalled) {
      super(in.getQuery());
      this.in = in;
      this.scorerCalled = scorerCalled;
      this.bulkScorerCalled = bulkScorerCalled;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      in.extractTerms(terms);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      return in.explain(context, doc);
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return in.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float boost) {
      in.normalize(norm, boost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      scorerCalled.set(true);
      return in.scorer(context);
    }

    @Override
    public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
      bulkScorerCalled.set(true);
      return in.bulkScorer(context);
    }
  }

  public void testPropagateBulkScorer() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
    AtomicBoolean scorerCalled = new AtomicBoolean();
    AtomicBoolean bulkScorerCalled = new AtomicBoolean();
    LRUQueryCache cache = new LRUQueryCache(1, Long.MAX_VALUE, context -> true);

    // test that the bulk scorer is propagated when a scorer should not be cached
    Weight weight = searcher.createNormalizedWeight(new MatchAllDocsQuery(), false);
    weight = new WeightWrapper(weight, scorerCalled, bulkScorerCalled);
    weight = cache.doCache(weight, NEVER_CACHE);
    weight.bulkScorer(leaf);
    assertEquals(true, bulkScorerCalled.get());
    assertEquals(false, scorerCalled.get());
    assertEquals(0, cache.getCacheCount());

    // test that the doc id set is computed using the bulk scorer
    bulkScorerCalled.set(false);
    weight = searcher.createNormalizedWeight(new MatchAllDocsQuery(), false);
    weight = new WeightWrapper(weight, scorerCalled, bulkScorerCalled);
    weight = cache.doCache(weight, QueryCachingPolicy.ALWAYS_CACHE);
    weight.scorer(leaf);
    assertEquals(true, bulkScorerCalled.get());
    assertEquals(false, scorerCalled.get());
    assertEquals(1, cache.getCacheCount());

    searcher.getIndexReader().close();
    dir.close();
  }

  public void testEvictEmptySegmentCache() throws IOException {
    Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    final DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    final LRUQueryCache queryCache = new LRUQueryCache(2, 100000, context -> true) {
      @Override
      protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
        super.onDocIdSetEviction(readerCoreKey, numEntries, sumRamBytesUsed);
        assertTrue(numEntries > 0);
      }
    };

    searcher.setQueryCache(queryCache);
    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);

    Query query = new DummyQuery();
    searcher.count(query);
    assertEquals(Collections.singletonList(query), queryCache.cachedQueries());
    queryCache.clearQuery(query);

    reader.close(); // make sure this does not trigger eviction of segment caches with no entries
    w.close();
    dir.close();
  }

  public void testMinSegmentSizePredicate() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    w.addDocument(new Document());
    DirectoryReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);

    LRUQueryCache cache = new LRUQueryCache(2, 10000, new LRUQueryCache.MinSegmentSizePredicate(2, 0f));
    searcher.setQueryCache(cache);
    searcher.count(new DummyQuery());
    assertEquals(0, cache.getCacheCount());

    cache = new LRUQueryCache(2, 10000, new LRUQueryCache.MinSegmentSizePredicate(1, 0f));
    searcher.setQueryCache(cache);
    searcher.count(new DummyQuery());
    assertEquals(1, cache.getCacheCount());

    cache = new LRUQueryCache(2, 10000, new LRUQueryCache.MinSegmentSizePredicate(0, .6f));
    searcher.setQueryCache(cache);
    searcher.count(new DummyQuery());
    assertEquals(1, cache.getCacheCount());

    w.addDocument(new Document());
    reader.close();
    reader = w.getReader();
    searcher = newSearcher(reader);
    searcher.setQueryCachingPolicy(QueryCachingPolicy.ALWAYS_CACHE);
    cache = new LRUQueryCache(2, 10000, new LRUQueryCache.MinSegmentSizePredicate(0, .6f));
    searcher.setQueryCache(cache);
    searcher.count(new DummyQuery());
    assertEquals(0, cache.getCacheCount());

    reader.close();
    w.close();
    dir.close();
  }
}
