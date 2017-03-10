/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine.internal;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.KeepOnlyLastDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommitExistsMatcher;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.indexing.slowlog.ShardSlowLogIndexingService;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.merge.policy.LogByteSizeMergePolicyProvider;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.ConcurrentMergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.SerialMergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.distributor.LeastUsedDistributor;
import org.elasticsearch.index.store.ram.RamDirectoryService;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogSizeMatcher;
import org.elasticsearch.index.translog.fs.FsTranslog;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class InternalEngineTests extends ElasticsearchTestCase {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    protected ThreadPool threadPool;

    private Store store;
    private Store storeReplica;

    protected Engine engine;
    protected Engine replicaEngine;

    private IndexSettingsService engineSettingsService;

    private IndexSettingsService replicaSettingsService;

    private Settings defaultSettings;


    @Before
    public void setUp() throws Exception {
        super.setUp();
        defaultSettings = ImmutableSettings.builder()
                .put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, getRandom().nextBoolean())
                .build(); // TODO randomize more settings
        threadPool = new ThreadPool();
        store = createStore();
        store.deleteContent();
        storeReplica = createStoreReplica();
        storeReplica.deleteContent();
        engineSettingsService = new IndexSettingsService(shardId.index(), EMPTY_SETTINGS);
        engine = createEngine(engineSettingsService, store, createTranslog());
        engine.start();
        replicaSettingsService = new IndexSettingsService(shardId.index(), EMPTY_SETTINGS);
        replicaEngine = createEngine(replicaSettingsService, storeReplica, createTranslogReplica());
        replicaEngine.start();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        replicaEngine.close();
        storeReplica.close();

        engine.close();
        store.close();

        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    private Document testDocumentWithTextField() {
        Document document = testDocument();
        document.add(new TextField("value", "test", Field.Store.YES));
        return document;
    }

    private Document testDocument() {
        return new Document();
    }


    private ParsedDocument testParsedDocument(String uid, String id, String type, String routing, long timestamp, long ttl, Document document, Analyzer analyzer, BytesReference source, boolean mappingsModified) {
        Field uidField = new Field("_uid", uid, UidFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        document.add(uidField);
        document.add(versionField);
        return new ParsedDocument(uidField, versionField, id, type, routing, timestamp, ttl, Arrays.asList(document), analyzer, source, mappingsModified);
    }

    protected Store createStore() throws IOException {
        DirectoryService directoryService = new RamDirectoryService(shardId, EMPTY_SETTINGS);
        return new Store(shardId, EMPTY_SETTINGS, null, null, directoryService, new LeastUsedDistributor(directoryService));
    }

    protected Store createStoreReplica() throws IOException {
        DirectoryService directoryService = new RamDirectoryService(shardId, EMPTY_SETTINGS);
        return new Store(shardId, EMPTY_SETTINGS, null, null, directoryService, new LeastUsedDistributor(directoryService));
    }

    protected Translog createTranslog() {
        return new FsTranslog(shardId, EMPTY_SETTINGS, new File("work/fs-translog/primary"));
    }

    protected Translog createTranslogReplica() {
        return new FsTranslog(shardId, EMPTY_SETTINGS, new File("work/fs-translog/replica"));
    }

    protected IndexDeletionPolicy createIndexDeletionPolicy() {
        return new KeepOnlyLastDeletionPolicy(shardId, EMPTY_SETTINGS);
    }

    protected SnapshotDeletionPolicy createSnapshotDeletionPolicy() {
        return new SnapshotDeletionPolicy(createIndexDeletionPolicy());
    }

    protected MergePolicyProvider<?> createMergePolicy() {
        return new LogByteSizeMergePolicyProvider(store, new IndexSettingsService(new Index("test"), EMPTY_SETTINGS));
    }

    protected MergeSchedulerProvider createMergeScheduler() {
        return new SerialMergeSchedulerProvider(shardId, EMPTY_SETTINGS, threadPool);
    }

    protected Engine createEngine(IndexSettingsService indexSettingsService, Store store, Translog translog) {
        return createEngine(indexSettingsService, store, translog, createMergeScheduler());
    }

    protected Engine createEngine(IndexSettingsService indexSettingsService, Store store, Translog translog, MergeSchedulerProvider mergeSchedulerProvider) {
        return new InternalEngine(shardId, defaultSettings, threadPool, indexSettingsService, new ShardIndexingService(shardId, EMPTY_SETTINGS, new ShardSlowLogIndexingService(shardId, EMPTY_SETTINGS, indexSettingsService)), null, store, createSnapshotDeletionPolicy(), translog, createMergePolicy(), mergeSchedulerProvider,
                new AnalysisService(shardId.index()), new SimilarityService(shardId.index()), new CodecService(shardId.index()));
    }

    protected static final BytesReference B_1 = new BytesArray(new byte[]{1});
    protected static final BytesReference B_2 = new BytesArray(new byte[]{2});
    protected static final BytesReference B_3 = new BytesArray(new byte[]{3});

    @Test
    public void testSegments() throws Exception {
        List<Segment> segments = engine.segments();
        assertThat(segments.isEmpty(), equalTo(true));
        assertThat(engine.segmentsStats().getCount(), equalTo(0l));
        final boolean defaultCompound = defaultSettings.getAsBoolean(InternalEngine.INDEX_COMPOUND_ON_FLUSH, true);

        // create a doc and refresh
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_2, false);
        engine.create(new Engine.Create(null, newUid("2"), doc2));
        engine.refresh(new Engine.Refresh("test").force(false));

        segments = engine.segments();
        assertThat(segments.size(), equalTo(1));
        assertThat(engine.segmentsStats().getCount(), equalTo(1l));
        assertThat(segments.get(0).isCommitted(), equalTo(false));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(2));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));

        engine.flush(new Engine.Flush());

        segments = engine.segments();
        assertThat(segments.size(), equalTo(1));
        assertThat(engine.segmentsStats().getCount(), equalTo(1l));
        assertThat(segments.get(0).isCommitted(), equalTo(true));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(2));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));

        engineSettingsService.refreshSettings(ImmutableSettings.builder().put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, false).build());

        ParsedDocument doc3 = testParsedDocument("3", "3", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_3, false);
        engine.create(new Engine.Create(null, newUid("3"), doc3));
        engine.refresh(new Engine.Refresh("test").force(false));

        segments = engine.segments();
        assertThat(segments.size(), equalTo(2));
        assertThat(engine.segmentsStats().getCount(), equalTo(2l));
        assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
        assertThat(segments.get(0).isCommitted(), equalTo(true));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(2));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));


        assertThat(segments.get(1).isCommitted(), equalTo(false));
        assertThat(segments.get(1).isSearch(), equalTo(true));
        assertThat(segments.get(1).getNumDocs(), equalTo(1));
        assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(1).isCompound(), equalTo(false));


        engine.delete(new Engine.Delete("test", "1", newUid("1")));
        engine.refresh(new Engine.Refresh("test").force(false));

        segments = engine.segments();
        assertThat(segments.size(), equalTo(2));
        assertThat(engine.segmentsStats().getCount(), equalTo(2l));
        assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
        assertThat(segments.get(0).isCommitted(), equalTo(true));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(1));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(1));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));

        assertThat(segments.get(1).isCommitted(), equalTo(false));
        assertThat(segments.get(1).isSearch(), equalTo(true));
        assertThat(segments.get(1).getNumDocs(), equalTo(1));
        assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(1).isCompound(), equalTo(false));

        engineSettingsService.refreshSettings(ImmutableSettings.builder().put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, true).build());
        ParsedDocument doc4 = testParsedDocument("4", "4", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_3, false);
        engine.create(new Engine.Create(null, newUid("4"), doc4));
        engine.refresh(new Engine.Refresh("test").force(false));

        segments = engine.segments();
        assertThat(segments.size(), equalTo(3));
        assertThat(engine.segmentsStats().getCount(), equalTo(3l));
        assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
        assertThat(segments.get(0).isCommitted(), equalTo(true));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(1));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(1));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));

        assertThat(segments.get(1).isCommitted(), equalTo(false));
        assertThat(segments.get(1).isSearch(), equalTo(true));
        assertThat(segments.get(1).getNumDocs(), equalTo(1));
        assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(1).isCompound(), equalTo(false));

        assertThat(segments.get(2).isCommitted(), equalTo(false));
        assertThat(segments.get(2).isSearch(), equalTo(true));
        assertThat(segments.get(2).getNumDocs(), equalTo(1));
        assertThat(segments.get(2).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(2).isCompound(), equalTo(true));
    }

    @Test
    public void testSegmentsWithMergeFlag() throws Exception {
        ConcurrentMergeSchedulerProvider mergeSchedulerProvider = new ConcurrentMergeSchedulerProvider(shardId, EMPTY_SETTINGS, threadPool);
        final AtomicReference<CountDownLatch> waitTillMerge = new AtomicReference<CountDownLatch>();
        final AtomicReference<CountDownLatch> waitForMerge = new AtomicReference<CountDownLatch>();
        mergeSchedulerProvider.addListener(new MergeSchedulerProvider.Listener() {
            @Override
            public void beforeMerge(OnGoingMerge merge) {
                try {
                    if (waitTillMerge.get() != null) {
                        waitTillMerge.get().countDown();
                    }
                    if (waitForMerge.get() != null) {
                        waitForMerge.get().await();
                    }
                } catch (InterruptedException e) {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }

            @Override
            public void afterMerge(OnGoingMerge merge) {
            }
        });

        Engine engine = createEngine(engineSettingsService, store, createTranslog(), mergeSchedulerProvider);
        engine.start();
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        engine.flush(new Engine.Flush());
        assertThat(engine.segments().size(), equalTo(1));
        index = new Engine.Index(null, newUid("2"), doc);
        engine.index(index);
        engine.flush(new Engine.Flush());
        assertThat(engine.segments().size(), equalTo(2));
        for (Segment segment : engine.segments()) {
            assertThat(segment.getMergeId(), nullValue());
        }
        index = new Engine.Index(null, newUid("3"), doc);
        engine.index(index);
        engine.flush(new Engine.Flush());
        assertThat(engine.segments().size(), equalTo(3));
        for (Segment segment : engine.segments()) {
            assertThat(segment.getMergeId(), nullValue());
        }

        waitTillMerge.set(new CountDownLatch(1));
        waitForMerge.set(new CountDownLatch(1));
        engine.optimize(new Engine.Optimize().maxNumSegments(1).waitForMerge(false));
        waitTillMerge.get().await();

        for (Segment segment : engine.segments()) {
            assertThat(segment.getMergeId(), notNullValue());
        }

        waitForMerge.get().countDown();

        index = new Engine.Index(null, newUid("4"), doc);
        engine.index(index);
        engine.flush(new Engine.Flush());

        // now, optimize and wait for merges, see that we have no merge flag
        engine.optimize(new Engine.Optimize().flush(true).maxNumSegments(1).waitForMerge(true));

        for (Segment segment : engine.segments()) {
            assertThat(segment.getMergeId(), nullValue());
        }

        engine.close();
    }

    @Test
    public void testSimpleOperations() throws Exception {
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        searchResult.release();

        // create a document
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, B_1.toBytes(), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, document, Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.release();

        // but, we can still get it (in realtime)
        Engine.GetResult getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.source().source.toBytesArray(), equalTo(B_1.toBytesArray()));
        assertThat(getResult.docIdAndVersion(), nullValue());
        getResult.release();

        // but, not there non realtime
        getResult = engine.get(new Engine.Get(false, newUid("1")));
        assertThat(getResult.exists(), equalTo(false));
        getResult.release();
        // refresh and it should be there
        engine.refresh(new Engine.Refresh("test").force(false));

        // now its there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.release();

        // also in non realtime
        getResult = engine.get(new Engine.Get(false, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // now do an update
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        document.add(new Field(SourceFieldMapper.NAME, B_2.toBytes(), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, Lucene.STANDARD_ANALYZER, B_2, false);
        engine.index(new Engine.Index(null, newUid("1"), doc));

        // its not updated yet...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.release();

        // but, we can still get it (in realtime)
        getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.source().source.toBytesArray(), equalTo(B_2.toBytesArray()));
        assertThat(getResult.docIdAndVersion(), nullValue());
        getResult.release();

        // refresh and it should be updated
        engine.refresh(new Engine.Refresh("test").force(false));

        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.release();

        // now delete
        engine.delete(new Engine.Delete("test", "1", newUid("1")));

        // its not deleted yet
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.release();

        // but, get should not see it (in realtime)
        getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(false));
        getResult.release();

        // refresh and it should be deleted
        engine.refresh(new Engine.Refresh("test").force(false));

        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.release();

        // add it back
        document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, B_1.toBytes(), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.release();

        // refresh and it should be there
        engine.refresh(new Engine.Refresh("test").force(false));

        // now its there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.release();

        // now flush
        engine.flush(new Engine.Flush());

        // and, verify get (in real time)
        getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.source(), nullValue());
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // make sure we can still work with the engine
        // now do an update
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, Lucene.STANDARD_ANALYZER, B_1, false);
        engine.index(new Engine.Index(null, newUid("1"), doc));

        // its not updated yet...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.release();

        // refresh and it should be updated
        engine.refresh(new Engine.Refresh("test").force(false));

        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.release();

        engine.close();
    }

    @Test
    public void testSearchResultRelease() throws Exception {
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        searchResult.release();

        // create a document
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.release();

        // refresh and it should be there
        engine.refresh(new Engine.Refresh("test").force(false));

        // now its there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        // don't release the search result yet...

        // delete, refresh and do a new search, it should not be there
        engine.delete(new Engine.Delete("test", "1", newUid("1")));
        engine.refresh(new Engine.Refresh("test").force(false));
        Engine.Searcher updateSearchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(updateSearchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        updateSearchResult.release();

        // the non release search result should not see the deleted yet...
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.release();
    }

    @Test
    public void testSimpleSnapshot() throws Exception {
        // create a document
        ParsedDocument doc1 = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc1));

        final ExecutorService executorService = Executors.newCachedThreadPool();

        engine.snapshot(new Engine.SnapshotHandler<Void>() {
            @Override
            public Void snapshot(final SnapshotIndexCommit snapshotIndexCommit1, final Translog.Snapshot translogSnapshot1) {
                MatcherAssert.assertThat(snapshotIndexCommit1, SnapshotIndexCommitExistsMatcher.snapshotIndexCommitExists());
                assertThat(translogSnapshot1.hasNext(), equalTo(true));
                Translog.Create create1 = (Translog.Create) translogSnapshot1.next();
                assertThat(create1.source().toBytesArray(), equalTo(B_1.toBytesArray()));
                assertThat(translogSnapshot1.hasNext(), equalTo(false));

                Future<Object> future = executorService.submit(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        engine.flush(new Engine.Flush());
                        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_2, false);
                        engine.create(new Engine.Create(null, newUid("2"), doc2));
                        engine.flush(new Engine.Flush());
                        ParsedDocument doc3 = testParsedDocument("3", "3", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_3, false);
                        engine.create(new Engine.Create(null, newUid("3"), doc3));
                        return null;
                    }
                });

                try {
                    future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                    assertThat(e.getMessage(), false, equalTo(true));
                }

                MatcherAssert.assertThat(snapshotIndexCommit1, SnapshotIndexCommitExistsMatcher.snapshotIndexCommitExists());

                engine.snapshot(new Engine.SnapshotHandler<Void>() {
                    @Override
                    public Void snapshot(SnapshotIndexCommit snapshotIndexCommit2, Translog.Snapshot translogSnapshot2) throws EngineException {
                        MatcherAssert.assertThat(snapshotIndexCommit1, SnapshotIndexCommitExistsMatcher.snapshotIndexCommitExists());
                        MatcherAssert.assertThat(snapshotIndexCommit2, SnapshotIndexCommitExistsMatcher.snapshotIndexCommitExists());
                        assertThat(snapshotIndexCommit2.getSegmentsFileName(), not(equalTo(snapshotIndexCommit1.getSegmentsFileName())));
                        assertThat(translogSnapshot2.hasNext(), equalTo(true));
                        Translog.Create create3 = (Translog.Create) translogSnapshot2.next();
                        assertThat(create3.source().toBytesArray(), equalTo(B_3.toBytesArray()));
                        assertThat(translogSnapshot2.hasNext(), equalTo(false));
                        return null;
                    }
                });
                return null;
            }
        });

        engine.close();
    }

    @Test
    public void testSimpleRecover() throws Exception {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc));
        engine.flush(new Engine.Flush());

        engine.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
                try {
                    engine.flush(new Engine.Flush());
                    assertThat("flush is not allowed in phase 3", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws EngineException {
                MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
                try {
                    engine.flush(new Engine.Flush());
                    assertThat("flush is not allowed in phase 3", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws EngineException {
                MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
                try {
                    // we can do this here since we are on the same thread
                    engine.flush(new Engine.Flush());
                    assertThat("flush is not allowed in phase 3", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }
        });

        engine.flush(new Engine.Flush());
        engine.close();
    }

    @Test
    public void testRecoverWithOperationsBetweenPhase1AndPhase2() throws Exception {
        ParsedDocument doc1 = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc1));
        engine.flush(new Engine.Flush());
        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_2, false);
        engine.create(new Engine.Create(null, newUid("2"), doc2));

        engine.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws EngineException {
                assertThat(snapshot.hasNext(), equalTo(true));
                Translog.Create create = (Translog.Create) snapshot.next();
                assertThat(create.source().toBytesArray(), equalTo(B_2));
                assertThat(snapshot.hasNext(), equalTo(false));
            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws EngineException {
                MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
            }
        });

        engine.flush(new Engine.Flush());
        engine.close();
    }

    @Test
    public void testRecoverWithOperationsBetweenPhase1AndPhase2AndPhase3() throws Exception {
        ParsedDocument doc1 = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc1));
        engine.flush(new Engine.Flush());
        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_2, false);
        engine.create(new Engine.Create(null, newUid("2"), doc2));

        engine.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws EngineException {
                assertThat(snapshot.hasNext(), equalTo(true));
                Translog.Create create = (Translog.Create) snapshot.next();
                assertThat(snapshot.hasNext(), equalTo(false));
                assertThat(create.source().toBytesArray(), equalTo(B_2));

                // add for phase3
                ParsedDocument doc3 = testParsedDocument("3", "3", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_3, false);
                engine.create(new Engine.Create(null, newUid("3"), doc3));
            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws EngineException {
                assertThat(snapshot.hasNext(), equalTo(true));
                Translog.Create create = (Translog.Create) snapshot.next();
                assertThat(snapshot.hasNext(), equalTo(false));
                assertThat(create.source().toBytesArray(), equalTo(B_3));
            }
        });

        engine.flush(new Engine.Flush());
        engine.close();
    }

    @Test
    public void testVersioningNewCreate() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc);
        engine.create(create);
        assertThat(create.version(), equalTo(1l));

        create = new Engine.Create(null, newUid("1"), doc).version(create.version())
                .versionType(create.versionType().versionTypeForReplicationAndRecovery()).origin(REPLICA);
        replicaEngine.create(create);
        assertThat(create.version(), equalTo(1l));
    }

    @Test
    public void testExternalVersioningNewCreate() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc).versionType(VersionType.EXTERNAL).version(12);
        engine.create(create);
        assertThat(create.version(), equalTo(12l));

        create = new Engine.Create(null, newUid("1"), doc).version(create.version())
                .versionType(create.versionType().versionTypeForReplicationAndRecovery()).origin(REPLICA);
        replicaEngine.create(create);
        assertThat(create.version(), equalTo(12l));
    }

    @Test
    public void testVersioningNewIndex() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc).version(index.version())
                .versionType(index.versionType().versionTypeForReplicationAndRecovery()).origin(REPLICA);
        replicaEngine.index(index);
        assertThat(index.version(), equalTo(1l));
    }

    @Test
    public void testExternalVersioningNewIndex() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc).versionType(VersionType.EXTERNAL).version(12);
        engine.index(index);
        assertThat(index.version(), equalTo(12l));

        index = new Engine.Index(null, newUid("1"), doc)
                .version(index.version()).versionType(index.versionType().versionTypeForReplicationAndRecovery()).origin(REPLICA);
        replicaEngine.index(index);
        assertThat(index.version(), equalTo(12l));
    }

    @Test
    public void testVersioningIndexConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        index = new Engine.Index(null, newUid("1"), doc).version(1l);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // future versions should not work as well
        index = new Engine.Index(null, newUid("1"), doc).version(3l);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testExternalVersioningIndexConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc).versionType(VersionType.EXTERNAL).version(12);
        engine.index(index);
        assertThat(index.version(), equalTo(12l));

        index = new Engine.Index(null, newUid("1"), doc).versionType(VersionType.EXTERNAL).version(14);
        engine.index(index);
        assertThat(index.version(), equalTo(14l));

        index = new Engine.Index(null, newUid("1"), doc).versionType(VersionType.EXTERNAL).version(13l);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningIndexConflictWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        engine.flush(new Engine.Flush());

        index = new Engine.Index(null, newUid("1"), doc).version(1l);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // future versions should not work as well
        index = new Engine.Index(null, newUid("1"), doc).version(3l);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testExternalVersioningIndexConflictWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc).versionType(VersionType.EXTERNAL).version(12);
        engine.index(index);
        assertThat(index.version(), equalTo(12l));

        index = new Engine.Index(null, newUid("1"), doc).versionType(VersionType.EXTERNAL).version(14);
        engine.index(index);
        assertThat(index.version(), equalTo(14l));

        engine.flush(new Engine.Flush());

        index = new Engine.Index(null, newUid("1"), doc).versionType(VersionType.EXTERNAL).version(13);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningDeleteConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        Engine.Delete delete = new Engine.Delete("test", "1", newUid("1")).version(1l);
        try {
            engine.delete(delete);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // future versions should not work as well
        delete = new Engine.Delete("test", "1", newUid("1")).version(3l);
        try {
            engine.delete(delete);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // now actually delete
        delete = new Engine.Delete("test", "1", newUid("1")).version(2l);
        engine.delete(delete);
        assertThat(delete.version(), equalTo(3l));

        // now check if we can index to a delete doc with version
        index = new Engine.Index(null, newUid("1"), doc).version(2l);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // we shouldn't be able to create as well
        Engine.Create create = new Engine.Create(null, newUid("1"), doc).version(2l);
        try {
            engine.create(create);
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningDeleteConflictWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        engine.flush(new Engine.Flush());

        Engine.Delete delete = new Engine.Delete("test", "1", newUid("1")).version(1l);
        try {
            engine.delete(delete);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // future versions should not work as well
        delete = new Engine.Delete("test", "1", newUid("1")).version(3l);
        try {
            engine.delete(delete);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        engine.flush(new Engine.Flush());

        // now actually delete
        delete = new Engine.Delete("test", "1", newUid("1")).version(2l);
        engine.delete(delete);
        assertThat(delete.version(), equalTo(3l));

        engine.flush(new Engine.Flush());

        // now check if we can index to a delete doc with version
        index = new Engine.Index(null, newUid("1"), doc).version(2l);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // we shouldn't be able to create as well
        Engine.Create create = new Engine.Create(null, newUid("1"), doc).version(2l);
        try {
            engine.create(create);
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningCreateExistsException() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc);
        engine.create(create);
        assertThat(create.version(), equalTo(1l));

        create = new Engine.Create(null, newUid("1"), doc);
        try {
            engine.create(create);
            fail();
        } catch (DocumentAlreadyExistsException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningCreateExistsExceptionWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc);
        engine.create(create);
        assertThat(create.version(), equalTo(1l));

        engine.flush(new Engine.Flush());

        create = new Engine.Create(null, newUid("1"), doc);
        try {
            engine.create(create);
            fail();
        } catch (DocumentAlreadyExistsException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningReplicaConflict1() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        // apply the second index to the replica, should work fine
        index = new Engine.Index(null, newUid("1"), doc).version(index.version())
                .versionType(VersionType.INTERNAL.versionTypeForReplicationAndRecovery()).origin(REPLICA);
        replicaEngine.index(index);
        assertThat(index.version(), equalTo(2l));

        // now, the old one should not work
        index = new Engine.Index(null, newUid("1"), doc).version(1l).versionType(VersionType.INTERNAL.versionTypeForReplicationAndRecovery()).origin(REPLICA);
        try {
            replicaEngine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // second version on replica should fail as well
        try {
            index = new Engine.Index(null, newUid("1"), doc).version(2l)
                    .versionType(VersionType.INTERNAL.versionTypeForReplicationAndRecovery()).origin(REPLICA);
            replicaEngine.index(index);
            assertThat(index.version(), equalTo(2l));
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningReplicaConflict2() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        // apply the first index to the replica, should work fine
        index = new Engine.Index(null, newUid("1"), doc).version(1l)
                .versionType(VersionType.INTERNAL.versionTypeForReplicationAndRecovery()).origin(REPLICA);
        replicaEngine.index(index);
        assertThat(index.version(), equalTo(1l));

        // index it again
        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        // now delete it
        Engine.Delete delete = new Engine.Delete("test", "1", newUid("1"));
        engine.delete(delete);
        assertThat(delete.version(), equalTo(3l));

        // apply the delete on the replica (skipping the second index)
        delete = new Engine.Delete("test", "1", newUid("1")).version(3l)
                .versionType(VersionType.INTERNAL.versionTypeForReplicationAndRecovery()).origin(REPLICA);
        replicaEngine.delete(delete);
        assertThat(delete.version(), equalTo(3l));

        // second time delete with same version should fail
        try {
            delete = new Engine.Delete("test", "1", newUid("1")).version(3l)
                    .versionType(VersionType.INTERNAL.versionTypeForReplicationAndRecovery()).origin(REPLICA);
            replicaEngine.delete(delete);
            fail("excepted VersionConflictEngineException to be thrown");
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // now do the second index on the replica, it should fail
        try {
            index = new Engine.Index(null, newUid("1"), doc).version(2l)
                    .versionType(VersionType.INTERNAL.versionTypeForReplicationAndRecovery()).origin(REPLICA);
            replicaEngine.index(index);
            fail("excepted VersionConflictEngineException to be thrown");
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }


    @Test
    public void testBasicCreatedFlag() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertTrue(index.created());

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertFalse(index.created());

        engine.delete(new Engine.Delete(null, "1", newUid("1")));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertTrue(index.created());
    }

    @Test
    public void testCreatedFlagAfterFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertTrue(index.created());

        engine.delete(new Engine.Delete(null, "1", newUid("1")));

        engine.flush(new Engine.Flush());

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertTrue(index.created());
    }

    protected Term newUid(String id) {
        return new Term("_uid", id);
    }
}
