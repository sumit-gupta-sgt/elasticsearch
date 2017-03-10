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

package org.elasticsearch.indices.recovery;

import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class RecoveryStatus {

    public static enum Stage {
        INIT,
        INDEX,
        TRANSLOG,
        FINALIZE,
        DONE
    }

    final ShardId shardId;
    final long recoveryId;
    final InternalIndexShard indexShard;

    public RecoveryStatus(long recoveryId, InternalIndexShard indexShard) {
        this.recoveryId = recoveryId;
        this.indexShard = indexShard;
        this.shardId = indexShard.shardId();
    }

    volatile Thread recoveryThread;
    private volatile boolean canceled;
    volatile boolean sentCanceledToSource;

    private volatile ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();
    ConcurrentMap<String, String> checksums = ConcurrentCollections.newConcurrentMap();

    final long startTime = System.currentTimeMillis();
    long time;
    List<String> phase1FileNames;
    List<Long> phase1FileSizes;
    List<String> phase1ExistingFileNames;
    List<Long> phase1ExistingFileSizes;
    long phase1TotalSize;
    long phase1ExistingTotalSize;

    volatile Stage stage = Stage.INIT;
    volatile long currentTranslogOperations = 0;
    AtomicLong currentFilesSize = new AtomicLong();

    public long startTime() {
        return startTime;
    }

    public long time() {
        return this.time;
    }

    public long phase1TotalSize() {
        return phase1TotalSize;
    }

    public long phase1ExistingTotalSize() {
        return phase1ExistingTotalSize;
    }

    public Stage stage() {
        return stage;
    }

    public long currentTranslogOperations() {
        return currentTranslogOperations;
    }

    public long currentFilesSize() {
        return currentFilesSize.get();
    }
    
    public boolean isCanceled() {
        return canceled;
    }
    
    public synchronized void cancel() {
        canceled = true;
    }
    
    public IndexOutput getOpenIndexOutput(String key) {
        final ConcurrentMap<String, IndexOutput> outputs = openIndexOutputs;
        if (canceled || outputs == null) {
            return null;
        }
        return outputs.get(key);
    }
    
    public synchronized Set<Entry<String, IndexOutput>> cancleAndClearOpenIndexInputs() {
        cancel();
        final ConcurrentMap<String, IndexOutput> outputs = openIndexOutputs;
        openIndexOutputs = null;
        if (outputs == null) {
            return null;
        }
        Set<Entry<String, IndexOutput>> entrySet = outputs.entrySet();
        return entrySet;
    }
    

    public IndexOutput removeOpenIndexOutputs(String name) {
        final ConcurrentMap<String, IndexOutput> outputs = openIndexOutputs;
        if (outputs == null) {
            return null;
        }
        return outputs.remove(name);
    }

    public synchronized IndexOutput openAndPutIndexOutput(String key, String name, Store store) throws IOException {
        if (isCanceled()) {
            return null;
        }
        final ConcurrentMap<String, IndexOutput> outputs = openIndexOutputs;
        IndexOutput indexOutput = store.createOutputRaw(name);
        outputs.put(key, indexOutput);
        return indexOutput;
    }
}
