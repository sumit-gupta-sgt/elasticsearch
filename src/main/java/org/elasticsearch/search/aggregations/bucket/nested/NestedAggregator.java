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
package org.elasticsearch.search.aggregations.bucket.nested;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;

/**
 *
 */
public class NestedAggregator extends SingleBucketAggregator implements ReaderContextAware {

    private final Filter parentFilter;
    private final Filter childFilter;

    private Bits childDocs;
    private FixedBitSet parentDocs;

    public NestedAggregator(String name, AggregatorFactories factories, String nestedPath, AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, aggregationContext, parent);
        MapperService.SmartNameObjectMapper mapper = aggregationContext.searchContext().smartNameObjectMapper(nestedPath);
        if (mapper == null) {
            throw new AggregationExecutionException("facet nested path [" + nestedPath + "] not found");
        }
        ObjectMapper objectMapper = mapper.mapper();
        if (objectMapper == null) {
            throw new AggregationExecutionException("facet nested path [" + nestedPath + "] not found");
        }
        if (!objectMapper.nested().isNested()) {
            throw new AggregationExecutionException("facet nested path [" + nestedPath + "] is not nested");
        }
        parentFilter = aggregationContext.searchContext().filterCache().cache(NonNestedDocsFilter.INSTANCE);
        childFilter = aggregationContext.searchContext().filterCache().cache(objectMapper.nestedTypeFilter());
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        try {
            DocIdSet docIdSet = parentFilter.getDocIdSet(reader, null);
            // In ES if parent is deleted, then also the children are deleted. Therefore acceptedDocs can also null here.
            childDocs = DocIdSets.toSafeBits(reader.reader(), childFilter.getDocIdSet(reader, null));
            if (DocIdSets.isEmpty(docIdSet)) {
                parentDocs = null;
            } else {
                parentDocs = (FixedBitSet) docIdSet;
            }
        } catch (IOException ioe) {
            throw new AggregationExecutionException("Failed to aggregate [" + name + "]", ioe);
        }
    }

    @Override
    public void collect(int parentDoc, long bucketOrd) throws IOException {

        // here we translate the parent doc to a list of its nested docs, and then call super.collect for evey one of them
        // so they'll be collected

        if (parentDoc == 0 || parentDocs == null) {
            return;
        }
        int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
        int numChildren = 0;
        for (int i = (parentDoc - 1); i > prevParentDoc; i--) {
            if (childDocs.get(i)) {
                ++numChildren;
                collectBucketNoCounts(i, bucketOrd);
            }
        }
        incrementBucketDocCount(numChildren, bucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        return new InternalNested(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalNested(name, 0, buildEmptySubAggregations());
    }

    public static class Factory extends AggregatorFactory {

        private final String path;

        public Factory(String name, String path) {
            super(name, InternalNested.TYPE.name());
            this.path = path;
        }

        @Override
        public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
            return new NestedAggregator(name, factories, path, context, parent);
        }
    }
}
