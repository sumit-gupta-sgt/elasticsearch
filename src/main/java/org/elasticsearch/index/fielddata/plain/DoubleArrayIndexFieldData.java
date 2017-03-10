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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigDoubleArrayList;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

/**
 */
public class DoubleArrayIndexFieldData extends AbstractIndexFieldData<DoubleArrayAtomicFieldData> implements IndexNumericFieldData<DoubleArrayAtomicFieldData> {

    private final CircuitBreakerService breakerService;

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            return new DoubleArrayIndexFieldData(index, indexSettings, mapper.names(), mapper.fieldDataType(), cache, breakerService);
        }
    }

    public DoubleArrayIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames,
                                     FieldDataType fieldDataType, IndexFieldDataCache cache, CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        this.breakerService = breakerService;
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.DOUBLE;
    }

    @Override
    public boolean valuesOrdered() {
        // because we might have single values? we can dynamically update a flag to reflect that
        // based on the atomic field data loaded
        return false;
    }

    @Override
    public DoubleArrayAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {

        AtomicReader reader = context.reader();
        Terms terms = reader.terms(getFieldNames().indexName());
        DoubleArrayAtomicFieldData data = null;
        // TODO: Use an actual estimator to estimate before loading.
        NonEstimatingEstimator estimator = new NonEstimatingEstimator(breakerService.getBreaker());
        if (terms == null) {
            data = DoubleArrayAtomicFieldData.empty(reader.maxDoc());
            estimator.afterLoad(null, data.getMemorySizeInBytes());
            return data;
        }
        // TODO: how can we guess the number of terms? numerics end up creating more terms per value...
        final BigDoubleArrayList values = new BigDoubleArrayList();

        values.add(0); // first "t" indicates null value
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        OrdinalsBuilder builder = new OrdinalsBuilder(reader.maxDoc(), acceptableTransientOverheadRatio);
        boolean success = false;
        try {
            final BytesRefIterator iter = builder.buildFromTerms(getNumericType().wrapTermsEnum(terms.iterator(null)));
            BytesRef term;
            while ((term = iter.next()) != null) {
                values.add(NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(term)));
            }
            Ordinals build = builder.build(fieldDataType.getSettings());
            if (!build.isMultiValued() && CommonSettings.removeOrdsOnSingleValue(fieldDataType)) {
                Docs ordinals = build.ordinals();
                final FixedBitSet set = builder.buildDocsWithValuesSet();

                // there's sweet spot where due to low unique value count, using ordinals will consume less memory
                long singleValuesArraySize = reader.maxDoc() * RamUsageEstimator.NUM_BYTES_DOUBLE + (set == null ? 0 : RamUsageEstimator.sizeOf(set.getBits()) + RamUsageEstimator.NUM_BYTES_INT);
                long uniqueValuesArraySize = values.sizeInBytes();
                long ordinalsSize = build.getMemorySizeInBytes();
                if (uniqueValuesArraySize + ordinalsSize < singleValuesArraySize) {
                    data = new DoubleArrayAtomicFieldData.WithOrdinals(values, reader.maxDoc(), build);
                    success = true;
                    return data;
                }

                int maxDoc = reader.maxDoc();
                BigDoubleArrayList sValues = new BigDoubleArrayList(maxDoc);
                for (int i = 0; i < maxDoc; i++) {
                    sValues.add(values.get(ordinals.getOrd(i)));
                }
                assert sValues.size() == maxDoc;
                if (set == null) {
                    data = new DoubleArrayAtomicFieldData.Single(sValues, maxDoc, ordinals.getNumOrds());
                } else {
                    data = new DoubleArrayAtomicFieldData.SingleFixedSet(sValues, maxDoc, set, ordinals.getNumOrds());
                }
            } else {
                data = new DoubleArrayAtomicFieldData.WithOrdinals(values, reader.maxDoc(), build);
            }
            success = true;
            return data;
        } finally {
            if (success) {
                estimator.afterLoad(null, data.getMemorySizeInBytes());
            }
            builder.close();
        }

    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        return new DoubleValuesComparatorSource(this, missingValue, sortMode);
    }
}
