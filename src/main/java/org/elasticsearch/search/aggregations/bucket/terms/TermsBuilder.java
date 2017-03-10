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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 * Builds a {@code terms} aggregation
 */
public class TermsBuilder extends ValuesSourceAggregationBuilder<TermsBuilder> {


    private int size = -1;
    private int shardSize = -1;
    private long minDocCount = -1;
    private Terms.ValueType valueType;
    private Terms.Order order;
    private String includePattern;
    private int includeFlags;
    private String excludePattern;
    private int excludeFlags;
    private String executionHint;

    public TermsBuilder(String name) {
        super(name, "terms");
    }

    /**
     * Sets the size - indicating how many term buckets should be returned (defaults to 10)
     */
    public TermsBuilder size(int size) {
        this.size = size;
        return this;
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard will return to the coordinating node (the
     * node that coordinates the search execution). The higher the shard size is, the more accurate the results are.
     */
    public TermsBuilder shardSize(int shardSize) {
        this.shardSize = shardSize;
        return this;
    }

    /**
     * Set the minimum document count terms should have in order to appear in the response.
     */
    public TermsBuilder minDocCount(long minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    /**
     * Define a regular expression that will determine what terms should be aggregated. The regular expression is based
     * on the {@link java.util.regex.Pattern} class.
     *
     * @see #include(String, int)
     */
    public TermsBuilder include(String regex) {
        return include(regex, 0);
    }

    /**
     * Define a regular expression that will determine what terms should be aggregated. The regular expression is based
     * on the {@link java.util.regex.Pattern} class.
     *
     * @see java.util.regex.Pattern#compile(String, int)
     */
    public TermsBuilder include(String regex, int flags) {
        this.includePattern = regex;
        this.includeFlags = flags;
        return this;
    }

    /**
     * Define a regular expression that will filter out terms that should be excluded from the aggregation. The regular
     * expression is based on the {@link java.util.regex.Pattern} class.
     *
     * @see #exclude(String, int)
     */
    public TermsBuilder exclude(String regex) {
        return exclude(regex, 0);
    }

    /**
     * Define a regular expression that will filter out terms that should be excluded from the aggregation. The regular
     * expression is based on the {@link java.util.regex.Pattern} class.
     *
     * @see java.util.regex.Pattern#compile(String, int)
     */
    public TermsBuilder exclude(String regex, int flags) {
        this.excludePattern = regex;
        this.excludeFlags = flags;
        return this;
    }

    /**
     * When using scripts, the value type indicates the types of the values the script is generating.
     */
    public TermsBuilder valueType(Terms.ValueType valueType) {
        this.valueType = valueType;
        return this;
    }

    /**
     * Defines the order in which the buckets will be returned.
     */
    public TermsBuilder order(Terms.Order order) {
        this.order = order;
        return this;
    }

    public TermsBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (size >=0) {
            builder.field("size", size);
        }
        if (shardSize >= 0) {
            builder.field("shard_size", shardSize);
        }
        if (minDocCount >= 0) {
            builder.field("min_doc_count", minDocCount);
        }
        if (valueType != null) {
            builder.field("value_type", valueType.name().toLowerCase(Locale.ROOT));
        }
        if (order != null) {
            builder.field("order");
            order.toXContent(builder, params);
        }
        if (includePattern != null) {
            if (includeFlags == 0) {
                builder.field("include", includePattern);
            } else {
                builder.startObject("include")
                        .field("pattern", includePattern)
                        .field("flags", includeFlags)
                        .endObject();
            }
        }
        if (excludePattern != null) {
            if (excludeFlags == 0) {
                builder.field("exclude", excludePattern);
            } else {
                builder.startObject("exclude")
                        .field("pattern", excludePattern)
                        .field("flags", excludeFlags)
                        .endObject();
            }
        }
        if (executionHint != null) {
            builder.field("execution_hint", executionHint);
        }
        return builder;
    }
}
