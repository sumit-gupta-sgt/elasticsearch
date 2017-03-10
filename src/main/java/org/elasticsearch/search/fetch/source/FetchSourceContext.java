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

package org.elasticsearch.search.fetch.source;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 */
public class FetchSourceContext implements Streamable {

    public static final FetchSourceContext FETCH_SOURCE = new FetchSourceContext(true);
    public static final FetchSourceContext DO_NOT_FETCH_SOURCE = new FetchSourceContext(false);
    private boolean fetchSource;
    private String[] includes;
    private String[] excludes;


    FetchSourceContext() {

    }

    public FetchSourceContext(boolean fetchSource) {
        this(fetchSource, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
    }

    public FetchSourceContext(String include) {
        this(include, null);
    }

    public FetchSourceContext(String include, String exclude) {
        this(true,
                include == null ? Strings.EMPTY_ARRAY : new String[]{include},
                exclude == null ? Strings.EMPTY_ARRAY : new String[]{exclude});
    }

    public FetchSourceContext(String[] includes) {
        this(true, includes, Strings.EMPTY_ARRAY);
    }

    public FetchSourceContext(String[] includes, String[] excludes) {
        this(true, includes, excludes);
    }

    public FetchSourceContext(boolean fetchSource, String[] includes, String[] excludes) {
        this.fetchSource = fetchSource;
        this.includes = includes == null ? Strings.EMPTY_ARRAY : includes;
        this.excludes = excludes == null ? Strings.EMPTY_ARRAY : excludes;
    }

    public boolean fetchSource() {
        return this.fetchSource;
    }

    public FetchSourceContext fetchSource(boolean fetchSource) {
        this.fetchSource = fetchSource;
        return this;
    }

    public String[] includes() {
        return this.includes;
    }

    public FetchSourceContext includes(String[] includes) {
        this.includes = includes;
        return this;
    }

    public String[] excludes() {
        return this.excludes;
    }

    public FetchSourceContext excludes(String[] excludes) {
        this.excludes = excludes;
        return this;
    }

    public static FetchSourceContext optionalReadFromStream(StreamInput in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        FetchSourceContext context = new FetchSourceContext();
        context.readFrom(in);
        return context;
    }

    public static void optionalWriteToStream(FetchSourceContext context, StreamOutput out) throws IOException {
        if (context == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        context.writeTo(out);
    }

    public static FetchSourceContext parseFromRestRequest(RestRequest request) {
        Boolean fetchSource = null;
        String[] source_excludes = null;
        String[] source_includes = null;

        String source = request.param("_source");
        if (source != null) {
            if (Booleans.isExplicitTrue(source)) {
                fetchSource = true;
            } else if (Booleans.isExplicitFalse(source)) {
                fetchSource = false;
            } else {
                source_includes = Strings.splitStringByCommaToArray(source);
            }
        }
        String sIncludes = request.param("_source_includes");
        sIncludes = request.param("_source_include", sIncludes);
        if (sIncludes != null) {
            source_includes = Strings.splitStringByCommaToArray(sIncludes);
        }

        String sExcludes = request.param("_source_excludes");
        sExcludes = request.param("_source_exclude", sExcludes);
        if (sExcludes != null) {
            source_excludes = Strings.splitStringByCommaToArray(sExcludes);
        }

        if (fetchSource != null || source_includes != null || source_excludes != null) {
            return new FetchSourceContext(fetchSource == null ? true : fetchSource, source_includes, source_excludes);
        }
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        fetchSource = in.readBoolean();
        includes = in.readStringArray();
        excludes = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(fetchSource);
        out.writeStringArray(includes);
        out.writeStringArray(excludes);
    }
}
