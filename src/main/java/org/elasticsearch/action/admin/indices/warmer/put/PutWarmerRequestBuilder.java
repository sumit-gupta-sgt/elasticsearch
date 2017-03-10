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

package org.elasticsearch.action.admin.indices.warmer.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;

/**
 *
 */
public class PutWarmerRequestBuilder extends AcknowledgedRequestBuilder<PutWarmerRequest, PutWarmerResponse, PutWarmerRequestBuilder> {

    public PutWarmerRequestBuilder(IndicesAdminClient indicesClient, String name) {
        super((InternalIndicesAdminClient) indicesClient, new PutWarmerRequest().name(name));
    }

    public PutWarmerRequestBuilder(IndicesAdminClient indicesClient) {
        super((InternalIndicesAdminClient) indicesClient, new PutWarmerRequest());
    }

    /**
     * Sets the name of the warmer.
     */
    public PutWarmerRequestBuilder setName(String name) {
        request.name(name);
        return this;
    }

    /**
     * Sets the search request to use to warm the index when applicable.
     */
    public PutWarmerRequestBuilder setSearchRequest(SearchRequest searchRequest) {
        request.searchRequest(searchRequest);
        return this;
    }

    /**
     * Sets the search request to use to warm the index when applicable.
     */
    public PutWarmerRequestBuilder setSearchRequest(SearchRequestBuilder searchRequest) {
        request.searchRequest(searchRequest);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PutWarmerResponse> listener) {
        ((IndicesAdminClient) client).putWarmer(request, listener);
    }
}
