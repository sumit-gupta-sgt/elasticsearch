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

package org.elasticsearch.action.admin.indices.warmer.get;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 */
public class TransportGetWarmersAction extends TransportClusterInfoAction<GetWarmersRequest, GetWarmersResponse> {

    @Inject
    public TransportGetWarmersAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, transportService, clusterService, threadPool);
    }

    @Override
    protected String transportAction() {
        return GetWarmersAction.NAME;
    }

    @Override
    protected GetWarmersRequest newRequest() {
        return new GetWarmersRequest();
    }

    @Override
    protected GetWarmersResponse newResponse() {
        return new GetWarmersResponse();
    }

    @Override
    protected void doMasterOperation(final GetWarmersRequest request, final ClusterState state, final ActionListener<GetWarmersResponse> listener) throws ElasticsearchException {
        ImmutableOpenMap<String, ImmutableList<IndexWarmersMetaData.Entry>> result = state.metaData().findWarmers(
                request.indices(), request.types(), request.warmers()
        );
        listener.onResponse(new GetWarmersResponse(result));
    }
}
