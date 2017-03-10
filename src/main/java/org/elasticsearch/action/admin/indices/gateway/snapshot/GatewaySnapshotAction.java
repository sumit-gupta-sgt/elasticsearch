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

package org.elasticsearch.action.admin.indices.gateway.snapshot;

import org.elasticsearch.action.admin.indices.IndicesAction;
import org.elasticsearch.client.IndicesAdminClient;

/**
 * @deprecated Use snapshot/restore API instead
 */
@Deprecated
public class GatewaySnapshotAction extends IndicesAction<GatewaySnapshotRequest, GatewaySnapshotResponse, GatewaySnapshotRequestBuilder> {

    public static final GatewaySnapshotAction INSTANCE = new GatewaySnapshotAction();
    public static final String NAME = "indices/gateway/snapshot";

    private GatewaySnapshotAction() {
        super(NAME);
    }

    @Override
    public GatewaySnapshotResponse newResponse() {
        return new GatewaySnapshotResponse();
    }

    @Override
    public GatewaySnapshotRequestBuilder newRequestBuilder(IndicesAdminClient client) {
        return new GatewaySnapshotRequestBuilder(client);
    }
}
