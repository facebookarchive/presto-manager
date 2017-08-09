/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.teradata.prestomanager.controller;

import com.google.inject.Inject;
import com.teradata.prestomanager.common.ApiRequester;
import com.teradata.prestomanager.controller.ResponseWrapper.WrappedResponse;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.teradata.prestomanager.common.ExtendedStatus.MULTI_STATUS;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@ThreadSafe
public class RequestDispatcher
{
    private static final Logger LOGGER = Logger.get(RequestDispatcher.class);

    private final ResponseWrapper wrapper;
    private AgentMap agentMap;

    @Inject
    public RequestDispatcher(ResponseWrapper wrapper,
            AgentMap agentMap)
    {
        this.wrapper = requireNonNull(wrapper);
        this.agentMap = requireNonNull(agentMap);
    }

    public Response forwardRequest(
            String scope, ApiRequester apiRequester, Collection<UUID> nodeId)
    {
        if (((scope != null) && (!nodeId.isEmpty()))
                || (scope == null && nodeId.isEmpty())) {
            LOGGER.error("Invalid parameters");
            return Response.status(BAD_REQUEST)
                    .entity("Invalid parameters").build();
        }

        ApiScope apiScope;
        try {
            apiScope = ApiScope.fromString(scope);
        }
        catch (IllegalArgumentException e) {
            LOGGER.error("Invalid scope");
            return Response.status(BAD_REQUEST).entity("Invalid scope").build();
        }

        Map<UUID, URI> uriMap;
        try {
            uriMap = nodeId.isEmpty()
                    ? agentMap.getUrisByScope(apiScope)
                    : agentMap.getUrisByIds(nodeId);
        }
        catch (IllegalArgumentException e) {
            LOGGER.error("Invalid or duplicate node ID");
            return Response.status(BAD_REQUEST)
                    .entity("Invalid or duplicate node ID").build();
        }

        if (apiScope == ApiScope.COORDINATOR && uriMap.size() != 1) {
            LOGGER.error("Number of coordinator is not 1");
            return Response.status(INTERNAL_SERVER_ERROR)
                    .entity("Number of coordinator is not 1").build();
        }

        // Jackson serializes ArrayLists as JSON arrays
        Map<UUID, WrappedResponse> responses = uriMap.entrySet().parallelStream()
                .map(e -> new SimpleEntry<>(
                        e.getKey(),
                        wrapper.wrapResponse(apiRequester.send(e.getValue()))))
                .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue));

        return Response.status(MULTI_STATUS)
                .type(MediaType.APPLICATION_JSON)
                .entity(responses)
                .build();
    }
}
