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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.teradata.prestomanager.common.ApiRequester;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.teradata.prestomanager.common.ExtendedStatus.MULTI_STATUS;
import static com.teradata.prestomanager.controller.ResponseWrapper.wrapResponseList;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public class RequestDispatcher
{
    private static final Logger LOGGER = LogManager.getLogger(RequestDispatcher.class);

    private NodeSet nodeSet;

    @Inject
    public RequestDispatcher(NodeSet nodeSet)
    {
        this.nodeSet = nodeSet;
    }

    public Response forwardRequest(String scope, ApiRequester apiRequester, Collection<Integer> nodeId)
    {
        if (((scope != null) && (!nodeId.isEmpty())) || (scope == null && nodeId.isEmpty())) {
            LOGGER.error("Invalid parameters");
            return Response.status(BAD_REQUEST).entity("Invalid parameters").build();
        }

        ApiScope apiScope;
        try {
            apiScope = ApiScope.fromString(scope);
        }
        catch (IllegalArgumentException e) {
            LOGGER.error("Invalid scope");
            return Response.status(BAD_REQUEST).entity("Invalid scope").build();
        }

        Collection<URI> uriCollection;
        try {
            uriCollection = nodeId.isEmpty() ? nodeSet.getUris(apiScope) : nodeSet.getUrisByIds(nodeId);
        }
        catch (IllegalArgumentException e) {
            LOGGER.error("Invalid or duplicate node ID");
            return Response.status(BAD_REQUEST).entity("Invalid or duplicate node ID").build();
        }

        if (apiScope == ApiScope.COORDINATOR && uriCollection.size() != 1) {
            LOGGER.error("Number of coordinator is not 1");
            return Response.status(INTERNAL_SERVER_ERROR).entity("Number of coordinator is not 1").build();
        }

        List<Response> responseList = new ArrayList<>();
        for (URI uri : uriCollection) {
            responseList.add(apiRequester.send(uri));
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            Entity entity = Entity.entity(
                    mapper.writeValueAsString(wrapResponseList(responseList)), MediaType.APPLICATION_JSON_TYPE);
            return Response.status(MULTI_STATUS).entity(entity).build();
        }
        catch (JsonProcessingException e) {
            return Response.status(INTERNAL_SERVER_ERROR).entity("Error converting Agent responses to JSON").build();
        }
    }
}
