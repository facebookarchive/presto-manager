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
package com.teradata.prestomanager.controller.api;

import com.google.gson.Gson;
import com.teradata.prestomanager.controller.NodeSet;
import com.teradata.prestomanager.common.ApiRequester;
import com.teradata.prestomanager.controller.ApiScope;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.teradata.prestomanager.common.ExtendedStatus.MULTI_STATUS;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static org.eclipse.jetty.http.HttpMethod.DELETE;
import static org.eclipse.jetty.http.HttpMethod.GET;
import static org.eclipse.jetty.http.HttpMethod.POST;

@Path("/config")
@Singleton
public class ControllerConfigAPI
{
    private static final Logger LOGGER = LogManager.getLogger(ControllerConfigAPI.class);
    private NodeSet nodeSet = NodeSet.getInstance();

    @GET
    @Produces({MediaType.TEXT_PLAIN})
    public Response getConfig(@QueryParam("scope") String scope,
            @QueryParam("nodeId") List<Integer> nodeId)
    {
        ApiRequester apiRequester = ApiRequester.builder(ControllerConfigAPI.class)
                .pathMethod("getConfig")
                .httpMethod(GET)
                .accept(MediaType.TEXT_PLAIN)
                .build();

        return getResponseFromRequest(scope, apiRequester, nodeId);
    }

    @GET
    @Path("/{file}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response getConfigFile(
            @PathParam("file") String file,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<Integer> nodeId)
    {
        ApiRequester apiRequester = ApiRequester.builder(ControllerConfigAPI.class)
                .pathMethod("getConfigFile")
                .httpMethod(GET)
                .resolveTemplate("file", file)
                .accept(MediaType.TEXT_PLAIN)
                .build();

        return getResponseFromRequest(scope, apiRequester, nodeId);
    }

    @GET
    @Path("/{file}/{property}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response getConfigProperty(
            @PathParam("file") String file,
            @PathParam("property") String property,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<Integer> nodeId)
    {
        ApiRequester apiRequester = ApiRequester.builder(ControllerConfigAPI.class)
                .pathMethod("getConfigProperty")
                .httpMethod(GET)
                .resolveTemplate("file", file)
                .resolveTemplate("property", property)
                .accept(MediaType.TEXT_PLAIN)
                .build();

        return getResponseFromRequest(scope, apiRequester, nodeId);
    }

    @POST
    @Path("/{file}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response setConfigFileByURL(
            @PathParam("file") String file,
            String url,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<Integer> nodeId)
    {
        ApiRequester apiRequester = ApiRequester.builder(ControllerConfigAPI.class)
                .pathMethod("setConfigFileByURL")
                .httpMethod(POST)
                .resolveTemplate("file", file)
                .accept(MediaType.TEXT_PLAIN)
                .entity(Entity.entity(url, MediaType.TEXT_PLAIN))
                .build();

        return getResponseFromRequest(scope, apiRequester, nodeId);
    }

    @DELETE
    @Path("/{file}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response deleteConfigFile(
            @PathParam("file") String file,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<Integer> nodeId)
    {
        ApiRequester apiRequester = ApiRequester.builder(ControllerConfigAPI.class)
                .pathMethod("deleteConfigFile")
                .httpMethod(DELETE)
                .resolveTemplate("file", file)
                .accept(MediaType.TEXT_PLAIN)
                .build();

        return getResponseFromRequest(scope, apiRequester, nodeId);
    }

    private Response getResponseFromRequest(String scope, ApiRequester apiRequester, Collection<Integer> nodeId)
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
        return Response.status(MULTI_STATUS)
                .entity(new Gson().toJson(responseList)).type(MediaType.APPLICATION_JSON).build();
    }
}
