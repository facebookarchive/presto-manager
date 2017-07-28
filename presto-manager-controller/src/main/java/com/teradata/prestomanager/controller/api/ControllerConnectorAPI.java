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

import com.google.inject.Inject;
import com.teradata.prestomanager.common.ApiRequester;
import com.teradata.prestomanager.controller.AbstractControllerAPI;
import com.teradata.prestomanager.controller.RequestDispatcher;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.UUID;

import static org.eclipse.jetty.http.HttpMethod.DELETE;
import static org.eclipse.jetty.http.HttpMethod.GET;
import static org.eclipse.jetty.http.HttpMethod.POST;

@Path("/connectors")
@Singleton
public class ControllerConnectorAPI
        extends AbstractControllerAPI
{
    @Inject
    public ControllerConnectorAPI(
            Client forwardingClient,
            RequestDispatcher requestDispatcher)
    {
        super(forwardingClient, requestDispatcher);
    }

    @GET
    @Produces({MediaType.TEXT_PLAIN})
    public Response getConnectors(@QueryParam("scope") String scope,
            @QueryParam("nodeId") List<UUID> nodeId)
    {
        ApiRequester apiRequester = requesterBuilder(ControllerConnectorAPI.class)
                .httpMethod(GET)
                .accept(MediaType.TEXT_PLAIN)
                .build();

        return forwardRequest(scope, apiRequester, nodeId);
    }

    @GET
    @Path("/{file}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response getConnectorFile(
            @PathParam("file") String file,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<UUID> nodeId)
    {
        ApiRequester apiRequester = requesterBuilder(ControllerConnectorAPI.class)
                .pathMethod("getConnectorFile")
                .httpMethod(GET)
                .resolveTemplate("file", file)
                .accept(MediaType.TEXT_PLAIN)
                .build();

        return forwardRequest(scope, apiRequester, nodeId);
    }

    @GET
    @Path("/{file}/{property}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response getConnectorProperty(
            @PathParam("file") String file,
            @PathParam("property") String property,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<UUID> nodeId)
    {
        ApiRequester apiRequester = requesterBuilder(ControllerConnectorAPI.class)
                .pathMethod("getConnectorProperty")
                .httpMethod(GET)
                .resolveTemplate("file", file)
                .resolveTemplate("property", property)
                .accept(MediaType.TEXT_PLAIN)
                .build();

        return forwardRequest(scope, apiRequester, nodeId);
    }

    @POST
    @Path("/{file}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response setConnectorFileByURL(
            String url,
            @PathParam("file") String file,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<UUID> nodeId)
    {
        ApiRequester apiRequester = requesterBuilder(ControllerConnectorAPI.class)
                .pathMethod("setConnectorFileByURL")
                .httpMethod(POST)
                .resolveTemplate("file", file)
                .accept(MediaType.TEXT_PLAIN)
                .entity(Entity.entity(url, MediaType.TEXT_PLAIN))
                .build();

        return forwardRequest(scope, apiRequester, nodeId);
    }

    @DELETE
    @Path("/{file}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response deleteConnectorFile(
            @PathParam("file") String file,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<UUID> nodeId)
    {
        ApiRequester apiRequester = requesterBuilder(ControllerConnectorAPI.class)
                .pathMethod("deleteConnectorFile")
                .httpMethod(DELETE)
                .resolveTemplate("file", file)
                .accept(MediaType.TEXT_PLAIN)
                .build();

        return forwardRequest(scope, apiRequester, nodeId);
    }
}
