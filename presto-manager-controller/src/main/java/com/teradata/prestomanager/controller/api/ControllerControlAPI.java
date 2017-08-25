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
import com.google.inject.Singleton;
import com.teradata.prestomanager.common.ApiRequester;
import com.teradata.prestomanager.common.StopType;
import com.teradata.prestomanager.controller.RequestDispatcher;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.eclipse.jetty.http.HttpMethod.GET;
import static org.eclipse.jetty.http.HttpMethod.POST;

@Path("/presto")
@Api(description = "API to start, stop, restart Presto and get Presto status in the specified scope")
@Singleton
public class ControllerControlAPI
        extends AbstractControllerAPI
{
    @Inject
    public ControllerControlAPI(
            Client forwardingClient,
            RequestDispatcher requestDispatcher)
    {
        super(forwardingClient, requestDispatcher);
    }

    @POST
    @Path("/start")
    @ApiOperation(value = "Start Presto")
    @Produces(MediaType.TEXT_PLAIN)
    @ApiResponses(value = {
            @ApiResponse(code = 207, message = "Multiple responses available"),
            @ApiResponse(code = 400, message = "Request contains invalid parameters")})
    public Response startPresto(
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<String> nodeId)
    {
        ApiRequester apiRequester = requesterBuilder(ControllerControlAPI.class)
                .httpMethod(POST)
                .accept(MediaType.TEXT_PLAIN)
                .pathMethod("startPresto")
                .build();

        return forwardRequest(scope, apiRequester, nodeId);
    }

    @POST
    @Path("/stop")
    @ApiOperation(value = "Stop Presto")
    @ApiResponses(value = {
            @ApiResponse(code = 207, message = "Multiple responses available"),
            @ApiResponse(code = 400, message = "Request contains invalid parameters")})
    public Response stopPresto(
            @QueryParam("stopType") @DefaultValue("GRACEFUL") StopType stopType,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<String> nodeId)
    {
        ApiRequester.Builder apiRequester = requesterBuilder(ControllerControlAPI.class)
                .httpMethod(POST)
                .pathMethod("stopPresto");

        optionalQueryParam(apiRequester, "stopType", stopType);

        return forwardRequest(scope, apiRequester.build(), nodeId);
    }

    @POST
    @Path("/restart")
    @ApiOperation(value = "Restart Presto")
    @ApiResponses(value = {
            @ApiResponse(code = 207, message = "Multiple responses available"),
            @ApiResponse(code = 400, message = "Request contains invalid parameters")})
    public Response restartPresto(
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<String> nodeId)
    {
        ApiRequester apiRequester = requesterBuilder(ControllerControlAPI.class)
                .httpMethod(POST)
                .pathMethod("restartPresto")
                .build();

        return forwardRequest(scope, apiRequester, nodeId);
    }

    @GET
    @Path("/status")
    @Produces(APPLICATION_JSON)
    @ApiOperation(value = "Get Presto status")
    @ApiResponses(value = {
            @ApiResponse(code = 207, message = "Multiple responses available"),
            @ApiResponse(code = 400, message = "Request contains invalid parameters")})
    public Response prestoStatus(
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<String> nodeId)
    {
        ApiRequester apiRequester = requesterBuilder(ControllerControlAPI.class)
                .httpMethod(GET)
                .accept(MediaType.APPLICATION_JSON)
                .pathMethod("prestoStatus")
                .build();

        return forwardRequest(scope, apiRequester, nodeId);
    }
}
