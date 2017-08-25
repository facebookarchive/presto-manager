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
import com.teradata.prestomanager.controller.RequestDispatcher;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.time.Instant;
import java.util.List;

import static org.eclipse.jetty.http.HttpMethod.DELETE;
import static org.eclipse.jetty.http.HttpMethod.GET;

@Path("/logs")
@Api(description = "API to read and modify log files in the specified scope")
@Singleton
public class ControllerLogsAPI
        extends AbstractControllerAPI
{
    @Inject
    public ControllerLogsAPI(
            Client forwardingClient,
            RequestDispatcher requestDispatcher)
    {
        super(forwardingClient, requestDispatcher);
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Get a listing of log files")
    @ApiResponses(value = {
            @ApiResponse(code = 207, message = "Multiple responses available"),
            @ApiResponse(code = 400, message = "Request contains invalid parameters")})
    public Response getConnectors(@QueryParam("scope") String scope,
            @QueryParam("nodeId") List<String> nodeId)
    {
        ApiRequester apiRequester = requesterBuilder(ControllerLogsAPI.class)
                .httpMethod(GET)
                .accept(MediaType.TEXT_PLAIN)
                .build();

        return forwardRequest(scope, apiRequester, nodeId);
    }

    @GET
    @Path("/{file}")
    @Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Get Presto log file")
    @ApiResponses(value = {
            @ApiResponse(code = 207, message = "Multiple responses available"),
            @ApiResponse(code = 400, message = "Request contains invalid parameters")})
    public Response getLog(
            @PathParam("file") String file,
            @QueryParam("from") Instant fromDate,
            @QueryParam("to") Instant toDate,
            @QueryParam("level") @DefaultValue("ALL") String level,
            @QueryParam("n") Integer maxEntries,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<String> nodeId)
    {
        ApiRequester.Builder apiRequester = requesterBuilder(ControllerLogsAPI.class)
                .httpMethod(GET)
                .accept(MediaType.TEXT_PLAIN)
                .pathMethod("getLog")
                .resolveTemplate("file", file);

        optionalQueryParam(apiRequester, "from", fromDate);
        optionalQueryParam(apiRequester, "to", toDate);
        optionalQueryParam(apiRequester, "level", level);
        optionalQueryParam(apiRequester, "n", maxEntries);

        return forwardRequest(scope, apiRequester.build(), nodeId);
    }

    @DELETE
    @Path("/{file}")
    @Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Delete Presto logs")
    @ApiResponses(value = {
            @ApiResponse(code = 207, message = "Multiple responses available"),
            @ApiResponse(code = 400, message = "Request contains invalid parameters")})
    public Response deleteLog(
            @PathParam("file") String file,
            @QueryParam("to") Instant toDate,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<String> nodeId)
    {
        ApiRequester.Builder apiRequester = requesterBuilder(ControllerLogsAPI.class)
                .httpMethod(DELETE)
                .accept(MediaType.TEXT_PLAIN)
                .pathMethod("deleteLog")
                .resolveTemplate("file", file);

        optionalQueryParam(apiRequester, "to", toDate);

        return forwardRequest(scope, apiRequester.build(), nodeId);
    }
}
