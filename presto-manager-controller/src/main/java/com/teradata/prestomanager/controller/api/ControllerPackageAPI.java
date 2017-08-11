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

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.UUID;

import static org.eclipse.jetty.http.HttpMethod.DELETE;
import static org.eclipse.jetty.http.HttpMethod.POST;
import static org.eclipse.jetty.http.HttpMethod.PUT;

@Path("/package")
@Api(description = "API to install, uninstall, upgrade Presto in the specified scope")
@Singleton
public class ControllerPackageAPI
        extends AbstractControllerAPI
{
    @Inject
    public ControllerPackageAPI(
            Client forwardingClient,
            RequestDispatcher requestDispatcher)
    {
        super(forwardingClient, requestDispatcher);
    }

    @PUT
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Install Presto using rpm or tarball")
    @ApiResponses(value = {
            @ApiResponse(code = 207, message = "Multiple responses available"),
            @ApiResponse(code = 400, message = "Request contains invalid parameters")})
    public Response install(String urlToFetchPackage,
            @QueryParam("checkDependencies") @DefaultValue("true") boolean checkDependencies,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<UUID> nodeId)
    {
        ApiRequester.Builder apiRequester = requesterBuilder(ControllerPackageAPI.class)
                .httpMethod(PUT)
                .accept(MediaType.TEXT_PLAIN)
                .entity(Entity.entity(urlToFetchPackage, MediaType.TEXT_PLAIN));

        optionalQueryParam(apiRequester, "checkDependencies", checkDependencies);

        return forwardRequest(scope, apiRequester.build(), nodeId);
    }

    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Upgrade Presto")
    @ApiResponses(value = {
            @ApiResponse(code = 207, message = "Multiple responses available"),
            @ApiResponse(code = 400, message = "Request contains invalid parameters")})
    public Response upgrade(String urlToFetchPackage,
            @QueryParam("checkDependencies") @DefaultValue("true") boolean checkDependencies,
            @QueryParam("preserveConfig") @DefaultValue("true") boolean preserveConfig,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<UUID> nodeId)
    {
        ApiRequester.Builder apiRequester = requesterBuilder(ControllerPackageAPI.class)
                .httpMethod(POST)
                .accept(MediaType.TEXT_PLAIN)
                .entity(Entity.entity(urlToFetchPackage, MediaType.TEXT_PLAIN));

        optionalQueryParam(apiRequester, "checkDependencies", checkDependencies);
        optionalQueryParam(apiRequester, "preserveConfig", preserveConfig);

        return forwardRequest(scope, apiRequester.build(), nodeId);
    }

    @DELETE
    @ApiOperation(value = "Uninstall Presto")
    @ApiResponses(value = {
            @ApiResponse(code = 207, message = "Multiple responses available"),
            @ApiResponse(code = 400, message = "Request contains invalid parameters")})
    public Response uninstall(
            @QueryParam("checkDependencies") @DefaultValue("true") boolean checkDependencies,
            @QueryParam("ignoreErrors") @DefaultValue("false") boolean ignoreErrors,
            @QueryParam("scope") String scope,
            @QueryParam("nodeId") List<UUID> nodeId)
    {
        ApiRequester.Builder apiRequester = requesterBuilder(ControllerPackageAPI.class)
                .httpMethod(DELETE);

        optionalQueryParam(apiRequester, "ignoreErrors", ignoreErrors);
        optionalQueryParam(apiRequester, "checkDependencies", checkDependencies);

        return forwardRequest(scope, apiRequester.build(), nodeId);
    }
}
