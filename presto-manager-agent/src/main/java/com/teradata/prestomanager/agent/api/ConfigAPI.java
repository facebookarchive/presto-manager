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
package com.teradata.prestomanager.agent.api;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.teradata.prestomanager.agent.ApiFileHandler;
import com.teradata.prestomanager.agent.PrestoConfig;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static java.util.Objects.requireNonNull;

@Path("/config")
@Api(description = "API to read and write to Presto configuration files")
@Singleton
public final class ConfigAPI
{
    private final ApiFileHandler apiFileHandler;

    @Inject
    public ConfigAPI(PrestoConfig config)
    {
        apiFileHandler = new ApiFileHandler(
                requireNonNull(config.getConfigDirectory()));
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get available configuration files")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Retrieved configuration")})
    public synchronized Response getConfig()
    {
        return apiFileHandler.getFileNameList();
    }

    @GET
    @Path("/{file}")
    @Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Get the content of a configuration file")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Retrieved file"),
            @ApiResponse(code = 404, message = "Resource not found")})
    public synchronized Response getConfigFile(
            @PathParam("file") @ApiParam("The name of a file") String file)
    {
        return apiFileHandler.getFile(file);
    }

    @GET
    @Path("/{file}/{property}")
    @Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Get specific configuration property")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Retrieved property"),
            @ApiResponse(code = 404, message = "Resource not found")})
    public synchronized Response getConfigProperty(
            @PathParam("file") @ApiParam("The name of a file") String file,
            @PathParam("property") @ApiParam("A specific property") String property)
    {
        return apiFileHandler.getFileProperty(file, property);
    }

    @PUT
    @Path("/{file}/{property}")
    @Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Update or create a certain property of configuration file")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Property updated or created"),
            @ApiResponse(code = 404, message = "Resource not found")})
    public synchronized Response updateConfigProperty(
            @PathParam("file") @ApiParam("The name of a file") String file,
            @PathParam("property") @ApiParam("A specific property") String property,
            String value)
    {
        return apiFileHandler.updateProperty(file, property, value);
    }

    @POST
    @Path("/{file}")
    @Consumes(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Replace this file with the file at the given URL")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 409, message = "Request conflicts with current state")})
    public synchronized Response setConfigFileByURL(
            @PathParam("file") @ApiParam("The name of a file") String file,
            String url)
    {
        return apiFileHandler.replaceFileFromURL(file, url);
    }

    @DELETE
    @Path("/{file}")
    @ApiOperation(value = "Delete a configuration file")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 404, message = "Resource not found"),
            @ApiResponse(code = 409, message = "Request conflicts with current state")})
    public synchronized Response deleteConfigFile(
            @PathParam("file") @ApiParam("The name of a file") String file)
    {
        return apiFileHandler.deleteFile(file);
    }
}
