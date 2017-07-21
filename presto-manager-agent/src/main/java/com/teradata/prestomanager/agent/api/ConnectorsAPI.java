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

import com.teradata.prestomanager.agent.APIFileHandler;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.nio.file.Paths;

@Path("/connectors")
@Api(description = "the connectors API")
@javax.annotation.Generated(
        value = "io.swagger.codegen.languages.JavaJAXRSSpecServerCodegen",
        date = "2017-06-23T09:53:13.549-04:00")
@Singleton
public final class ConnectorsAPI
{
    private static final APIFileHandler apiFileHandler = new APIFileHandler(Paths.get("/etc/presto/catalog"));
    private static final Logger LOGGER = LogManager.getLogger(ConnectorsAPI.class);

    @GET
    @Produces({MediaType.TEXT_PLAIN})
    @ApiOperation(value = "Get avaiable connector file names")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Retrieved configuration", response = String.class)})
    public synchronized Response getConnectors()
    {
        LOGGER.debug("GET /connectors");
        return apiFileHandler.getFileNameList();
    }

    @GET
    @Path("/{file}")
    @Produces({MediaType.TEXT_PLAIN})
    @ApiOperation(value = "Get connectors by file")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Retrieved file", response = String.class),
            @ApiResponse(code = 404, message = "Resource not found")})
    public synchronized Response getConnectorFile(
            @PathParam("file") @ApiParam("The name of a file") String file)
    {
        LOGGER.debug("GET /connectors/{}", file);
        return apiFileHandler.getFile(file);
    }

    @POST
    @Path("/{file}")
    @ApiOperation(value = "Replace this file with the file at the given URL")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 409, message = "Request conflicts with current state")})
    public synchronized Response setConnectorFileByURL(
            @PathParam("file") @ApiParam("The name of a file") String file,
            String url)
    {
        LOGGER.debug("POST /connectors/{}", file);
        return apiFileHandler.replaceFileFromURL(file, url);
    }

    @DELETE
    @Path("/{file}")
    @ApiOperation(value = "Delete a connector file")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 404, message = "Resource not found"),
            @ApiResponse(code = 409, message = "Request conflicts with current state")})
    public synchronized Response deleteConnectorFile(
            @PathParam("file") @ApiParam("The name of a file") String file)
    {
        LOGGER.debug("DELETE /connectors/{}", file);
        return apiFileHandler.deleteFile(file);
    }

    @GET
    @Path("/{file}/{property}")
    @Produces({MediaType.TEXT_PLAIN})
    @ApiOperation(value = "Get specific connector property")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Retrieved property", response = String.class),
            @ApiResponse(code = 404, message = "Resource not found")})
    public synchronized Response getConnectorProperty(
            @PathParam("file") @ApiParam("The name of a file") String file,
            @PathParam("property") @ApiParam("A specific property") String property)
    {
        LOGGER.debug("GET /connectors/{}/{}", file, property);
        return apiFileHandler.getFileProperty(file, property);
    }
}
