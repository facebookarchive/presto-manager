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

import com.google.inject.Singleton;
import com.teradata.prestomanager.agent.PackageController;
import com.teradata.prestomanager.agent.RpmController;
import com.teradata.prestomanager.common.StopType;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

@Path("/presto")
@Api(description = "API to start, stop, restart Presto and get Presto status")
@Singleton
public class ControlAPI
{
    private static final PackageController CONTROLLER = new RpmController();

    @POST
    @Path("/start")
    @Produces(TEXT_PLAIN)
    @ApiOperation(value = "Start Presto")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 404, message = "Presto is not installed")})
    public synchronized Response startPresto()
    {
        return CONTROLLER.start();
    }

    @POST
    @Path("/stop")
    @ApiOperation(value = "Stop Presto")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully stopped Presto"),
            @ApiResponse(code = 404, message = "Presto is not installed"),
            @ApiResponse(code = 409, message = "Coordinator can't be gracefully stopped")})
    public synchronized Response stopPresto(@QueryParam("stopType") @ApiParam("StopType: TERMINATE, KILL or GRACEFUL")
    @DefaultValue("GRACEFUL") StopType stopType)
    {
        return CONTROLLER.stop(stopType);
    }

    @POST
    @Path("/restart")
    @ApiOperation(value = "Restart Presto")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully restarted Presto"),
            @ApiResponse(code = 404, message = "Presto is not installed")})
    public synchronized Response restartPresto()
    {
        return CONTROLLER.restart();
    }

    @GET
    @Path("/status")
    @Produces(APPLICATION_JSON)
    @ApiOperation(value = "Get Presto status")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Presto status")})
    public synchronized Response prestoStatus()
    {
        return CONTROLLER.status();
    }
}
