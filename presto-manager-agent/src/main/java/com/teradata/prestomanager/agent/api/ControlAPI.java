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

import com.teradata.prestomanager.agent.StopType;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import static com.teradata.prestomanager.agent.PrestoRpmControlUtils.restartUsingRpm;
import static com.teradata.prestomanager.agent.PrestoRpmControlUtils.startUsingRpm;
import static com.teradata.prestomanager.agent.PrestoRpmControlUtils.statusUsingRpm;
import static com.teradata.prestomanager.agent.PrestoRpmControlUtils.stopUsingRpm;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

@Path("/presto")
@Api(description = "API to start, stop, restart presto and get presto status")
@Singleton
public class ControlAPI
{
    private static final Logger LOGGER = LogManager.getLogger(ControlAPI.class);

    @POST
    @Path("/start")
    @Produces(TEXT_PLAIN)
    @ApiOperation(value = "Start presto")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 404, message = "Presto is not installed"),
            @ApiResponse(code = 500, message = "Failed to start presto")
    })
    public synchronized Response startPresto()
    {
        LOGGER.debug("POST /presto/start");
        return startUsingRpm();
    }

    @POST
    @Path("/stop")
    @ApiOperation(value = "Stop presto")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully stopped presto"),
            @ApiResponse(code = 404, message = "Presto is not installed"),
            @ApiResponse(code = 409, message = "Coordinator can't be gracefully stopped"),
            @ApiResponse(code = 500, message = "Failed to stop presto")
    })
    public synchronized Response stopPresto(@QueryParam("stopType") @ApiParam("StopType: TERMINATE, KILL or GRACEFUL")
    @DefaultValue("GRACEFUL") StopType stopType)
    {
        LOGGER.debug("POST /presto/stop ; stopType: {}", stopType);
        return stopUsingRpm(stopType);
    }

    @POST
    @Path("/restart")
    @ApiOperation(value = "Restart presto")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully restarted presto"),
            @ApiResponse(code = 404, message = "Presto is not installed"),
            @ApiResponse(code = 500, message = "Failed to restart presto")
    })
    public synchronized Response restartPresto()
    {
        LOGGER.debug("POST /presto/restart");
        return restartUsingRpm();
    }

    @GET
    @Path("/status")
    @Produces(APPLICATION_JSON)
    @ApiOperation(value = "Get presto status")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved presto status"),
            @ApiResponse(code = 500, message = "Failed to get presto status")
    })
    public synchronized Response prestoStatus()
    {
        LOGGER.debug("GET /presto/status");
        return statusUsingRpm();
    }
}