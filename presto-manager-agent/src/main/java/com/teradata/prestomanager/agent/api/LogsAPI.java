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

import com.teradata.prestomanager.agent.LogsHandler;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.time.Instant;

@Path("/logs")
@Api(description = "the logs API")
@javax.annotation.Generated(
        value = "io.swagger.codegen.languages.JavaJAXRSSpecServerCodegen",
        date = "2017-06-23T09:53:13.549-04:00")
public class LogsAPI
{
    @GET
    @Produces({MediaType.TEXT_PLAIN})
    @ApiOperation(value = "Get a listing of log files")
    @ApiResponses({@ApiResponse(code = 200, message = "Retrieved file list")})
    public Response getLogList()
    {
        return LogsHandler.getLogList();
    }

    @GET
    @Path("/{file}")
    @Produces({MediaType.TEXT_PLAIN})
    @ApiOperation(value = "Get Presto log file")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Retrieved logs", response = String.class),
            @ApiResponse(code = 400, message = "Invalid parameters"),
            @ApiResponse(code = 404, message = "Resource not found")})
    public Response getLog(
            @PathParam("file") @ApiParam("The name of a file") String file,
            @QueryParam("from") @ApiParam("Ignore logs before this date") Instant fromDate,
            @QueryParam("to") @ApiParam("Ignore logs after this date") Instant toDate,
            @QueryParam("level") @ApiParam("Only get logs of this level") @DefaultValue(LogsHandler.DEFAULT_LOG_LEVEL) String level,
            @QueryParam("n") @ApiParam("The maximum number of log entries to get") Integer maxEntries)
    {
        return LogsHandler.getLogs(file, fromDate, toDate, level, maxEntries);
    }

    @DELETE
    @Path("/{file}")
    @Produces({MediaType.TEXT_PLAIN})
    @ApiOperation(value = "Delete Presto logs")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Deleted logs"),
            @ApiResponse(code = 400, message = "Invalid parameters"),
            @ApiResponse(code = 404, message = "Resource not found")})
    public Response deleteLog(
            @PathParam("file") @ApiParam("The name of a file") String file,
            @QueryParam("to") @ApiParam("Ignore logs after this date") Instant toDate)
    {
        return LogsHandler.deleteLogs(file, toDate);
    }
}
