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
import io.airlift.log.Logger;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
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
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

@Path("/package")
@Api(description = "API to install, uninstall, upgrade Presto")
@Singleton
// TODO: Add configuration to get package type from PM config file
public final class PackageAPI
{
    private static final Logger LOGGER = Logger.get(PackageAPI.class);

    private static final PackageController CONTROLLER = new RpmController();

    @PUT
    @Consumes(TEXT_PLAIN)
    @Produces(TEXT_PLAIN)
    @ApiOperation(value = "Install Presto using rpm or tarball")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 400, message = "Invalid url"),
            @ApiResponse(code = 409, message = "Presto is already installed.")
    })
    public synchronized Response install(@ApiParam("Url to fetch package") String packageUrl,
            @QueryParam("checkDependencies") @DefaultValue("true") @ApiParam("If false, disables dependency checking") boolean checkDependencies)
    {
        return CONTROLLER.install(packageUrl, checkDependencies);
    }

    @POST
    @Consumes(TEXT_PLAIN)
    @Produces(TEXT_PLAIN)
    @ApiOperation(value = "Upgrade Presto")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 400, message = "Invalid url"),
            @ApiResponse(code = 409, message = "Presto is running. Please stop Presto before beginning upgrade.")
    })
    public synchronized Response upgrade(@ApiParam("Url to fetch package") String packageUrl,
            @QueryParam("checkDependencies") @DefaultValue("true") @ApiParam("If false, disables dependency checking") boolean checkDependencies,
            @QueryParam("preserveConfig") @DefaultValue("true") @ApiParam("If false, config files are not preserved") boolean preserveConfig,
            @QueryParam("forceUpgrade") @DefaultValue("false") @ApiParam("If true, warnings are ignored during upgrade") boolean forceUpgrade)
    {
        return CONTROLLER.upgrade(packageUrl, checkDependencies, preserveConfig, forceUpgrade);
    }

    @DELETE
    @Produces(TEXT_PLAIN)
    @ApiOperation(value = "Uninstall Presto")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 404, message = "Presto is not installed"),
            @ApiResponse(code = 409, message = "Presto is running. Please stop Presto before beginning uninstall.")
    })
    public synchronized Response uninstall(
            @QueryParam("checkDependencies") @DefaultValue("true") @ApiParam("If false, disables dependency checking") boolean checkDependencies,
            @QueryParam("forceUninstall") @DefaultValue("false") @ApiParam("If true, warnings are ignored during uninstall") boolean forceUninstall)
    {
        return CONTROLLER.uninstall(checkDependencies, forceUninstall);
    }
}
