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
import com.teradata.prestomanager.agent.PrestoCommand;
import com.teradata.prestomanager.agent.PrestoInstaller;
import com.teradata.prestomanager.agent.PrestoManagerException;
import com.teradata.prestomanager.agent.PrestoUninstaller;
import com.teradata.prestomanager.agent.PrestoUpgrader;
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

import java.net.MalformedURLException;
import java.net.URL;

import static com.teradata.prestomanager.agent.PackageApiUtils.isInstalled;
import static com.teradata.prestomanager.agent.PackageApiUtils.isRunning;
import static com.teradata.prestomanager.agent.PackageType.RPM;
import static com.teradata.prestomanager.agent.PrestoRpmController.terminatePresto;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Path("/package")
@Api(description = "API to install, uninstall, upgrade Presto")
@Singleton
// TODO: Add configuration to get package type from PM config file
public final class PackageAPI
{
    private static final Logger LOGGER = Logger.get(PackageAPI.class);

    @PUT
    @Consumes(TEXT_PLAIN)
    @Produces(TEXT_PLAIN)
    @ApiOperation(value = "Install Presto using rpm or tarball")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 400, message = "Invalid url"),
            @ApiResponse(code = 409, message = "Presto is already installed.")
    })
    public synchronized Response install(@ApiParam("Url to fetch package") String urlToFetchPackage,
            @QueryParam("checkDependencies") @DefaultValue("true") @ApiParam("If false, disables dependency checking") boolean checkDependencies)
    {
        if ("".equals(urlToFetchPackage)) {
            LOGGER.error("Url is empty or null");
            return Response.status(BAD_REQUEST).entity("Expected URL in the request body").build();
        }
        try {
            if (isInstalled()) {
                LOGGER.error("Presto is already installed.");
                return Response.status(CONFLICT).entity("Presto is already installed.").build();
            }
            URL url = new URL(urlToFetchPackage);
            new Thread(new PrestoAsynchronousCommand(new PrestoInstaller(RPM, url, checkDependencies))).start();
        }
        catch (PrestoManagerException e) {
            LOGGER.error(e, "Failed to ascertain whether presto is already installed.");
            return Response.status(INTERNAL_SERVER_ERROR).entity("Failed to ascertain whether presto is already installed.").build();
        }
        catch (MalformedURLException e) {
            LOGGER.error(e, "Invalid url: %s", urlToFetchPackage);
            return Response.status(BAD_REQUEST).entity("Invalid url").build();
        }
        return Response.status(ACCEPTED).entity("Presto is being installed.\r\n" +
                "To verify that installation succeeded, check back later using the status API.").build();
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
    public synchronized Response upgrade(@ApiParam("Url to fetch package") String urlToFetchPackage,
            @QueryParam("checkDependencies") @DefaultValue("true") @ApiParam("If false, disables dependency checking") boolean checkDependencies,
            @QueryParam("preserveConfig") @DefaultValue("true") @ApiParam("If false, config files are not preserved") boolean preserveConfig,
            @QueryParam("forceUpgrade") @DefaultValue("false") @ApiParam("If true, warnings are ignored during upgrade") boolean forceUpgrade)
    {
        if ("".equals(urlToFetchPackage)) {
            LOGGER.error("Url is empty or null");
            return Response.status(BAD_REQUEST).entity("Expected URL in the request body").build();
        }
        try {
            if (isRunning()) {
                if (!forceUpgrade) {
                    LOGGER.error("Presto is running. Stop Presto before beginning upgrade.");
                    return Response.status(CONFLICT).entity("Presto is running. Stop Presto before beginning upgrade.").build();
                }
                LOGGER.warn("Presto is running; Presto will be forcibly stopped before attempting to upgrade.");
                terminatePresto();
            }
            URL url = new URL(urlToFetchPackage);
            new Thread(new PrestoAsynchronousCommand(new PrestoUpgrader(RPM, url, checkDependencies, preserveConfig))).start();
        }
        catch (PrestoManagerException e) {
            LOGGER.error(e, "Failed to ascertain whether presto is running");
            return Response.status(INTERNAL_SERVER_ERROR).entity("Failed to ascertain whether presto is running").build();
        }
        catch (MalformedURLException e) {
            LOGGER.error(e, "Invalid url: %s", urlToFetchPackage);
            return Response.status(BAD_REQUEST).entity("Invalid url").build();
        }
        return Response.status(ACCEPTED).entity("Presto is being upgraded.\r\n" +
                "To verify that upgrade succeeded, check back later using the status API.").build();
    }

    @DELETE
    @Produces({TEXT_PLAIN})
    @ApiOperation(value = "Uninstall Presto")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request")
    })
    public synchronized Response uninstall(
            @QueryParam("checkDependencies") @DefaultValue("true") @ApiParam("If false, disables dependency checking") boolean checkDependencies,
            @QueryParam("forceUninstall") @DefaultValue("false") @ApiParam("If true, warnings are ignored during uninstall") boolean forceUninstall)
    {
        new Thread(new PrestoAsynchronousCommand(new PrestoUninstaller(RPM, checkDependencies, forceUninstall))).start();
        return Response.status(ACCEPTED).entity("Presto is being uninstalled.\r\n" +
                "To verify that uninstallation succeeded, check back later using the status API.").build();
    }

    public class PrestoAsynchronousCommand
            implements Runnable
    {
        PrestoCommand command;

        public PrestoAsynchronousCommand(PrestoCommand command)
        {
            this.command = command;
        }

        @Override
        public void run()
        {
            try {
                command.runCommand();
            }
            catch (PrestoManagerException e) {
                LOGGER.error(e.getCause(), e.getMessage());
            }
            catch (Exception e) {
                LOGGER.error(e, "Unhandled exception");
            }
        }
    }
}
