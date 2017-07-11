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

import com.teradata.prestomanager.agent.PrestoCommand;
import com.teradata.prestomanager.agent.PrestoInstaller;
import com.teradata.prestomanager.agent.PrestoUninstaller;
import com.teradata.prestomanager.agent.PrestoUpgrader;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
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

import static com.google.common.io.Files.getFileExtension;
import static javax.ws.rs.client.Entity.text;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/package")
@Api(description = "the package API")
@Singleton
public final class PackageAPI
{
    private static PackageType installedPackageType;

    @PUT
    @Consumes({"text/plain"})
    @Produces({"text/plain"})
    @ApiOperation(value = "Install presto using rpm or tarball")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 400, message = "Bad package format or Invalid url")
    })
    public synchronized Response install(@NotNull @ApiParam("Url to fetch package") String locationToFetchPackage,
            @QueryParam("checkDependencies") @DefaultValue("true") @ApiParam("If false, disables dependency checking") boolean checkDependencies)
    {
        try {
            installedPackageType = PackageType.valueOf(getFileExtension(locationToFetchPackage).toUpperCase());
        }
        catch (IllegalArgumentException e) {
            return Response.status(BAD_REQUEST).entity(text("Invalid package type for presto-server. Package must be .tar.gz or .rpm")).build();
        }
        try {
            URL url = new URL(locationToFetchPackage);
            new Thread(new PrestoAsynchronousCommand(new PrestoInstaller(installedPackageType, url, checkDependencies))).start();
        }
        catch (MalformedURLException e) {
            return Response.status(BAD_REQUEST).entity(text("Invalid url")).build();
        }
        return Response.status(ACCEPTED).entity(text("Check status to make sure that presto is installed")).build();
    }

    @POST
    @Consumes({"text/plain"})
    @Produces({"text/plain"})
    @ApiOperation(value = "Upgrade presto")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request"),
            @ApiResponse(code = 400, message = "Bad package format or Invalid url")
    })
    public synchronized Response upgrade(@NotNull @ApiParam("Url to fetch package") String locationToFetchPackage,
            @QueryParam("checkDependencies") @DefaultValue("true") @ApiParam("If false, disables dependency checking") boolean checkDependencies)
    {
        try {
            if (installedPackageType != PackageType.valueOf(getFileExtension(locationToFetchPackage).toUpperCase())) {
                return Response.status(BAD_REQUEST).entity(text("Provided package format doesn't match the installed package format")).build();
            }
        }
        catch (IllegalArgumentException e) {
            return Response.status(BAD_REQUEST).entity(text("Invalid package type for presto-server. Package must be .tar.gz or .rpm")).build();
        }
        try {
            final URL url = new URL(locationToFetchPackage);
            new Thread(new PrestoAsynchronousCommand(new PrestoUpgrader(installedPackageType, url, checkDependencies))).start();
        }
        catch (MalformedURLException e) {
            return Response.status(BAD_REQUEST).entity(text("Invalid url")).build();
        }
        return Response.status(ACCEPTED).entity(text("Check status to make sure that presto is upgraded")).build();
    }

    @DELETE
    @ApiOperation(value = "Uninstall presto")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Acknowledged request")
    })
    public synchronized Response uninstall(
            @QueryParam("checkDependencies") @DefaultValue("true") @ApiParam("If false, disables dependency checking") boolean checkDependencies,
            @QueryParam("ignoreErrors") @DefaultValue("false") @ApiParam("If true, warnings are ignored during uninstall") boolean ignoreErrors)
    {
        new Thread(new PrestoAsynchronousCommand(new PrestoUninstaller(installedPackageType, checkDependencies, ignoreErrors))).start();
        return Response.status(ACCEPTED).entity(text("Presto is being uninstalled")).build();
    }

    public enum PackageType
    {
        RPM, TARBALL;
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
            catch (Exception e) {
                // TODO: Add to logger
            }
        }
    }
}
