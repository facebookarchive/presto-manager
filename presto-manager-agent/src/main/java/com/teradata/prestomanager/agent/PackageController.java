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
package com.teradata.prestomanager.agent;

import com.google.common.collect.ImmutableMap;
import com.teradata.prestomanager.common.StopType;
import com.teradata.prestomanager.common.json.JsonResponseReader;
import io.airlift.log.Logger;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.UriBuilder.fromUri;

public abstract class PackageController
{
    private static final Logger LOGGER = Logger.get(PackageController.class);

    private final Client client;
    private final JsonResponseReader responseReader;
    private final PrestoInformer informer;

    protected PackageController(Client client,
            JsonResponseReader responseReader, PrestoInformer informer)
    {
        this.client = requireNonNull(client);
        this.responseReader = requireNonNull(responseReader);
        this.informer = requireNonNull(informer);
    }

    public Response install(String packageUrl, boolean checkDependencies)
    {
        if ("".equals(packageUrl)) {
            LOGGER.error("Url is empty or null");
            return Response.status(BAD_REQUEST).entity("Expected URL in the request body").build();
        }
        try {
            if (isInstalled()) {
                LOGGER.error("Presto is already installed.");
                return Response.status(CONFLICT).entity("Presto is already installed.").build();
            }
            URL url = new URL(packageUrl);
            PrestoAsyncCommand(() -> installAsync(url, checkDependencies));
        }
        catch (MalformedURLException e) {
            LOGGER.error(e, "Invalid url: %s", packageUrl);
            return Response.status(BAD_REQUEST).entity("Invalid url").build();
        }
        catch (PrestoManagerException e) {
            LOGGER.error(e, "Failed to ascertain whether presto is already installed.");
            return javax.ws.rs.core.Response.status(INTERNAL_SERVER_ERROR).entity("Failed to ascertain whether presto is already installed.").build();
        }
        return javax.ws.rs.core.Response.status(ACCEPTED).entity("Presto is being installed.\r\n" +
                "To verify that installation succeeded, check back later using the status API.").build();
    }

    public Response uninstall(boolean checkDependencies, boolean forceUninstall)
    {
        try {
            if (!isInstalled()) {
                LOGGER.error("Presto is not installed");
                return Response.status(NOT_FOUND).entity("Presto is not installed").build();
            }
            if (isRunning()) {
                if (!forceUninstall) {
                    LOGGER.error("Presto is running. Stop Presto before beginning upgrade.");
                    return Response.status(CONFLICT).entity("Presto is running. Stop Presto before beginning upgrade.").build();
                }
                LOGGER.warn("Presto is running; Presto will be forcibly stopped before attempting to uninstall.");
                terminate();
            }
        }
        catch (PrestoManagerException e) {
            LOGGER.error(e, "Failed to ascertain whether presto is already installed or running.");
            return Response.status(INTERNAL_SERVER_ERROR).entity("Failed to ascertain whether presto is already installed or running.").build();
        }
        PrestoAsyncCommand(() -> uninstallAsync(checkDependencies));
        return Response.status(ACCEPTED).entity("Presto is being uninstalled.\r\n" +
                "To verify that uninstallation succeeded, check back later using the status API.").build();
    }

    public Response upgrade(String packageUrl, boolean checkDependencies, boolean preserveConfig, boolean forceUpgrade)
    {
        if ("".equals(packageUrl)) {
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
                terminate();
            }
            URL url = new URL(packageUrl);
            PrestoAsyncCommand(() -> upgradeAsync(url, checkDependencies, preserveConfig));
        }
        catch (MalformedURLException e) {
            LOGGER.error(e, "Invalid url: %s", packageUrl);
            return Response.status(BAD_REQUEST).entity("Invalid url").build();
        }
        catch (PrestoManagerException e) {
            LOGGER.error(e, "Failed to ascertain whether presto is running");
            return Response.status(INTERNAL_SERVER_ERROR).entity("Failed to ascertain whether presto is running").build();
        }
        return Response.status(ACCEPTED).entity("Presto is being upgraded.\r\n" +
                "To verify that upgrade succeeded, check back later using the status API.").build();
    }

    public Response start()
    {
        try {
            if (!isInstalled()) {
                LOGGER.error("Presto is not installed");
                return Response.status(NOT_FOUND).entity("Presto is not installed").build();
            }
        }
        catch (PrestoManagerException e) {
            LOGGER.error(e.getCause(), e.getMessage());
            return Response.status(INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
        PrestoAsyncCommand(this::startAsync);
        return Response.status(ACCEPTED).entity("Presto is being started.\r\n" +
                "To verify that Presto has started, check back later using the status API.").build();
    }

    public Response stop(StopType stopType)
    {
        try {
            gracefulStop();
            if (!isInstalled()) {
                LOGGER.error("Presto is not installed");
                return Response.status(NOT_FOUND).entity("Presto is not installed").build();
            }
            if (!isRunning()) {
                LOGGER.info("Presto is not running");
                return Response.status(OK).entity("Presto is not running").build();
            }
            switch (stopType) {
                case TERMINATE:
                    terminate();
                    break;
                case KILL:
                    kill();
                    break;
                case GRACEFUL:
                    if (informer.isRunningCoordinator()) {
                        LOGGER.error("Coordinator can't be gracefully stopped.");
                        return Response.status(CONFLICT).entity("Coordinator can't be gracefully stopped").build();
                    }
                    gracefulStop();
                    break;
                default:
                    LOGGER.error("Invalid stop type: %s", stopType);
                    return Response.status(INTERNAL_SERVER_ERROR).entity("Invalid stop type").build();
            }
        }
        catch (PrestoManagerException e) {
            LOGGER.error(e.getCause(), e.getMessage());
            return Response.status(INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
        catch (IOException e) {
            LOGGER.error(e, "Failed to get Presto port number");
            return Response.status(INTERNAL_SERVER_ERROR).entity("Failed to get Presto port number").build();
        }
        return Response.status(OK).entity("Presto successfully stopped").build();
    }

    public void gracefulStop()
            throws PrestoManagerException
    {
        try {
            URI uri = fromUri("http://localhost").port(8000) // informer.getPrestoPort())
                    .path("/v1/info/state").build();
            Response response = client.target(uri).request(TEXT_PLAIN)
                    .buildPut(entity("SHUTTING_DOWN", APPLICATION_JSON)).invoke();
            if (response.getStatus() != 200) {
                throw new PrestoManagerException("Failed to stop presto gracefully");
            }
        }
        catch (ProcessingException e) {
            LOGGER.warn(e, "Presto is not running");
        }
    }

    public Response restart()
    {
        try {
            if (!isInstalled()) {
                LOGGER.error("Presto is not installed");
                return Response.status(NOT_FOUND).entity("Presto is not installed").build();
            }
            PrestoAsyncCommand(this::restartAsync);
        }
        catch (PrestoManagerException e) {
            LOGGER.error(e.getCause(), e.getMessage());
            return Response.status(INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
        return Response.status(OK).entity("Presto successfully restarted").build();
    }

    /**
     * The status api can be used to ascertain whether presto is installed and running.
     * If Presto is installed, this method returns a JSON file containing Presto version
     * If Presto is running, this method returns a JSON file containing the following details
     * nodeVersion : Presto version installed in the node
     * environment : The name of the environment
     * coordinator : Specifies whether the current node functions as a coordinator
     * state : The state of the node. ACTIVE (or) SHUTTING_DOWN
     *
     * @return Response containing node status
     */
    public Response status()
    {
        String prestoVersion;
        try {
            if (!isInstalled()) {
                LOGGER.info("Presto is not installed");
                return Response.status(OK).entity(ImmutableMap.of("installed", false)).type(APPLICATION_JSON)
                        .type(APPLICATION_JSON).build();
            }
            prestoVersion = getVersion();
        }
        catch (PrestoManagerException e) {
            LOGGER.error(e.getCause(), e.getMessage());
            return Response.status(INTERNAL_SERVER_ERROR).entity(e.getMessage()).type(APPLICATION_JSON).build();
        }
        // TODO: Check isRunning() here
        try {
            UriBuilder uriBuilder;
            try {
                uriBuilder = fromUri("http://localhost").port(informer.getPrestoPort()).path("/v1/info");
            }
            catch (IOException e) {
                LOGGER.error(e, "Could not get Presto port");
                return Response.status(INTERNAL_SERVER_ERROR).entity("Could not get Presto port")
                        .type(APPLICATION_JSON).build();
            }
            // TODO: GET a JSON from that URI to get the real Presto version and other info
            Response prestoInfo = client.target(uriBuilder.build()).request(APPLICATION_JSON).buildGet().invoke();
            Response prestoState = client.target(uriBuilder.path("/state").build()).request(APPLICATION_JSON).buildGet().invoke();

            ImmutableMap.Builder<String, Object> jsonBuilder = ImmutableMap.builder();

            responseReader.readIfPossible(prestoState)
                    .ifPresent(o -> jsonBuilder.put("state", o));
            responseReader.readIfPossible(prestoInfo)
                    .ifPresent(o -> jsonBuilder.put("ServerInfo", o));

            jsonBuilder.put("version", prestoVersion);
            jsonBuilder.put("installed", true);
            jsonBuilder.put("running", true);
            return Response.status(OK).entity(jsonBuilder.build()).type(APPLICATION_JSON).build();
        }
        catch (ProcessingException e) {
            // TODO: Change this message; this exception means that invoking...
            // the request had an exception, not that Presto is not running
            LOGGER.warn(e, "Presto is not running or response could not be read");
            Map<String, Object> statusMap = ImmutableMap.of(
                    "installed", true,
                    "running", false,
                    "version", prestoVersion);
            return Response.status(OK).entity(statusMap).type(APPLICATION_JSON).build();
        }
    }

    private void PrestoAsyncCommand(PrestoRunnable runnable)
    {
        new Thread(() -> {
            try {
                runnable.run();
            }
            catch (PrestoManagerException e) {
                LOGGER.error(e.getCause(), e.getMessage());
            }
        }).start();
    }

    protected abstract void installAsync(URL url, boolean checkDependencies)
            throws PrestoManagerException;

    protected abstract void uninstallAsync(boolean checkDependencies)
            throws PrestoManagerException;

    protected abstract void upgradeAsync(URL url, boolean checkDependencies, boolean preserveConfig)
            throws PrestoManagerException;

    protected abstract void startAsync()
            throws PrestoManagerException;

    protected abstract void terminate()
            throws PrestoManagerException;

    protected abstract void kill()
            throws PrestoManagerException;

    protected abstract void restartAsync()
            throws PrestoManagerException;

    protected abstract String getVersion()
            throws PrestoManagerException;

    protected abstract boolean isInstalled()
            throws PrestoManagerException;

    protected abstract boolean isRunning()
            throws PrestoManagerException;
}
