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
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.teradata.prestomanager.agent.AgentFileUtils.getFileProperty;
import static com.teradata.prestomanager.agent.CommandExecutor.execCommandResult;
import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static java.lang.String.format;
import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.UriBuilder.fromUri;

public class PrestoRpmController
{
    private static final Logger LOGGER = Logger.get(PrestoRpmController.class);
    private static final String LAUNCHER_SCRIPT = "/usr/lib/presto/bin/launcher";
    private static final Path PRESTO_CONFIG_FILE = Paths.get("/etc/presto/config.properties");
    private static final Gson GSON = new Gson();
    private static final int SUBPROCESS_TIMEOUT = 120;

    private final Client client;

    @Inject
    private PrestoRpmController(Client client)
    {
        this.client = client;
    }

    public Response startUsingRpm()
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
        new Thread(() -> {
            try {
                int startPresto = executeCommand(SUBPROCESS_TIMEOUT, "service", "presto", "start");
                if (startPresto != 0) {
                    LOGGER.error("Failed to start presto");
                }
            }
            catch (PrestoManagerException e) {
                LOGGER.error(e.getCause(), e.getMessage());
            }
        }).start();
        return Response.status(ACCEPTED).entity("Presto is being started.\r\n" +
                "To verify that Presto has started, check back later using the status API.").build();
    }

    public Response stopUsingRpm(StopType stopType)
    {
        try {
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
                    terminatePresto();
                    break;
                case KILL:
                    killPresto();
                    break;
                case GRACEFUL:
                    if (isCoordinator()) {
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
        return Response.status(OK).entity("Presto successfully stopped").build();
    }

    public Response restartUsingRpm()
    {
        try {
            if (!isInstalled()) {
                LOGGER.error("Presto is not installed");
                return Response.status(NOT_FOUND).entity("Presto is not installed").build();
            }
            int prestoRestart = executeCommand(SUBPROCESS_TIMEOUT, "service", "presto", "restart");
            if (prestoRestart != 0) {
                throw new PrestoManagerException("Failed to restart presto", prestoRestart);
            }
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
    public Response statusUsingRpm()
    {
        String prestoVersion;
        try {
            if (!isInstalled()) {
                LOGGER.info("Presto is not installed");
                return Response.status(OK).entity(GSON.toJson(ImmutableMap.of("installed", false)))
                        .type(APPLICATION_JSON).build();
            }
            prestoVersion = getVersion();
        }
        catch (PrestoManagerException e) {
            LOGGER.error(e.getCause(), e.getMessage());
            return Response.status(INTERNAL_SERVER_ERROR).entity(GSON.toJson(e.getMessage())).type(APPLICATION_JSON).build();
        }
        try {
            String prestoPort = getPrestoPort();
            UriBuilder uriBuilder = fromUri(format("http://localhost:%s", prestoPort)).path("/v1/info");
            Response prestoInfo = client.target(uriBuilder.build()).request(APPLICATION_JSON).buildGet().invoke();
            Response prestoState = client.target(uriBuilder.path("/state").build())
                    .request(APPLICATION_JSON).buildGet().invoke();
            JsonParser jsonParser = new JsonParser();
            JsonObject prestoStatus = jsonParser.parse(prestoInfo.readEntity(String.class)).getAsJsonObject();
            String version = prestoStatus.getAsJsonObject("nodeVersion").get("version").getAsString();
            prestoStatus.remove("nodeVersion");
            prestoStatus.addProperty("version", version);
            prestoStatus.addProperty("state", jsonParser.parse(prestoState.readEntity(String.class)).getAsString());
            prestoStatus.addProperty("installed", true);
            prestoStatus.addProperty("running", true);
            return Response.status(OK).entity(prestoStatus.toString()).type(APPLICATION_JSON).build();
        }
        catch (ProcessingException e) {
            LOGGER.info("Presto is not running");
            Map<String, Object> statusMap = ImmutableMap.of(
                    "installed", true,
                    "running", false,
                    "version", prestoVersion);
            return Response.status(OK).entity(GSON.toJson(statusMap)).type(APPLICATION_JSON).build();
        }
        catch (IOException e) {
            LOGGER.error(e, "Failed to get status.");
            return Response.status(INTERNAL_SERVER_ERROR).entity(GSON.toJson("Failed to get status"))
                    .type(APPLICATION_JSON).build();
        }
    }

    private static boolean isInstalled()
            throws PrestoManagerException
    {
        return executeCommand("rpm", "-q", "presto-server-rpm") == 0;
    }

    private static boolean isRunning()
            throws PrestoManagerException
    {
        return isInstalled() && executeCommand("service", "presto", "status") == 0;
    }

    private static void terminatePresto()
            throws PrestoManagerException
    {
        int prestoTerminate = executeCommand("service", "presto", "stop");
        if (prestoTerminate != 0) {
            throw new PrestoManagerException("Failed to stop presto", prestoTerminate);
        }
    }

    private void killPresto()
            throws PrestoManagerException
    {
        executeCommand("sudo", LAUNCHER_SCRIPT, "kill");
        int prestoKill = executeCommand("service", "presto", "status");
        if (prestoKill != 3) {
            throw new PrestoManagerException("Failed to stop presto", prestoKill);
        }
    }

    private void gracefulStop()
            throws PrestoManagerException
    {
        try {
            String prestoPort = getPrestoPort();
            UriBuilder uriBuilder = fromUri(format("http://localhost:%s", prestoPort)).path("/v1/info/state");
            Response response = client.target(uriBuilder.build()).request(TEXT_PLAIN)
                    .buildPut(entity(GSON.toJson("SHUTTING_DOWN"), APPLICATION_JSON)).invoke();
            if (response.getStatus() != 200) {
                throw new PrestoManagerException("Failed to stop presto gracefully");
            }
        }
        catch (ProcessingException e) {
            LOGGER.warn(e, "Presto is not running");
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to get Presto port number.", e);
        }
    }

    private static String getPrestoPort()
            throws IOException
    {
        return getFileProperty(PRESTO_CONFIG_FILE, "http-server.http.port");
    }

    private boolean isCoordinator()
            throws PrestoManagerException
    {
        try {
            String prestoPort = getPrestoPort();
            UriBuilder uriBuilder = fromUri(format("http://localhost:%s", prestoPort)).path("/v1/info/coordinator");
            Response isCoordinator = client.target(uriBuilder.build()).request(TEXT_PLAIN).buildGet().invoke();
            return isCoordinator.getStatus() == 200;
        }
        catch (ProcessingException e) {
            LOGGER.warn(e, "Presto is not running");
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to get Presto port number.", e);
        }
        return false;
    }

    private static String getVersion()
            throws PrestoManagerException
    {
        CommandExecutor.CommandResult commandResult = execCommandResult("rpm", "-q", "--qf", "%{VERSION}", "presto-server-rpm");
        if (commandResult.getExitValue() != 0) {
            throw new PrestoManagerException("Failed to retrieve Presto version", commandResult.getExitValue());
        }
        return commandResult.getOutput();
    }
}
