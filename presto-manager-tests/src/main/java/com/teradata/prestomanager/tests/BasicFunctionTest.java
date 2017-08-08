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
package com.teradata.prestomanager.tests;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BasicFunctionTest
{
    private URI controllerUri;
    private URI coordinatorUri;
    private Client client;
    private int numPrestoNodes;

    @BeforeClass
    public void setUp()
            throws URISyntaxException, IOException, InterruptedException, TimeoutException, ExecutionException
    {
        controllerUri = new URI("http://localhost:8088");
        coordinatorUri = new URI("http://localhost:8080");
        client = JerseyClientBuilder.createClient();
        numPrestoNodes = 2;
        assertWithRetry(this::checkPrestoManager, 1, 300);
        assertTrue(isNotInstalled());
    }

    @Test
    public void runBasicFunctionTest()
            throws Exception
    {
        installPrestoTest();
        startPrestoTest();
        queryExecutionTest();
        terminatePrestoTest();
        uninstallPrestoTest();
    }

    //TODO: Replace this hack of checking if presto manager is up
    // and discovery service has kicked in
    private boolean checkPrestoManager()
    {
        List<JsonObject> statusList;
        try {
            statusList = parseStatusResponse(queryPrestoStatus());
        }
        catch (ProcessingException e) {
            return false;
        }
        return statusList.size() == numPrestoNodes;
    }

    private void installPrestoTest()
            throws Exception
    {
        Response response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/package")
                        .queryParam("scope", "ALL")
                        .build())
                .request(TEXT_PLAIN)
                .buildPut(Entity.entity("http://teradata-presto.s3.amazonaws.com/travis_build_artifacts" +
                        "/Teradata/presto/0.167-t/8277/presto-server-rpm-0.167-t.x86_64.rpm", TEXT_PLAIN))
                .invoke();

        assertEquals(response.getStatus(), 207);
        assertWithRetry(this::isInstalled, 10, 1200);
        assertTrue(isNotRunning());
    }

    private void startPrestoTest()
            throws Exception
    {
        Response response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/presto/start")
                        .queryParam("scope", "ALL")
                        .build())
                .request(TEXT_PLAIN)
                .buildPost(Entity.text(""))
                .invoke();

        assertEquals(response.getStatus(), 207);
        assertWithRetry(this::isRunning, 1, 300);
    }

    private void queryExecutionTest()
            throws Exception
    {
        assertWithRetry(this::queryTest, 5, 1000);
    }

    private boolean queryTest()
    {
        JsonArray queryResult = runQuery(client, "select count(*) from nation", "tiny", "tpch");
        if (queryResult.size() == 1 && queryResult.get(0).getAsInt() == 25) {
            return true;
        }
        else {
            return false;
        }
    }

    private JsonArray runQuery(Client client, String sql, String schema, String catalog)
    {
        //Send POST to /v1/statement with query in the body and appropriate headers
        Response response = client
                .target(UriBuilder
                        .fromUri(coordinatorUri)
                        .path("/v1/statement")
                        .build())
                .request(APPLICATION_JSON)
                .header("X-Presto-User", "localhost")
                .header("X-Presto-Schema", schema)
                .header("X-Presto-Source", "presto-manager")
                .header("X-Presto-Catalog", catalog)
                .buildPost(Entity.text(sql))
                .invoke();

        assertEquals(response.getStatus(), 200);

        // Send GET requests to the server using the 'nextUri' from the
        // previous response until the servers response does not contain
        // anymore 'nextUri's.  When there is no 'nextUri' the query is finished
        JsonParser jsonParser = new JsonParser();
        JsonObject queryResponse = jsonParser.parse(response.readEntity(String.class)).getAsJsonObject();
        JsonArray queryResult = new JsonArray();
        while (queryResponse.has("nextUri")) {
            if (queryResponse.has("data")) {
                queryResult.addAll(queryResponse.getAsJsonArray("data"));
            }
            String nextUri = queryResponse.get("nextUri").getAsString();
            response = client
                    .target(UriBuilder
                            .fromUri(nextUri)
                            .build())
                    .request(APPLICATION_JSON)
                    .buildGet()
                    .invoke();

            queryResponse = jsonParser.parse(response.readEntity(String.class)).getAsJsonObject();
        }

        if (queryResponse.has("data")) {
            queryResult.addAll(queryResponse.getAsJsonArray("data"));
        }
        return queryResult;
    }

    private void terminatePrestoTest()
            throws Exception
    {
        Response response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/presto/stop")
                        .queryParam("stopType", "TERMINATE")
                        .queryParam("scope", "ALL")
                        .build())
                .request(TEXT_PLAIN)
                .buildPost(Entity.text(""))
                .invoke();

        assertEquals(response.getStatus(), 207);
        assertWithRetry(this::isNotRunning, 1, 300);
    }

    private void uninstallPrestoTest()
            throws Exception
    {
        Response response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/package")
                        .queryParam("scope", "ALL")
                        .build())
                .request(TEXT_PLAIN)
                .buildDelete()
                .invoke();

        assertEquals(response.getStatus(), 207);
        assertWithRetry(this::isNotInstalled, 1, 300);
    }

    private boolean isInstalled()
    {
        return checkStatus("installed", true);
    }

    private boolean isNotInstalled()
    {
        return !checkStatus("installed", false);
    }

    private boolean isRunning()
    {
        return checkStatus("running", true);
    }

    private boolean isNotRunning()
    {
        return !checkStatus("running", false);
    }

    private boolean checkStatus(String entry, boolean expected)
    {
        List<JsonObject> statusList = parseStatusResponse(queryPrestoStatus());
        if (statusList.size() != numPrestoNodes) {
            return !expected;
        }
        for (JsonObject status : statusList) {
            if (status.get(entry).getAsBoolean() != expected) {
                return !expected;
            }
        }
        return expected;
    }

    private List<JsonObject> parseStatusResponse(Response response)
    {
        JsonParser jsonParser = new JsonParser();
        JsonObject prestoStatus = jsonParser.parse(response.readEntity(String.class)).getAsJsonObject();

        // Get the list of nodeIDs
        List<String> nodeIdList = new ArrayList<>();
        for (Map.Entry<String, JsonElement> entry : prestoStatus.entrySet()) {
            nodeIdList.add(entry.getKey());
        }

        List<JsonObject> statusList = new ArrayList<>();
        for (String nodeId : nodeIdList) {
            statusList.add(prestoStatus.getAsJsonObject(nodeId).getAsJsonObject("body"));
        }
        return statusList;
    }

    private Response queryPrestoStatus()
    {
        return client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/presto/status")
                        .queryParam("scope", "ALL")
                        .build())
                .request(APPLICATION_JSON)
                .buildGet()
                .invoke();
    }

    private void assertWithRetry(BooleanSupplier supplier, int retryTime, int timeout)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeout);
        while (System.currentTimeMillis() < endTime && !Thread.currentThread().isInterrupted()) {
            if (supplier.getAsBoolean()) {
                return;
            }
            else {
                sleepUninterruptibly(retryTime, SECONDS);
            }
        }
        throw new AssertionError("assertWithRetry failed due to timeout");
    }
}
