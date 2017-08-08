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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.testng.annotations.Test;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

@Test
public class TestBasicFunction
{
    private URI controllerUri;
    private URI coordinatorUri;
    private Client client;

    //TODO: Change to use TESTNG Annotations i.e @beforeClass
    public TestBasicFunction()
            throws URISyntaxException
    {
        controllerUri = new URI("http://localhost:8088");
        coordinatorUri = new URI("http://localhost:8080");
        client = JerseyClientBuilder.createClient();
    }

    //TODO: Replace this hack of checking if presto manager is up
    private boolean testIsRunning()
    {
        Response response;
        try {
            response = queryPrestoStatus();
        }
        catch (ProcessingException e) {
            return false;
        }
        return response.getStatus() == 207;
    }

    @Test
    public void runAll()
            throws InterruptedException
    {
        while (!testIsRunning()) {
            TimeUnit.SECONDS.sleep(1);
        }

        System.out.println("TEST STARTS HERE");

        assertEquals(isInstalled(), "false");

        //Install Presto
        Response response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/package")
                        .queryParam("scope", "ALL")
                        .build())
                .request(MediaType.TEXT_PLAIN)
                .buildPut(Entity.entity("http://teradata-presto.s3.amazonaws.com/travis_build_artifacts" +
                        "/Teradata/presto/0.167-t/8277/presto-server-rpm-0.167-t.x86_64.rpm", MediaType.TEXT_PLAIN))
                .invoke();
        assertEquals(response.getStatus(), 207);
        while (!isInstalled()) {
            TimeUnit.SECONDS.sleep(1);
        }
        assertEquals(isRunning(), "false");

        System.out.println("INSTALL TEST PASSED!");

        //Start Presto
        response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/presto/start")
                        .queryParam("scope", "ALL")
                        .build())
                .request(MediaType.TEXT_PLAIN)
                .buildPost(null)
                .invoke();
        assertEquals(response.getStatus(), 207);
        while (!isRunning()) {
            TimeUnit.SECONDS.sleep(1);
        }
        assertEquals(isInstalled(), "true");

        System.out.println("START TEST PASSED!");

        //Run Query
        //Send POST to /v1/statement with query in the body
        response = client
                .target(UriBuilder
                        .fromUri(coordinatorUri)
                        .path("/v1/statement")
                        .queryParam("scope", "COORDINATOR")
                        .build())
                .request(MediaType.TEXT_PLAIN)
                .buildPut(Entity.entity("select count(*) from tpch.tiny.nation", MediaType.TEXT_PLAIN))
                .invoke();
        assertEquals(response.getStatus(), 200);
        //Get the next URI
        JsonParser jsonParser = new JsonParser();
        JsonObject queryResult = jsonParser.parse(response.readEntity(String.class)).getAsJsonObject();
        String nextUri = queryResult.get("nextUri").getAsString();
        response = client
                .target(UriBuilder
                        .fromUri(nextUri)
                        .build())
                .request(MediaType.TEXT_PLAIN)
                .buildGet()
                .invoke();
        //Get the data
        JsonObject data = jsonParser.parse(response.readEntity(String.class)).getAsJsonObject();
        assertEquals(data.getAsJsonArray("data").get(0), 25);

        System.out.println("QUERY TEST PASSED!");

        //Modify Configuration and check with GET
        response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/config/config.properties/query.max-memory")
                        .queryParam("scope", "ALL")
                        .build())
                .request(MediaType.TEXT_PLAIN)
                .buildPut(Entity.entity("2GB", MediaType.TEXT_PLAIN))
                .invoke();
        assertEquals(response.getStatus(), 207);

        response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/config/config.properties/query.max-memory")
                        .queryParam("scope", "ALL")
                        .build())
                .request(MediaType.TEXT_PLAIN)
                .buildGet()
                .invoke();
        assertEquals(response.getStatus(), 207);
        assertEquals(response.readEntity(String.class), "2GB");

        System.out.println("CONFIGURATION TEST PASSED!");

        //Uninstall Presto, Failed attempt
        assertEquals(isRunning(), "true");
        response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/package")
                        .queryParam("scope", "ALL")
                        .build())
                .request(MediaType.TEXT_PLAIN)
                .buildDelete()
                .invoke();
        assertEquals(response.getStatus(), 207);
        assertEquals(isRunning(), "true");

        //Stop Presto
        response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/presto/stop")
                        .queryParam("stopType", "TERMINATE")
                        .queryParam("scope", "ALL")
                        .build())
                .request(MediaType.TEXT_PLAIN)
                .buildPost(null)
                .invoke();
        assertEquals(response.getStatus(), 207);
        while (isRunning()) {
            TimeUnit.SECONDS.sleep(1);
        }

        //Try Uninstall again
        response = client
                .target(UriBuilder
                        .fromUri(controllerUri)
                        .path("/package")
                        .queryParam("scope", "ALL")
                        .build())
                .request(MediaType.TEXT_PLAIN)
                .buildDelete()
                .invoke();
        assertEquals(response.getStatus(), 207);
        while (isInstalled()) {
            TimeUnit.SECONDS.sleep(1);
        }
        assertEquals(isRunning(), "false");

        System.out.println("UNINSTALL TEST PASSED!");
        System.out.println("Basic Function Test Passed!");
    }

    private boolean isInstalled()
    {
        for (String status : readJsonProperty(queryPrestoStatus())) {
            if (status.contains("\"installed\":false")) {
                return false;
            }
        }
        return true;
    }

    private boolean isRunning()
    {
        for (String status : readJsonProperty(queryPrestoStatus())) {
            if (status.equals("\"running\":false")) {
                return false;
            }
        }
        return true;
    }

    private List<String> readJsonProperty(Response response)
    {
        JsonParser jsonParser = new JsonParser();
        JsonObject prestoStatus = jsonParser.parse(response.readEntity(String.class)).getAsJsonObject();

        //Hack the list of nodeIDs
        List<String> nodeIdList = new ArrayList<>();
        Set<Map.Entry<String, JsonElement>> entrySet = prestoStatus.entrySet();
        for (Map.Entry<String, JsonElement> entry : entrySet) {
            nodeIdList.add(entry.getKey());
        }

        List<String> statusList = new ArrayList<>();
        for (String nodeId : nodeIdList) {
            statusList.add(prestoStatus.getAsJsonObject(nodeId).get("entity").getAsString());
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
                .request(MediaType.APPLICATION_JSON)
                .buildGet()
                .invoke();
    }
}
