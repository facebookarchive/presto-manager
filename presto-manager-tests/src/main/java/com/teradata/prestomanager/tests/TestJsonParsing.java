package com.teradata.prestomanager.tests;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.glassfish.jersey.client.JerseyClientBuilder;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestJsonParsing
{
    private TestJsonParsing() {}

    public static void main(String[] args)
    {
        Client client = JerseyClientBuilder.createClient();
        Response response = client.target(UriBuilder
                .fromUri("http://localhost:8088")
                .path("/presto/status")
                .queryParam("scope", "ALL")
                .build())
                .request(MediaType.APPLICATION_JSON)
                .buildGet()
                .invoke();
        for (String status : readJsonProperty(response, "installed")) {
            System.out.println(status.contains("\"installed\":false"));
        }
    }

    public static List<String> readJsonProperty(Response response, String key)
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
}
