package com.teradata.prestomanager.agent;

import com.teradata.prestomanager.common.ServerBuilder;
import org.eclipse.jetty.server.Server;

import java.net.BindException;

class AgentServer
{
    private static final int PORT = 8081;
    private static final String URI = "http://localhost/";

    public static void main(String[] args) throws Exception
    {
        ServerBuilder sb = new ServerBuilder();
        sb.setPort(8081);
        sb.setURI("http://localhost/");
        // sb.registerComponent(Resource.class);
        Server server = sb.build();

        try {
            server.start();
        }
        catch (BindException e) {
            e.printStackTrace();
            System.out.println("BindException: " + e.getMessage());
            return;
        }
        try {
            server.join();
        }
        finally {
            System.out.println("Stopping");
            server.stop();
        }
    }
}
