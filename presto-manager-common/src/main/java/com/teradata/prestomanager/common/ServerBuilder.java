package com.teradata.prestomanager.common;

import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;

import java.net.URI;

/**
 * Class for constructing servers using Jersey and Jetty
 */
public class ServerBuilder
{
    private Integer port;
    private String uri;
    private ResourceConfig resourceConfig = new ResourceConfig();

    public ServerBuilder() {}

    /**
     * Complete construction of the server
     *
     * @return A {@link Server}
     */
    public Server build()
    {
        URI uri = UriBuilder.fromUri(this.uri).port(port).build();
        return JettyHttpContainerFactory.createServer(uri, resourceConfig, false);

    }

    /**
     * Set the port the server should run on
     */
    public void setPort(int port)
    {
        this.port = port;
    }

    /**
     * Set the URI of the server
     */
    public void setURI(String uri)
    {
        this.uri = uri;
    }

    /**
     * Register a component class with the underlying {@link ResourceConfig}
     *b
     * @param componentClass The class to register
     * @return this {@code ServerBuilder}
     */
    public ServerBuilder registerComponent(Class<?> componentClass)
    {
        resourceConfig.register(componentClass);
        return this;
    }

    /**
     * Register multiple component classes with the server's {@link ResourceConfig}
     *
     * @param classes The classes to register
     * @return this {@code ServerBuilder}
     */
    public ServerBuilder registerClasses(Class<?>... classes)
    {
        resourceConfig.registerClasses(classes);
        return this;
    }
}
