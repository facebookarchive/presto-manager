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

    public Server build()
    {
        URI uri = UriBuilder.fromUri(this.uri).port(port).build();
        return JettyHttpContainerFactory.createServer(uri, resourceConfig, false);
    }

    public ServerBuilder setPort(int port)
    {
        this.port = port;
        return this;
    }

    public ServerBuilder setURI(String uri)
    {
        this.uri = uri;
        return this;
    }

    /**
     * Register a component class with the underlying {@link ResourceConfig}
     *
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
