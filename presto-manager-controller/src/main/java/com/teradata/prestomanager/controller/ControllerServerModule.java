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
package com.teradata.prestomanager.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.teradata.prestomanager.common.InstantConverterProvider;
import com.teradata.prestomanager.controller.api.ControllerConfigAPI;
import com.teradata.prestomanager.controller.api.ControllerConnectorAPI;
import com.teradata.prestomanager.controller.api.ControllerControlAPI;
import com.teradata.prestomanager.controller.api.ControllerLogsAPI;
import com.teradata.prestomanager.controller.api.ControllerPackageAPI;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;

import javax.ws.rs.client.Client;

import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class ControllerServerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.disableCircularProxies();

        binder.bind(AgentMap.class).to(NodeSet.class).in(Scopes.SINGLETON);
        binder.bind(RequestDispatcher.class).in(Scopes.SINGLETON);
        binder.bind(Client.class).to(JerseyClient.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(ControllerConfigAPI.class);
        jaxrsBinder(binder).bind(ControllerConnectorAPI.class);
        jaxrsBinder(binder).bind(ControllerLogsAPI.class);
        jaxrsBinder(binder).bind(ControllerPackageAPI.class);
        jaxrsBinder(binder).bind(ControllerControlAPI.class);
        jaxrsBinder(binder).bind(InstantConverterProvider.class);

        discoveryBinder(binder).bindSelector("presto-manager");
    }

    @Provides
    @Singleton
    public JerseyClient jerseyClientProvider()
    {
        return JerseyClientBuilder.createClient();
    }

    @Provides
    @Singleton
    @ForController
    public ObjectMapper objectMapper()
    {
        // TODO: Don't enable the pretty printer ("INDENT_OUTPUT")
        return new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .enable(SerializationFeature.CLOSE_CLOSEABLE);
    }
}
