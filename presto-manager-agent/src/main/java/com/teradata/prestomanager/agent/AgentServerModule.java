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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.teradata.prestomanager.agent.api.ConfigAPI;
import com.teradata.prestomanager.agent.api.ConnectorsAPI;
import com.teradata.prestomanager.agent.api.ControlAPI;
import com.teradata.prestomanager.agent.api.LogsAPI;
import com.teradata.prestomanager.agent.api.PackageAPI;
import com.teradata.prestomanager.common.InstantConverterProvider;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;

import javax.ws.rs.client.Client;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class AgentServerModule
    extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.disableCircularProxies();

        // buildConfigObject also calls bindConfig
        AgentConfig config = buildConfigObject(AgentConfig.class);
        configBinder(binder).bindConfig(PrestoConfig.class, "presto");

        switch (config.getPackageType()) {
            case RPM:
                binder.bind(PackageController.class).to(RpmController.class)
                        .in(Scopes.SINGLETON);
                break;
            case TARBALL:
                binder.bind(PackageController.class).to(TarController.class)
                        .in(Scopes.SINGLETON);
                break;
            default:
                throw new IllegalArgumentException("Unsupported package type");
        }

        binder.bind(Client.class).to(JerseyClient.class).in(Scopes.SINGLETON);
        binder.bind(LogsHandler.class).in(Scopes.SINGLETON);
        binder.bind(PrestoInformer.class);
        binder.bind(CommandExecutor.class);
        binder.bind(PrestoConfigDeployer.class);

        jaxrsBinder(binder).bind(ConfigAPI.class);
        jaxrsBinder(binder).bind(ConnectorsAPI.class);
        jaxrsBinder(binder).bind(ControlAPI.class);
        jaxrsBinder(binder).bind(LogsAPI.class);
        jaxrsBinder(binder).bind(PackageAPI.class);
        jaxrsBinder(binder).bind(InstantConverterProvider.class);

        discoveryBinder(binder).bindHttpAnnouncement("presto-manager");
    }

    @Provides
    @Singleton
    JerseyClient jerseyClientProvider()
    {
        return JerseyClientBuilder.createClient();
    }
}
