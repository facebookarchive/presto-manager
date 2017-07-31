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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.discovery.client.DiscoveryException;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collector;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DiscoveryAgentMap
    implements AgentMap
{
    private static final Logger LOG = Logger.get(DiscoveryAgentMap.class);

    private Set<Agent> agentSet = ImmutableSet.of();
    private ServiceSelector serviceSelector;

    @Inject
    public DiscoveryAgentMap(
            @ServiceType("presto-manager") ServiceSelector serviceSelector)
    {
        this.serviceSelector = serviceSelector;
    }

    @Override
    public Map<UUID, URI> getUrisByIds(Collection<UUID> ids)
    {
        refreshNodes();
        Map<UUID, URI> uriMap = agentSet.stream()
                .filter(agent -> ids.contains(agent.getId()))
                .collect(toIdUriMap());
        if (ids.size() != uriMap.size()) {
            throw new IllegalArgumentException("Invalid or duplicate node ID");
        }
        else {
            return uriMap;
        }
    }

    @Override
    public Map<UUID, URI> getAllUris()
    {
        refreshNodes();
        return agentSet.stream()
                .collect(toIdUriMap());
    }

    @Override
    public Map<UUID, URI> getCoordinatorUris()
    {
        refreshNodes();
        return agentSet.stream()
                .filter(Agent::isCoordinator)
                .collect(toIdUriMap());
    }

    @Override
    public Map<UUID, URI> getWorkerUris()
    {
        refreshNodes();
        return agentSet.stream()
                .filter(Agent::isWorker)
                .collect(toIdUriMap());
    }

    private synchronized void refreshNodes()
    {
        List<ServiceDescriptor> services = serviceSelector.selectAllServices();

        ImmutableSet.Builder<Agent> setBuilder = ImmutableSet.builder();

        for (ServiceDescriptor service : services) {
            Map<String, String> properties = service.getProperties();
            // TODO: Make agents start without an ID, and provide one on first discovery
            UUID id = service.getId();
            boolean isCoordinator = Boolean.parseBoolean(properties.get("presto-coordinator"));
            boolean isWorker = Boolean.parseBoolean(properties.get("presto-worker"));
            URI uri;
            try {
                uri = new URI(properties.get("http")); // TODO: allow https
            }
            catch (URISyntaxException e) {
                agentSet = ImmutableSet.of(); // Don't keep an outdated or partial set of agents
                LOG.warn(e, "Invalid URI '%s' provided by node with ID '%s'",
                        properties.get("http"), id.toString());
                /* TODO: Map this exception to a specific response from the server with...
                 * ExceptionMapper, or change it to a WebApplicationException containing
                 * a Response (maybe via a utility class). If using ExceptionMapper, be
                 * careful not to catch other DiscoveryExceptions (i.e. change this one).
                 */
                throw new DiscoveryException(
                        String.format("Invalid URI '%s' for node with ID '%s'",
                                properties.get("http"), id), e);
            }
            Agent agent = new Agent(uri, isCoordinator, isWorker, id);
            // TODO: Check for duplicate IDs
            setBuilder.add(agent);
        }

        agentSet = setBuilder.build();
    }

    private static Collector<Agent, ?, ? extends Map<UUID, URI>> toIdUriMap()
    {
        return ImmutableMap.toImmutableMap(Agent::getId, Agent::getUri);
    }

    // TODO: Reorder constructor parameters
    private static class Agent
    {
        private URI uri;
        private UUID id;
        private boolean isCoordinator;
        private boolean isWorker;

        private Agent(URI uri, boolean isCoordinator, boolean isWorker, UUID id)
        {
            this.uri = requireNonNull(uri, "uri is null");
            this.isCoordinator = isCoordinator;
            this.isWorker = isWorker;
            this.id = requireNonNull(id, "null agent id");
        }

        private URI getUri()
        {
            return uri;
        }

        private UUID getId()
        {
            return id;
        }

        private boolean isCoordinator()
        {
            return isCoordinator;
        }

        private boolean isWorker()
        {
            return isWorker;
        }

        @Override
        public boolean equals(Object obj)
        {
            return (obj instanceof Agent) && (getId().equals(((Agent) obj).getId()));
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(id);
        }
    }
}
