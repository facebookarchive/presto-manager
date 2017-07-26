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
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class NodeSet
{
    private static final Logger LOG = Logger.get(NodeSet.class);

    private Set<Agent> nodeSet = ImmutableSet.of();
    private ServiceSelector serviceSelector;

    @Inject
    public NodeSet(
            @ServiceType("presto-manager") ServiceSelector serviceSelector)
    {
        this.serviceSelector = serviceSelector;
    }

    public Collection<URI> getUrisByIds(Collection<UUID> ids)
    {
        refreshNodes();
        Collection<URI> uriList = nodeSet.stream()
                .filter(agent -> ids.contains(agent.getId()))
                .map(Agent::getUri).collect(Collectors.toList());
        if (ids.size() != uriList.size()) {
            throw new IllegalArgumentException("Invalid or duplicate node ID");
        }
        else {
            return uriList;
        }
    }

    public Collection<URI> getUris(ApiScope scope)
    {
        refreshNodes();
        switch (scope) {
            case CLUSTER:
                return nodeSet.stream()
                        .map(Agent::getUri).collect(Collectors.toList());
            case COORDINATOR:
                return nodeSet.stream().filter(Agent::isCoordinator)
                        .map(Agent::getUri).collect(Collectors.toList());
            case WORKERS:
                return nodeSet.stream().filter(Agent::isWorker)
                        .map(Agent::getUri).collect(Collectors.toList());
            default:
                return null;
        }
    }

    public synchronized void refreshNodes()
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
                nodeSet = ImmutableSet.of(); // Don't keep an outdated or partial nodeSet
                LOG.warn(e, "Invalid URI '%s' provided by node with ID '%s'",
                        properties.get("http"), id.toString());
                /* TODO: Map this exception to a specific response from the server with...
                 * ExceptionMapper, or change it to a WebApplicationException containing
                 * a Response (maybe via a utility class). If using ExceptionMapper, be
                 * careful not to catch other DiscoveryExceptions (i.e. change this one).
                 */
                throw new DiscoveryException("", e);
            }
            Agent agent = new Agent(uri, isCoordinator, isWorker, id);
            setBuilder.add(agent);
        }

        nodeSet = setBuilder.build();
    }

    public static class Agent
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
