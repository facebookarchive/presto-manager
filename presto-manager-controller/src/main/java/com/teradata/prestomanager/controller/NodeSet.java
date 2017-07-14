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

import com.google.inject.Inject;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class NodeSet
{
    private Set<Agent> nodeSet = ConcurrentHashMap.newKeySet();

    @Inject
    public NodeSet() {}

    public void addAgent(URI uri, boolean isCoordinator, boolean isWorker)
    {
        nodeSet.add(new Agent(uri, isCoordinator, isWorker));
    }

    public void removeAgent(int id)
    {
        nodeSet.removeIf(agent -> agent.getId().equals(id));
    }

    public Collection<URI> getUrisByIds(Collection<Integer> ids)
    {
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
        switch (scope) {
            case CLUSTER:
                return nodeSet.stream().map(Agent::getUri).collect(Collectors.toList());
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

    //TODO: Add method to generate ID
    public static class Agent
    {
        private final URI uri;
        private static final AtomicInteger count = new AtomicInteger(0);
        private final int id;
        private final boolean isCoordinator;
        private final boolean isWorker;

        private Agent(URI uri, boolean isCoordinator, boolean isWorker)
        {
            this.uri = requireNonNull(uri, "uri is null");
            this.isCoordinator = isCoordinator;
            this.isWorker = isWorker;

            this.id = count.incrementAndGet();
        }

        private URI getUri()
        {
            return uri;
        }

        private Integer getId()
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
