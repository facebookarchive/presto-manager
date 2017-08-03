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
package com.teradata.prestomanager.agent.announcement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.ServiceAnnouncement;

import java.util.Set;

public class JsonAnnouncement
        extends Announcement
{
    @JsonCreator
    public JsonAnnouncement(
            @JsonProperty("environment") String environment,
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("pool") String pool,
            @JsonProperty("location") String location,
            @JsonProperty("services") Set<JsonServiceAnnouncement> services)
    {
        super(environment, nodeId, pool, location,
                services.stream()
                        .map(JsonServiceAnnouncement::getServiceAnnouncement)
                        .collect(ImmutableSet.toImmutableSet()));
    }

    @Override
    @JsonProperty
    public String getEnvironment()
    {
        return super.getEnvironment();
    }

    @Override
    @JsonProperty
    public String getNodeId()
    {
        return super.getNodeId();
    }

    @Override
    @JsonProperty
    public String getLocation()
    {
        return super.getLocation();
    }

    @Override
    @JsonProperty
    public String getPool()
    {
        return super.getPool();
    }

    @Override
    @JsonProperty
    public Set<ServiceAnnouncement> getServices()
    {
        return super.getServices();
    }
}
