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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.airlift.discovery.client.ServiceAnnouncement;

import java.util.Map;

import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static java.util.Objects.requireNonNull;

public class JsonServiceAnnouncement
{
    private transient ServiceAnnouncement service;

    @JsonCreator
    public JsonServiceAnnouncement(
            @JsonProperty("type") String type,
            @JsonProperty("properties") Map<String, String> properties)
    {
        service = serviceAnnouncement(type)
                .addProperties(properties)
                .build();
    }

    @JsonProperty
    public String getType()
    {
        return service.getType();
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return service.getProperties();
    }

    private JsonServiceAnnouncement(ServiceAnnouncement serviceAnnouncement)
    {
        service = serviceAnnouncement;
    }

    public static JsonServiceAnnouncement copyOf(ServiceAnnouncement service)
    {
        return new JsonServiceAnnouncement(service);
    }

    @JsonIgnore
    public ServiceAnnouncement getServiceAnnouncement()
    {
        return service;
    }

    public static Builder builder(String type)
    {
        return new Builder(type);
    }

    public static class Builder
    {
        private String type;
        private ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        public Builder(String type)
        {
            this.type = type;
        }

        public JsonServiceAnnouncement build()
        {
            return new JsonServiceAnnouncement(type, properties.build());
        }

        public Builder addProperty(String key, String value)
        {
            properties.put(
                    requireNonNull(key),
                    requireNonNull(value));
            return this;
        }

        public Builder addProperties(Map<String, String> properties)
        {
            this.properties.putAll(requireNonNull(properties));
            return this;
        }
    }
}
