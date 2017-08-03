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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.ws.rs.ext.Provider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;

@Provider
class DynamicAnnouncementFilter
        implements HttpRequestFilter
{
    private static final Logger LOGGER = Logger.get(DynamicAnnouncementFilter.class);

    private static final String ANNOUNCEMENT_PATH = "/v1/announcement/";

    private final JsonCodec<JsonAnnouncement> codec;
    /**
     * A map of service types to property/valueSupplier pairs.
     * <p>
     * The values in this map are the dynamic properties for each service.
     */
    private final Map<String, Map<String, Supplier<String>>> propertiesSuppliers;

    /**
     * Container class to allow optional injection into
     * {@link #DynamicAnnouncementFilter(JsonCodec, Map, PropertySetHolder)}
     */
    static class PropertySetHolder
    {
        @Inject(optional = true)
        @DynamicAnnouncementProperties
        private Set<DynamicProperty> propertiesSet = ImmutableSet.of();
    }

    /*
    Both the map and set will be used to construct dynamic announcements.

    The map should be constructed using DynamicDiscoveryBinder's MapBinder.
    The set may be @Provided manually.

    Ideally, entries in the Map could be provided with @ProvidesIntoMap, but
    that annotation doesn't seem to work.
     */
    @Inject
    DynamicAnnouncementFilter(
            JsonCodec<JsonAnnouncement> announcementCodec,
            @DynamicAnnouncementProperties
                    Map<String, Set<Entry<String, Supplier<String>>>> propertiesMap,
            PropertySetHolder propertySetHolder)
    {
        codec = announcementCodec;

        // Collect the properties from the set and the map into one collection.
        Map<String, Set<Entry<String, Supplier<String>>>> allProperties = new HashMap<>();

        for (DynamicProperty property : propertySetHolder.propertiesSet) {
            allProperties
                    .computeIfAbsent(property.getType(), __ -> new HashSet<>())
                    .add(property.getPropertyEntry());
        }
        for (Entry<String, Set<Entry<String, Supplier<String>>>> service
                : propertiesMap.entrySet()) {
            allProperties
                    .computeIfAbsent(service.getKey(), __ -> new HashSet<>())
                    .addAll(service.getValue());
        }

        // Re-collect into immutable nested maps without duplicate sub-keys
        ImmutableMap.Builder<String, Map<String, Supplier<String>>> builder =
                ImmutableMap.builder();

        for (Entry<String, Set<Entry<String, Supplier<String>>>> service
                : allProperties.entrySet()) {
            ImmutableMap<String, Supplier<String>> properties = service.getValue()
                    .stream().collect(toImmutableMap(
                            Entry::getKey,
                            Entry::getValue,
                            (v1, v2) -> {
                                throw new IllegalArgumentException(
                                        "Service has two dynamic properties with the same name");
                            }));
            builder.put(service.getKey(), properties);
        }
        this.propertiesSuppliers = builder.build();
    }

    @Override
    public Request filterRequest(Request request)
    {
        if (isAnnouncement(request.getUri())) {
            try {
                return rewriteRequest(request);
            }
            catch (Exception e) {
                LOGGER.error(e, "Could not add dynamic service properties to announcement; "
                        + "Original announcement used");
                return request;
            }
        }
        else {
            return request;
        }
    }

    private Request rewriteRequest(Request request)
            throws IOException, Exception
    {
        Announcement original = readBodyGenerator(request.getBodyGenerator());

        Set<JsonServiceAnnouncement> services = original.getServices().stream()
                .map(this::updateAnnouncement)
                .collect(ImmutableSet.toImmutableSet());

        // TODO: Memoize the rewritten requests
        JsonAnnouncement updated = new JsonAnnouncement(
                original.getEnvironment(),
                original.getNodeId(),
                original.getPool(),
                original.getLocation(),
                services);

        return new Request(
                request.getUri(),
                request.getMethod(),
                request.getHeaders(),
                jsonBodyGenerator(codec, updated));
    }

    private Announcement readBodyGenerator(BodyGenerator bodyGenerator)
            throws IOException, Exception
    {
        ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
        try {
            bodyGenerator.write(bytesStream);
        }
        catch (Exception e) {
            // getBodyGenerator() should return a JsonBodyGenerator,
            // which should only throw IOException
            LOGGER.error(e, (e instanceof IOException)
                    ? "IOException writing Request body to ByteArrayOutputStream"
                    : "Unexpected exception writing Request body to ByteArrayOutputStream");
            throw e;
        }
        try {
            return codec.fromJson(bytesStream.toByteArray());
        }
        catch (IllegalArgumentException e) {
            Throwable cause = e.getCause();
            // fromJson(byte[]) wraps IOExceptions in a useless IllegalArgumentException
            if (cause instanceof IOException) {
                LOGGER.error(cause, "Failed to deserialize announcement from Request body");
                throw (IOException) cause;
            }
            else {
                LOGGER.error(e, "Exception deserializing announcement from Request body");
                throw e;
            }
        }
    }

    private JsonServiceAnnouncement updateAnnouncement(ServiceAnnouncement service)
    {
        final String isCoordinator = String.valueOf(true);

        String type = service.getType();

        if (propertiesSuppliers.containsKey(type)) {
            Map<String, String> dynamicProperties = propertiesSuppliers.get(type)
                    .entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue().get()));

            return JsonServiceAnnouncement.builder(type)
                    .addProperties(service.getProperties())
                    .addProperties(dynamicProperties)
                    .addProperty("presto-coordinator", isCoordinator)
                    .build();
        }
        else {
            return JsonServiceAnnouncement.copyOf(service);
        }
    }

    private static boolean isAnnouncement(URI uri)
    {
        String path = uri.getPath();
        return path != null && path.startsWith(ANNOUNCEMENT_PATH);
    }
}
