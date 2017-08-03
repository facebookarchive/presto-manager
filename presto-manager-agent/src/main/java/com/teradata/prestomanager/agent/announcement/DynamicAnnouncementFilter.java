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
import java.util.AbstractMap.SimpleImmutableEntry;
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

    @Inject
    DynamicAnnouncementFilter(
            JsonCodec<JsonAnnouncement> announcementCodec,
            @ForDynamicAnnouncements
                    Map<String, Map<String, Supplier<String>>> propertiesMap)
    {
        codec = announcementCodec;

        // Re-collect into immutable nested maps
        ImmutableMap.Builder<String, Map<String, Supplier<String>>> builder =
                ImmutableMap.builder();

        for (Entry<String, Map<String, Supplier<String>>> service
                : propertiesMap.entrySet()) {
            ImmutableMap<String, Supplier<String>> properties = service.getValue()
                    .entrySet().stream()
                    .collect(toImmutableMap(
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
                    .map(e -> entry(e.getKey(), e.getValue().get()))
                    .filter(e -> e.getValue() != null)
                    .collect(toImmutableMap(Map.Entry::getKey, Entry::getValue));

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

    private static <K, V> Entry<K, V> entry(K key, V value)
    {
        return new SimpleImmutableEntry<>(key, value);
    }
}
