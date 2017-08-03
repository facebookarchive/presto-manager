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
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

public class DynamicAnnouncementModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        httpClientBinder(binder).addGlobalFilterBinding()
                .to(DynamicAnnouncementFilter.class)
                .asEagerSingleton();

        jsonCodecBinder(binder).bindJsonCodec(JsonAnnouncement.class);

        // Initialize various multibinders
        DynamicAnnouncementBinder.dynamicAnnouncementBinder(binder);
    }

    @Provides
    @Singleton
    @ForDynamicAnnouncements
    public Map<String, Map<String, Supplier<String>>> getDynamicProperties(
            @ForDynamicAnnouncements Set<Entry<String, String>> serviceProperties,
            @ForDynamicAnnouncements Map<Entry<String, String>, Object> instances,
            @ForDynamicAnnouncements Map<Entry<String, String>, Function<Object, String>> getters)
    {
        requireNonNull(serviceProperties, "null service property set injected");

        if (!serviceProperties.equals(instances.keySet()) || !serviceProperties.equals(getters.keySet())) {
            // This should be impossible as long as DynamicAnnouncementBinder is internally correct
            throw new Error("Mismatch between dynamic property keys in properties set and MapBinders");
        }

        // This will contain the dynamic properties
        Map<String, Map<String, Supplier<String>>> dynamicProperties = new HashMap<>();

        for (Entry<String, String> property : serviceProperties) {
            // ditto about the requireNonNulls
            String type = requireNonNull(property.getKey());
            String prop = requireNonNull(property.getValue());
            Function<Object, String> getter = requireNonNull(getters.get(property));
            Object instance = requireNonNull(instances.get(property));

            try {
                getter.apply(instance);
            }
            catch (ClassCastException e) {
                throw new Error("Inconsistent types in dynamic announcement MapBinders", e);
            }

            Map<String, Supplier<String>> service =
                    dynamicProperties.computeIfAbsent(type, __ -> new HashMap<>());

            Supplier<String> last = service.put(prop, () -> getter.apply(instance));
            if (last != null) {
                throw new Error("Duplicate property in service");
            }
        }

        // Make the nested maps all immutable
        return dynamicProperties.entrySet().stream()
                .collect(toImmutableMap(
                        Entry::getKey,
                        e -> ImmutableMap.copyOf(e.getValue())));
    }
}
