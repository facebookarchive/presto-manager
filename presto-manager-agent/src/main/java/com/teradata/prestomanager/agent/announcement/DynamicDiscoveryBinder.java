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

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceAnnouncement.ServiceAnnouncementBuilder;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static java.util.Objects.requireNonNull;

/**
 * Binder for binding announcements with dynamic properties
 * <p>
 * Based on {@link io.airlift.discovery.client.DiscoveryBinder}.
 */
public class DynamicDiscoveryBinder
{
    private final Binder binder;
    private final MapBinder<String, Entry<String, Supplier<String>>> propertiesBinder;

    protected DynamicDiscoveryBinder(Binder binder)
    {
        this.binder = binder;
        propertiesBinder = newMapBinder(binder, // String.class, DynamicProperty.class,
                new TypeLiteral<String>() {},
                new TypeLiteral<Entry<String, Supplier<String>>>() {},
                DynamicAnnouncementProperties.class).permitDuplicates();
        // This will also bind to Map<String, Set<Entry<~>>>
    }

    public static DynamicDiscoveryBinder dynamicDiscoveryBinder(Binder binder)
    {
        return new DynamicDiscoveryBinder(requireNonNull(binder));
    }

    /**
     * Bind a single dynamic property without creating an announcement
     */
    public DynamicDiscoveryBinder bindDynamicAnnouncementProperty(
            String serviceType, String propertyName, Supplier<String> valueSupplier)
    {
        this.propertiesBinder.addBinding(serviceType)
                .toInstance(createEntry(propertyName, valueSupplier));
        return this;
    }

    /*
       These two methods mirror the "bindAnnouncement" methods in DiscoveryBinder
       that have parameters that expose the service's type.
     */

    public DynamicServiceBuilder bindDynamicHttpAnnouncement(String type)
    {
        return new DynamicServiceBuilder(type,
                discoveryBinder(binder).bindHttpAnnouncement(type));
    }

    public DynamicPropertyBinder bindDynamicServiceAnnouncement(
            ServiceAnnouncement service)
    {
        requireNonNull(service);
        discoveryBinder(binder).bindServiceAnnouncement(service);
        return new DynamicPropertyBinder(service.getType());
    }

    /* Static utility methods */

    private static Entry<String, Supplier<String>> createEntry(
            String key, Supplier<String> value)
    {
        return new SimpleImmutableEntry<>(requireNonNull(key), requireNonNull(value));
    }

    private static Entry<String, Supplier<String>> createEntry(
            Entry<String, Supplier<String>> entry)
    {
        return new SimpleImmutableEntry<>(entry.getKey(), entry.getValue());
    }

    /* Static nested classes; utilities for constructing Service announcements */

    public class DynamicPropertyBinder
    {
        private final String type;

        protected DynamicPropertyBinder(String type)
        {
            requireNonNull(type);
            this.type = type;
        }

        public DynamicPropertyBinder addDynamicProperty(
                String key, Supplier<String> valueSupplier)
        {
            propertiesBinder.addBinding(type)
                    .toInstance(createEntry(key, valueSupplier));
            return this;
        }

        public DynamicPropertyBinder addDynamicProperties(
                Map<String, Supplier<String>> dynamicProperties)
        {
            requireNonNull(dynamicProperties);
            if (dynamicProperties.containsKey(null)
                    || dynamicProperties.containsValue(null)) {
                throw new NullPointerException("null dynamic property name or value");
            }
            for (Entry<String, Supplier<String>> property
                    : dynamicProperties.entrySet()) {
                propertiesBinder.addBinding(type).toInstance(createEntry(property));
            }
            return this;
        }
    }

    /**
     * This class provides the capabilties of {@link ServiceAnnouncementBuilder}
     * in addition to those of {@link DynamicPropertyBinder}, so both static and
     * dynamic service properties may be bound.
     * <p>
     * Static properties must be added before dynamic properties.
     */
    public class DynamicServiceBuilder
            extends DynamicPropertyBinder
    {
        private final ServiceAnnouncementBuilder serviceAnnouncementBuilder;

        protected DynamicServiceBuilder(String type, ServiceAnnouncementBuilder builder)
        {
            super(type);
            serviceAnnouncementBuilder = builder;
        }

        public DynamicServiceBuilder addProperty(String key, String value)
        {
            serviceAnnouncementBuilder.addProperty(key, value);
            return this;
        }

        public DynamicServiceBuilder addProperties(Map<String, String> properties)
        {
            serviceAnnouncementBuilder.addProperties(properties);
            return this;
        }
    }
}
