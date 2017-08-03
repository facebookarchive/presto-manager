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
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;

/**
 * Binder for adding dynamic properties to announcements.
 * <p>
 * Must be used alongside DynamicAnnouncementModule.
 * <p>
 * Acts as an extension to the {@link Binder} EDSL.
 * <pre>{@code
 * dynamicAnnouncementBinder(binder)
 *     .forService("service type")
 *     .bindProperty("property name")
 *     .toClassFromGetter(InjectedClass.class,
 *                        InjectedClass::getterMethod);
 * }</pre>
 */
public class DynamicAnnouncementBinder
{
    /*
       Any value present in keyBinder must be present as a key in either
       supplierBinder or both objectBinder and getterBinder.

       All keys in supplierBinder or objectBinder/getterBinder must be present
       in keyBinder.

       No key in supplierBinder may be present in objectBinder/getterBinder.
     */
    // These four binders be modified very carefully:
    private final Multibinder<Entry<String, String>> keyBinder;
    private final MapBinder<Entry<String, String>, Object> objectBinder;
    private final MapBinder<Entry<String, String>, Function<Object, String>> getterBinder;
    /*
       objectBinder and getterBinder are combined to create bindings in a
       Map<String, Map<String, Supplier<String>>>.

       The keys of these maps hold the keys of the outer and inner maps as
       their keys and values, respectively.

       Any key present in either map _must_ be present in both maps.

       The function injected into functionBinder for each key will be applied
       to the instance in objectBinder with the same key to form the Suppliers
       used as values in the dynamic properties map.
     */

    private static final TypeLiteral<Entry<String, String>> SERVICE_PROPERTY_TYPE
            = new TypeLiteral<Entry<String, String>>() {};

    protected DynamicAnnouncementBinder(Binder binder)
    {
        keyBinder = newSetBinder(binder,
                SERVICE_PROPERTY_TYPE,
                ForDynamicAnnouncements.class);
        getterBinder = newMapBinder(binder,
                SERVICE_PROPERTY_TYPE,
                new TypeLiteral<Function<Object, String>>() {},
                ForDynamicAnnouncements.class);
        objectBinder = newMapBinder(binder,
                SERVICE_PROPERTY_TYPE,
                new TypeLiteral<Object>() {},
                ForDynamicAnnouncements.class);
    }

    public static DynamicAnnouncementBinder dynamicAnnouncementBinder(Binder binder)
    {
        return new DynamicAnnouncementBinder(requireNonNull(binder));
    }

    /**
     * Prepare a binder to bind dynamic properties for the given service type
     */
    public ServicePropertyBinder forService(String type)
    {
        return new ServicePropertyBinder(requireNonNull(type,
                "null service type in dynamic announcement"));
    }

    public class ServicePropertyBinder
    {
        private String type;

        private ServicePropertyBinder(String type)
        {
            this.type = type;
        }

        /**
         * Begin a binding for a dynamic property with the given name
         * <p>
         * Calling this method without calling a method on the returned
         * {@link ServicePropertyValueBinder} will result in an error.
         */
        public ServicePropertyValueBinder bindProperty(String name)
        {
            return new ServicePropertyValueBinder(this, type,
                    requireNonNull(name, "null property name in dynamic announcement"));
        }
    }

    public class ServicePropertyValueBinder
    {
        private ServicePropertyBinder parent;
        private Entry<String, String> key;
        private LinkedBindingBuilder<Function<Object, String>> activeGetterBinder;

        private ServicePropertyValueBinder(ServicePropertyBinder parent, String type, String property)
        {
            this.parent = parent;
            this.key = entry(type, property);

            // Begin creating the bindings
            keyBinder.addBinding().toInstance(key);
            this.activeGetterBinder = getterBinder.addBinding(key);
        }

        @SuppressWarnings("unchecked")
        private <T> LinkedBindingBuilder<T> bindGetterInternal(Function<? super T, String> getter)
        {
            activeGetterBinder.toInstance((Function<Object, String>) getter);
            return (LinkedBindingBuilder<T>) objectBinder.addBinding(key);
        }

        /**
         * Create a binding for the future value of the dynamic property.
         * <p>
         * The class and getter will be injected into the dynamic properties
         * bindings. Each time the property is needed, the given getter will be
         * applied to the injected instance of the class to determine the value
         * of the property.
         * <p>
         * @return A {@link ServicePropertyBinder} that may be used to add more
         * properties
         */
        public <T> ServicePropertyBinder toClassFromGetter(Class<? extends T> clazz, Function<? super T, String> getter)
        {
            bindGetterInternal(getter).to(clazz);
            return parent;
        }

        /**
         * Identical to {@link #toClassFromGetter(Class, Function)}, but may
         * use any binding key to bind the class.
         */
        public <T> ServicePropertyBinder toKeyFromGetter(Key<? extends T> clazz, Function<? super T, String> getter)
        {
            bindGetterInternal(getter).to(clazz);
            return parent;
        }
    }

    /* Static utility methods */

    private static <K, V> Entry<K, V> entry(K key, V value)
    {
        return new SimpleImmutableEntry<>(
                requireNonNull(key,  "null service type in dynamic announcement"),
                requireNonNull(value, "null property name in dynamic announcement"));
    }
}
