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

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Class to allow simpler construction of Dynamic property sets that
 * may easily be converted to maps
 * <p>
 * <pre> {@code
 * Set<DynamicProperty> properties = ImmutableSet.builder()
 *     .add(new DynamicProperty(type, key, value))
 *     .add(new DynamicProperty(type, key, value))
 *     .build();
 * } </pre>
 * <p>
 * Such a set may be provided to {@link DynamicAnnouncementFilter} with the
 * binding annotation {@link DynamicAnnouncementProperties}.
 */
@Immutable
public class DynamicProperty
{
    @NotNull private final String type;
    @NotNull private final String key;
    @NotNull private final Supplier<String> value;

    private DynamicProperty(String type, String key, Supplier<String> value)
    {
        this.type = requireNonNull(type);
        this.key = requireNonNull(key);
        this.value = requireNonNull(value);
    }

    public static DynamicProperty dynamicProperty(
            String type, String key, Supplier<String> valueSupplier)
    {
        return new DynamicProperty(type, key, valueSupplier);
    }

    String getType()
    {
        return type;
    }

    String getKey()
    {
        return key;
    }

    Supplier<String> getValue()
    {
        return value;
    }

    Entry<String, Supplier<String>> getPropertyEntry()
    {
        return new SimpleImmutableEntry<>(getKey(), getValue());
    }

    @Override
    public String toString()
    {
        return type + ": " + key + "=" + value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof DynamicProperty)) {
            return false;
        }
        DynamicProperty d = (DynamicProperty) o;
        return Objects.equals(type, d.getType())
                && Objects.equals(key, d.getKey())
                && Objects.equals(value, d.getValue());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, key, value);
    }
}
