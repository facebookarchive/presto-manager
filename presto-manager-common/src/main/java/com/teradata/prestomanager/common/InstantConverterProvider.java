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
package com.teradata.prestomanager.common;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;

import static com.teradata.prestomanager.common.SimpleResponses.badRequest;

public class InstantConverterProvider
        implements ParamConverterProvider
{
    public static final String DEFAULT_DATE = "DEFAULT";

    private static final String DATE_PATTERN = "yyyy-MM-dd['T'HH[:]mm[[:]ss[.SSS][.SS][.S]][XX]";

    private static final DateTimeFormatter DATE_FORMAT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive().parseStrict()
            .appendPattern("uuuu-MM-dd['T'HH[:]mm[[:]ss[")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 3, true)
            .appendPattern("]]][XX]")
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
            .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
            .parseDefaulting(ChronoField.OFFSET_SECONDS, 0)
            .toFormatter().withChronology(IsoChronology.INSTANCE);

    @Override
    public <T> ParamConverter<T> getConverter(Class<T> rawType, Type genericType, Annotation[] annotations)
    {
        return Instant.class != rawType ? null : new ParamConverter<T>()
        {
            @Override
            public T fromString(String value)
            {
                try {
                    return rawType.cast(
                            (value == null || DEFAULT_DATE.equalsIgnoreCase(value))
                                    ? null
                                    : DATE_FORMAT.parse(value, Instant::from));
                }
                catch (DateTimeParseException e) {
                    throw new WebApplicationException(badRequest(
                            "Invalid date for format \"%s\"", DATE_PATTERN));
                }
            }

            @Override
            public String toString(T value)
            {
                if (value == null || !Instant.class.isInstance(value)) {
                    return null;
                }
                else {
                    return DATE_FORMAT.format(((Instant) value).atOffset(ZoneOffset.UTC));
                }
            }
        };
    }
}
