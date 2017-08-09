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
package com.teradata.prestomanager.common.json;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Class for reading the entities of {@link Response Responses} into Java
 * objects suitable to re-serialize in an outgoing {@link Response}
 */
public class JsonResponseReader
{
    // This should never be modified in this class.
    private final ObjectMapper mapper;

    @Inject
    JsonResponseReader(@JsonResponseMapper ObjectMapper mapper)
    {
        this.mapper = requireNonNull(mapper).copy();
    }

    /**
     * Convert the entity of the given response into a Java object.
     * <p>
     * If the response's entity is an object besides an {@link InputStream}, it
     * will be returned as is. If the entity is an {@link InputStream}, it will
     * be converted to a Java Object as if it were in JSON.
     *
     * @throws JsonParseException If the input stream was not in JSON format.
     */
    public Object read(Response response)
            throws IOException
    {
        if (!response.hasEntity()) {
            return null;
        }

        Object rawEntity = response.getEntity();
        Object entity;
        if (rawEntity instanceof InputStream) {
            InputStream entityInput = (InputStream) rawEntity;
            entity = mapper.readValue(entityInput, Object.class);
        }
        else {
            entity = rawEntity;
        }
        return entity;
    }

    /**
     * As {@link #read(Response)}, but the result is wrapped in an
     * {@link Optional}. If an error occurs or the JSON value is null, the
     * empty {@code Optional} is returned.
     */
    public Optional<Object> readIfPossible(Response response)
    {
        try {
            return Optional.ofNullable(read(response));
        }
        catch (IOException e) {
            return Optional.empty();
        }
    }
}
