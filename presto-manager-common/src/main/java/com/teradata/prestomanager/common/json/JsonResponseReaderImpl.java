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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

public class JsonResponseReaderImpl
        implements JsonResponseReader
{
    // This should never be modified in this class.
    private final ObjectMapper mapper;

    @Inject
    JsonResponseReaderImpl(@JsonResponseMapper ObjectMapper mapper)
    {
        this.mapper = requireNonNull(mapper).copy();
    }

    @Override
    public Object read(Response response)
            throws IOException, JsonParseException, JsonMappingException
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
}