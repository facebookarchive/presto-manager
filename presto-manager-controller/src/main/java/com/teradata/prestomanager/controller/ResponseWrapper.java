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
package com.teradata.prestomanager.controller;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import static java.util.Objects.requireNonNull;

public final class ResponseWrapper
{
    private int status;
    private String reasonPhrase;
    private String entity;
    private MultivaluedMap<String, Object> headers;

    private ResponseWrapper(Response response)
    {
        this.status = requireNonNull(response.getStatus());
        this.reasonPhrase = requireNonNull(response.getStatusInfo().getReasonPhrase());
        this.entity = response.hasEntity() ? response.readEntity(String.class) : null;
        this.headers = response.getHeaders().isEmpty() ? null : response.getHeaders();
    }

    public static ResponseWrapper wrapResponse(Response response)
    {
        return new ResponseWrapper(response);
    }

    @JsonProperty
    public int getStatus()
    {
        return status;
    }

    @JsonProperty
    public String getReasonPhrase()
    {
        return reasonPhrase;
    }

    @JsonProperty
    public String getEntity()
    {
        return entity;
    }

    @JsonProperty
    public MultivaluedMap<String, Object> getHeaders()
    {
        return headers;
    }
}
