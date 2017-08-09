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
import com.google.inject.Inject;
import com.teradata.prestomanager.common.json.JsonResponseReader;

import javax.annotation.Nullable;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import static java.util.Objects.requireNonNull;

public final class ResponseWrapper
{
    private final JsonResponseReader reader;

    @Inject
    private ResponseWrapper(JsonResponseReader reader)
    {
        this.reader = requireNonNull(reader);
    }

    public WrappedResponse wrapResponse(Response response)
    {
        return new WrappedResponse(response.getStatus(),
                response.getStatusInfo().getReasonPhrase(),
                response.getHeaders(),
                parseEntity(response));
    }

    private Object parseEntity(Response response)
    {
        String mediaType = response.getHeaderString("Content-Type");

        if ("application/json".equals(mediaType)) {
            return reader.readIfPossible(response).orElse(null);
        }
        else {
            return response.getEntity();
        }
    }

    public static class WrappedResponse
    {
        private int status;
        private String reasonPhrase;
        @Nullable private Object body;
        private MultivaluedMap<String, Object> headers;

        private WrappedResponse(int status, String reason,
                MultivaluedMap<String, Object> headers, Object body)
        {
            this.status = status;
            this.reasonPhrase = requireNonNull(reason);
            this.headers = requireNonNull(headers);
            this.body = body;
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
        public Object getBody()
        {
            return body;
        }

        @JsonProperty
        public MultivaluedMap<String, Object> getHeaders()
        {
            return headers;
        }
    }
}
