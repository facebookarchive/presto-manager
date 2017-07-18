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

import org.eclipse.jetty.http.HttpMethod;
import org.glassfish.jersey.client.JerseyClient;

import javax.annotation.Nullable;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.concurrent.Future;

import static java.util.Objects.requireNonNull;
import static org.glassfish.jersey.client.JerseyClientBuilder.createClient;

public final class ApiRequester
{
    private final UriBuilder uriBuilder;
    private final HttpMethod method;
    private final MultivaluedMap<String, Object> headers;
    private final String mediaType;
    @Nullable
    private Entity entity;

    private static JerseyClient jerseyClient = createClient();

    private ApiRequester(UriBuilder uriBuilder, HttpMethod method,
            Entity entity, MultivaluedMap<String, Object> headers,
            String mediaType)
    {
        this.uriBuilder = requireNonNull(uriBuilder);
        this.method = requireNonNull(method);
        this.headers = requireNonNull(headers);
        this.mediaType = requireNonNull(mediaType);

        this.entity = entity;
    }

    public Response send(URI uri)
    {
        Invocation invocation = createInvocation(uri);
        return invocation.invoke();
    }

    public Future<Response> sendAsync(URI uri)
    {
        Invocation invocation = createInvocation(uri);
        return invocation.submit();
    }

    private Invocation createInvocation(URI uri)
    {
        Invocation.Builder builder = jerseyClient
                .target(uriBuilder.uri(uri).build())
                .request(mediaType)
                .headers(headers);

        Invocation invocation;
        if (entity == null) {
            invocation = builder.build(method.asString());
        }
        else {
            invocation = builder.build(method.asString(), entity);
        }

        return invocation;
    }

    public static Builder builder(Class<?> resource)
    {
        return new Builder(resource);
    }

    public static class Builder
    {
        private UriBuilder uriBuilder;
        private Class<?> resource;
        private HttpMethod method;
        private Entity entity;
        private MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        private String mediaType = MediaType.TEXT_PLAIN;

        private Builder(Class<?> resource)
        {
            uriBuilder = UriBuilder.fromResource(resource);
        }

        public ApiRequester build()
        {
            return new ApiRequester(
                    uriBuilder, method, entity, headers, mediaType);
        }

        public Builder pathMethod(String method)
        {
            uriBuilder.path(resource, method);
            return this;
        }

        public Builder resolveTemplate(String name, Object value)
        {
            uriBuilder.resolveTemplate(name, value);
            return this;
        }

        public Builder queryParam(String name, Object... values)
        {
            uriBuilder.queryParam(name, values);
            return this;
        }

        public Builder httpMethod(HttpMethod method)
        {
            this.method = method;
            return this;
        }

        public Builder header(String name, String value)
        {
            headers.add(name, value);
            return this;
        }

        public Builder accept(String mediaType)
        {
            this.mediaType = mediaType;
            return this;
        }

        public Builder entity(Entity<?> entity)
        {
            this.entity = entity;
            return this;
        }
    }
}
