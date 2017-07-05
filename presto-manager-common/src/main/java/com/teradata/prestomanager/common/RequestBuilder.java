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

import org.glassfish.jersey.client.JerseyClient;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import java.net.URI;

import static java.util.Objects.requireNonNull;
import static org.glassfish.jersey.client.JerseyClientBuilder.createClient;

public class RequestBuilder
{
    private final URI path;
    private Entity body;
    private String headerName;
    private Object headerValue;
    private static JerseyClient jerseyClient = createClient();

    private RequestBuilder(URI path) {
        this.path = path;
    }

    public static RequestBuilder newRequestBuilder(URI path)
    {
        return new RequestBuilder(path);
    }

    public RequestBuilder withBody(Entity body)
    {
        this.body = body;
        return this;
    }

    public RequestBuilder withHeader(String headerName, Object headerValue)
    {
        this.headerName = headerName;
        this.headerValue = headerValue;
        return this;
    }

    public Invocation buildGet()
    {
        return build().buildGet();
    }

    public Invocation buildPut()
    {
        requireNonNull(body, "body is null");

        return build().buildPut(body);
    }

    public Invocation buildPost()
    {
        requireNonNull(body, "body is null");

        return build().buildPost(body);
    }

    public Invocation buildDelete()
    {
        return build().buildDelete();
    }

    private Invocation.Builder build()
    {
        WebTarget webTarget = jerseyClient.target(path);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.TEXT_PLAIN);
        if (headerValue != null && headerName != null) {
            invocationBuilder.header(headerName, headerValue);
        }
        return invocationBuilder;
    }
}
