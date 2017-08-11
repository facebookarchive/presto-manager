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
package com.teradata.prestomanager.controller.api;

import com.teradata.prestomanager.common.ApiRequester;
import com.teradata.prestomanager.controller.RequestDispatcher;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import java.util.Collection;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public abstract class AbstractControllerAPI
{
    private Client forwardingClient;
    private RequestDispatcher requestDispatcher;

    protected AbstractControllerAPI(
            Client forwardingClient,
            RequestDispatcher requestDispatcher)
    {
        this.forwardingClient = requireNonNull(forwardingClient);
        this.requestDispatcher = requireNonNull(requestDispatcher);
    }

    protected void optionalQueryParam(ApiRequester.Builder request, String name, Object value)
    {
        if (value != null) {
            request.queryParam(name, value);
        }
    }

    protected ApiRequester.Builder requesterBuilder(Class<?> clazz)
    {
        return ApiRequester.builder(forwardingClient, clazz);
    }

    protected Response forwardRequest(String scope, ApiRequester requester, Collection<UUID> nodeId)
    {
        return requestDispatcher.forwardRequest(scope, requester, nodeId);
    }
}
