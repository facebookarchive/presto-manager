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

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public interface AgentMap
{
    Map<UUID, URI> getUrisByIds(Collection<UUID> ids);

    Map<UUID, URI> getAllUris();

    Map<UUID, URI> getCoordinatorUris();

    Map<UUID, URI> getWorkerUris();

    default Map<UUID, URI> getUrisByScope(ApiScope scope)
    {
        switch (scope) {
            case CLUSTER:
                return getAllUris();
            case WORKERS:
                return getWorkerUris();
            case COORDINATOR:
                return getCoordinatorUris();
        }
        throw new IllegalArgumentException("Non-exhaustive Enum 'switch' statement");
    }
}
