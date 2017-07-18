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

import javax.ws.rs.core.Response;

public enum ExtendedStatus
        implements Response.StatusType
{
    MULTI_STATUS(207, "Multiple responses available");

    private final int code;
    private final String reason;
    private final Response.Status.Family family;

    ExtendedStatus(int statusCode, String reasonPhrase)
    {
        this.code = statusCode;
        this.reason = reasonPhrase;
        this.family = Response.Status.Family.familyOf(statusCode);
    }

    @Override
    public int getStatusCode()
    {
        return this.code;
    }

    @Override
    public Response.Status.Family getFamily()
    {
        return this.family;
    }

    @Override
    public String getReasonPhrase()
    {
        return this.reason;
    }
}
