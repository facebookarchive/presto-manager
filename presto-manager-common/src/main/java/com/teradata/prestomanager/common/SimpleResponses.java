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
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.StatusType;

import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

/**
 * Utility class for generating simple HTTP responses
 */
public final class SimpleResponses
{
    private SimpleResponses() {}

    private static Response simpleResponse(StatusType status, String message)
    {
        return Response.status(status).entity(message).type(TEXT_PLAIN).build();
    }

    /* Methods for common status codes, in numeric order */

    public static Response badRequest(String message)
    {
        return simpleResponse(Status.BAD_REQUEST, message);
    }

    public static Response notFound(String message)
    {
        return simpleResponse(Status.NOT_FOUND, message);
    }

    public static Response serverError(String message)
    {
        return simpleResponse(Status.INTERNAL_SERVER_ERROR, message);
    }

    /* The same methods, but with format strings and varargs */

    public static Response badRequest(String format, Object... objects)
    {
        return badRequest(format(format, objects));
    }

    public static Response notFound(String format, Object... objects)
    {
        return notFound(format(format, objects));
    }

    public static Response serverError(String format, Object... objects)
    {
        return serverError(format(format, objects));
    }
}
