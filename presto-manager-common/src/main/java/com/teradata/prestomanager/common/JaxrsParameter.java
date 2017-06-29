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

// TODO: Replace this class and its children with ParamConverters and ParamConverterProviders
/**
 * Abstract class for providing parameters to JAX-RS-annotated methods
 *
 * Any parameter using a concrete extension of this class should provide
 * a default value via {@link javax.ws.rs.DefaultValue @DefaultValue}.
 *
 * @param <T> The type of the parameter provided.
 */
public abstract class JaxrsParameter<T>
{
    private T value;
    private boolean isValid = true;

    public JaxrsParameter(String s)
    {
        initialize(s);
    }

    /**
     * Method to convert a string into an instance of {@code T}
     * @throws ParseException If the string can not be converted
     */
    protected abstract T parseString(String s)
            throws ParseException;

    protected void initialize(String s)
    {
        try {
            value = parseString(s);
        }
        catch (ParseException e) {
            isValid = false;
        }
    }

    public boolean isValid()
    {
        return isValid;
    }

    public T get()
    {
        if (!isValid) {
            throw new IllegalStateException("Attempted to get invalid parameter");
        }
        return value;
    }

    /**
     * Exception indicating that a string could not be parsed
     * as a parameter via {@link #parseString(String)}
     */
    public static class ParseException
            extends Exception
    {
        public ParseException()
        {
            super();
        }

        public ParseException(String message)
        {
            super(message);
        }

        public ParseException(Throwable cause)
        {
            super(cause);
        }

        public ParseException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
}
