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
package com.teradata.prestomanager.agent;

import java.util.concurrent.TimeUnit;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

// TODO: Add logger
public final class CommandExecutor
{
    private CommandExecutor() {}

    public static int executeCommand(String command)
            throws PrestoManagerException
    {
        try {
            Process process = getRuntime().exec(command);
            process.waitFor(2, TimeUnit.MINUTES);
            return process.exitValue();
        }
        catch (Exception e) {
            throw new PrestoManagerException(format("Error executing command: %s", command), e);
        }
    }
}
