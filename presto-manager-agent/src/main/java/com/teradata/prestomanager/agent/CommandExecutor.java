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

import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public final class CommandExecutor
{
    private static final Logger LOGGER = Logger.get(CommandExecutor.class);
    private static final int DEFAULT_TIMEOUT = 60;
    private final String[] commandArray;
    private final int timeout;

    private CommandExecutor(String[] commandArray, int timeout)
    {
        if (commandArray.length == 0) {
            throw new IllegalArgumentException("Command array is empty");
        }
        this.commandArray = commandArray;
        this.timeout = timeout;
    }

    public static int executeCommand(String... command)
            throws PrestoManagerException
    {
        return new CommandExecutor(command, DEFAULT_TIMEOUT).execute();
    }

    public static int executeCommand(int timeoutInSeconds, String... command)
            throws PrestoManagerException
    {
        return new CommandExecutor(command, timeoutInSeconds).execute();
    }

    private int execute()
            throws PrestoManagerException
    {
        String commandString = String.join(" ", commandArray);
        LOGGER.debug("Command to be executed: %s", commandString);

        ProcessBuilder processBuilder = new ProcessBuilder(commandArray);
        processBuilder.redirectErrorStream(true);
        try {
            Process process = processBuilder.start();
            new Thread(() -> {
                try (InputStream processStream = process.getInputStream()) {
                    LOGGER.info("Output from command: %s\n%s", commandString, new String(ByteStreams.toByteArray(processStream)));
                }
                catch (IOException e) {
                    LOGGER.error(e, "Failed to log the process output");
                }
            }).start();
            if (!process.waitFor(timeout, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new PrestoManagerException(format("Command timed out: %s", commandString));
            }
            return process.exitValue();
        }
        catch (IOException | InterruptedException e) {
            throw new PrestoManagerException(format("Error executing command: %s", commandString), e);
        }
    }
}
