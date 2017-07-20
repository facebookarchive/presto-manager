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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static java.io.File.createTempFile;
import static java.lang.String.format;
import static java.nio.file.Files.copy;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class PackageApiUtils
{
    private static final Logger LOGGER = LogManager.getLogger(PackageApiUtils.class);

    private PackageApiUtils() {}

    public static File fetchFileFromUrl(URL urlToFetchPackage)
            throws PrestoManagerException
    {
        File tempFile;
        try {
            LOGGER.debug("Downloading package from url: {}. This can take a few minutes.", urlToFetchPackage);
            tempFile = createTempFile("presto", ".rpm");
            try (InputStream inputStream = urlToFetchPackage.openStream()) {
                copy(inputStream, tempFile.toPath(), REPLACE_EXISTING);
            }
        }
        catch (IOException e) {
            throw new PrestoManagerException(format("Failed to download file from url %s", urlToFetchPackage), e);
        }
        return tempFile;
    }

    public static void checkRpmPackage(String pathToRpm)
            throws PrestoManagerException
    {
        int checkRpm = executeCommand("rpm", "-Kv", "--nosignature", pathToRpm);
        if (checkRpm != 0) {
            throw new PrestoManagerException("Corrupted RPM", checkRpm);
        }
    }

    public static boolean isInstalled()
            throws PrestoManagerException
    {
        return executeCommand("rpm", "-q", "presto-server-rpm") == 0;
    }

    public static boolean isRunning()
            throws PrestoManagerException
    {
        return isInstalled() && executeCommand("service", "presto", "status") == 0;
    }
}
