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

import com.teradata.prestomanager.agent.api.PackageAPI.PackageType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static java.io.File.createTempFile;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.FileUtils.copyURLToFile;

public final class PrestoInstaller
        implements PrestoCommand
{
    private static final Logger LOGGER = LogManager.getLogger(PrestoInstaller.class);
    private static final int CONNECTION_TIMEOUT = 1000000;
    private static final int READ_TIMEOUT = 10000;

    private final PackageType packageType;
    private final URL urlToFetchPackage;
    private final boolean disableDependencyChecking;

    public PrestoInstaller(PackageType packageType, URL urlToFetchPackage, boolean disableDependencyChecking)
    {
        this.packageType = requireNonNull(packageType);
        this.urlToFetchPackage = requireNonNull(urlToFetchPackage);
        this.disableDependencyChecking = requireNonNull(disableDependencyChecking);
    }

    public void runCommand()
            throws PrestoManagerException
    {
        switch (packageType) {
            case RPM:
                File tempFile;
                try {
                    LOGGER.debug("Downloading package from url: {}", urlToFetchPackage);
                    tempFile = createTempFile("presto", ".rpm");
                    copyURLToFile(urlToFetchPackage, tempFile, CONNECTION_TIMEOUT, READ_TIMEOUT);
                }
                catch (IOException e) {
                    throw new PrestoManagerException(format("Failed to download file from url %s", urlToFetchPackage), e);
                }
                installUsingRpm(tempFile.toString(), disableDependencyChecking);
                tempFile.delete();
                break;
            case TARBALL:
                // TODO: Add tarball installation
                throw new UnsupportedOperationException("Tarball installation is not supported");
            default:
                throw new IllegalArgumentException(format("Unsupported package type %s", packageType));
        }
    }

    private static void installUsingRpm(String pathToRpm, boolean checkDependencies)
            throws PrestoManagerException
    {
        int checkRpm = executeCommand("rpm", "-Kv", "--nosignature ", pathToRpm);
        if (checkRpm != 0) {
            throw new PrestoManagerException("Corrupted RPM", checkRpm);
        }
        String nodeps = checkDependencies ? "" : "--nodeps";
        int installRpm = executeCommand(150, "sudo", "rpm", "-iv", nodeps, pathToRpm);
        if (installRpm != 0) {
            throw new PrestoManagerException("Failed to install presto", installRpm);
        }
        LOGGER.debug("Successfully installed presto");
    }
}
