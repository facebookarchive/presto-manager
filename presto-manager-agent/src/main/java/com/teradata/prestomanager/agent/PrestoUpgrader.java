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
import java.net.URL;

import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static com.teradata.prestomanager.agent.PackageApiUtils.checkRpmPackage;
import static com.teradata.prestomanager.agent.PackageApiUtils.fetchFileFromUrl;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoUpgrader
        implements PrestoCommand
{
    private static final Logger LOGGER = LogManager.getLogger(PrestoUpgrader.class);
    private static final int SUBPROCESS_TIMEOUT = 150;

    private final PackageType packageType;
    private final URL urlToFetchPackage;
    private final boolean checkDependencies;

    public PrestoUpgrader(PackageType packageType, URL urlToFetchPackage, boolean checkDependencies)
    {
        this.packageType = requireNonNull(packageType);
        this.urlToFetchPackage = requireNonNull(urlToFetchPackage);
        this.checkDependencies = requireNonNull(checkDependencies);
    }

    public void runCommand()
            throws PrestoManagerException
    {
        switch (packageType) {
            case RPM:
                File tempFile = fetchFileFromUrl(urlToFetchPackage);
                try {
                    upgradeUsingRpm(tempFile.toString(), checkDependencies);
                }
                finally {
                    if (!tempFile.delete()) {
                        LOGGER.warn("Failed to delete the tempFile: {}", tempFile.toString());
                    }
                }
                break;
            case TARBALL:
                // TODO: Add tarball installation
                throw new UnsupportedOperationException("Tarball upgrade is not supported");
            default:
                throw new IllegalArgumentException(format("Unsupported package type %s", packageType));
        }
    }

    private static void upgradeUsingRpm(String pathToRpm, boolean checkDependencies)
            throws PrestoManagerException
    {
        checkRpmPackage(pathToRpm);
        int upgradeRpm;
        if (checkDependencies) {
            upgradeRpm = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-U", pathToRpm);
        }
        else {
            upgradeRpm = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-U", "--nodeps", pathToRpm);
        }
        if (upgradeRpm != 0) {
            throw new PrestoManagerException("Failed to upgrade presto", upgradeRpm);
        }
        LOGGER.debug("Successfully upgraded presto");
    }
}
