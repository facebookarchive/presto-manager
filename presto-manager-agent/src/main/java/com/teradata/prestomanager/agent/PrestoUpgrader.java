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

import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;

import static com.google.common.io.MoreFiles.deleteDirectoryContents;
import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static com.teradata.prestomanager.agent.PackageApiUtils.checkRpmPackage;
import static com.teradata.prestomanager.agent.PackageApiUtils.fetchFileFromUrl;
import static java.io.File.createTempFile;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoUpgrader
        implements PrestoCommand
{
    private static final Logger LOGGER = Logger.get(PrestoUpgrader.class);
    private static final int SUBPROCESS_TIMEOUT = 150;
    private static final String CONFIG_DIR = "/etc/presto";

    private final PackageType packageType;
    private final URL urlToFetchPackage;
    private final boolean checkDependencies;
    private final boolean preserveConfig;

    /**
     *  @param packageType The type of package to install.
     *  @param urlToFetchPackage The URL from which to retrieve Presto.
     *  @param checkDependencies Whether to check RPM dependencies during upgrade.
     *  @param preserveConfig Whether to preserve the existing configuration files.
     */
    public PrestoUpgrader(PackageType packageType, URL urlToFetchPackage, boolean checkDependencies, boolean preserveConfig)
    {
        this.packageType = requireNonNull(packageType);
        this.urlToFetchPackage = requireNonNull(urlToFetchPackage);
        this.checkDependencies = requireNonNull(checkDependencies);
        this.preserveConfig = requireNonNull(preserveConfig);
    }

    public void runCommand()
            throws PrestoManagerException
    {
        switch (packageType) {
            case RPM:
                File tempFile = fetchFileFromUrl(urlToFetchPackage);
                try {
                    upgradeUsingRpm(tempFile.toString(), checkDependencies, preserveConfig);
                }
                finally {
                    if (!tempFile.delete()) {
                        LOGGER.warn("Failed to delete the tempFile: %s", tempFile.toString());
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

    private static void upgradeUsingRpm(String pathToRpm, boolean checkDependencies, boolean preserveConfig)
            throws PrestoManagerException
    {
        checkRpmPackage(pathToRpm);
        if (preserveConfig) {
            File tempConfig = storeConfigFiles();
            try {
                upgradePackage(pathToRpm, checkDependencies);
                deployConfigFiles(tempConfig);
            }
            finally {
                if (!tempConfig.delete()) {
                    LOGGER.warn("Failed to delete the temporary file: %s", tempConfig.toString());
                }
            }
        }
        else {
            upgradePackage(pathToRpm, checkDependencies);
        }
        LOGGER.debug("Successfully upgraded presto");
    }

    private static void upgradePackage(String pathToRpm, boolean checkDependencies)
            throws PrestoManagerException
    {
        int upgradeRpm;
        if (checkDependencies) {
            upgradeRpm = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-U", pathToRpm);
        }
        else {
            upgradeRpm = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-U", "--nodeps", pathToRpm);
        }
        if (upgradeRpm != 0) {
            throw new PrestoManagerException("Failed to upgrade Presto", upgradeRpm);
        }
    }

    private static File storeConfigFiles()
            throws PrestoManagerException
    {
        File tempFile;
        try {
            tempFile = createTempFile("PrestoConfigs", ".tar.gz");
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to create temp file.", e);
        }
        int tarResult = executeCommand("tar", "-czvf", tempFile.getAbsolutePath(), "-C", CONFIG_DIR, ".");
        if (tarResult != 0) {
            throw new PrestoManagerException("Failed to tar config files.", tarResult);
        }
        return tempFile;
    }

    private static void deployConfigFiles(File configTar)
            throws PrestoManagerException
    {
        try {
            deleteDirectoryContents(Paths.get(CONFIG_DIR));
        }
        catch (IOException e) {
            throw new PrestoManagerException(format("Failed to delete contents of the directory: %s", CONFIG_DIR), e);
        }
        int untarResult = executeCommand("tar", "-xzvf", configTar.getAbsolutePath(), "-C", CONFIG_DIR);
        if (untarResult != 0) {
            throw new PrestoManagerException("Failed to deploy config files", untarResult);
        }
    }
}
