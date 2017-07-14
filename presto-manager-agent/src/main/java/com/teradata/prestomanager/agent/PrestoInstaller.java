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
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static com.teradata.prestomanager.agent.PackageApiUtils.checkRpmPackage;
import static com.teradata.prestomanager.agent.PackageApiUtils.fetchFileFromUrl;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.write;
import static java.util.Objects.requireNonNull;

public final class PrestoInstaller
        implements PrestoCommand
{
    private static final Logger LOGGER = Logger.get(PrestoInstaller.class);
    private static final Path CATALOG_DIR = Paths.get("/etc/presto/catalog");
    private static final int SUBPROCESS_TIMEOUT = 150;

    private final PackageType packageType;
    private final URL urlToFetchPackage;
    private final boolean checkDependencies;

    public PrestoInstaller(PackageType packageType, URL urlToFetchPackage, boolean checkDependencies)
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
                    installUsingRpm(tempFile.toString(), checkDependencies);
                }
                finally {
                    if (!tempFile.delete()) {
                        LOGGER.warn("Failed to delete the temporary file: %s", tempFile.toString());
                    }
                }
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
        checkRpmPackage(pathToRpm);
        int installRpm;
        if (checkDependencies) {
            installRpm = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-iv", pathToRpm);
        }
        else {
            installRpm = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-iv", "--nodeps", pathToRpm);
        }
        if (installRpm != 0) {
            throw new PrestoManagerException("Failed to install Presto", installRpm);
        }
        addTpchCatalog();
        LOGGER.debug("Successfully installed Presto");
    }

    private static void addTpchCatalog()
            throws PrestoManagerException
    {
        try {
            if (!isDirectory(CATALOG_DIR)) {
                createDirectory(CATALOG_DIR);
                write(CATALOG_DIR.resolve("tpch.properties"), "connector.name=tpch".getBytes());
            }
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to add tpch catalog", e);
        }
    }
}
