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

import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static com.teradata.prestomanager.agent.PrestoRpmController.terminatePresto;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoUninstaller
        implements PrestoCommand
{
    private static final Logger LOGGER = Logger.get(PrestoUninstaller.class);
    private static final String PRESTO_PACKAGE = "presto-server-rpm";
    private static final int SUBPROCESS_TIMEOUT = 90;

    private final PackageType packageType;
    private final boolean checkDependencies;
    private final boolean forceUninstall;

    public PrestoUninstaller(PackageType packageType, boolean checkDependencies, boolean forceUninstall)
    {
        this.packageType = requireNonNull(packageType);
        this.checkDependencies = requireNonNull(checkDependencies);
        this.forceUninstall = requireNonNull(forceUninstall);
    }

    public void runCommand()
            throws PrestoManagerException
    {
        switch (packageType) {
            case RPM:
                uninstallUsingRpm(checkDependencies, forceUninstall);
                break;
            case TARBALL:
                // TODO: Add tarball uninstallation
                throw new UnsupportedOperationException("Tarball uninstall is not supported");
            default:
                throw new IllegalArgumentException(format("Unsupported package type %s", packageType));
        }
    }

    private static void uninstallUsingRpm(boolean checkDependencies, boolean forceUninstall)
            throws PrestoManagerException
    {
        int checkPackageInstalled = executeCommand("sudo", "rpm", "-q", PRESTO_PACKAGE);
        if (checkPackageInstalled != 0) {
            if (!forceUninstall) {
                throw new PrestoManagerException(format("Package '%s' is not installed", PRESTO_PACKAGE), checkPackageInstalled);
            }
            LOGGER.warn("Package '%s' is not installed; Process exited with return value: %s", PRESTO_PACKAGE, checkPackageInstalled);
            return;
        }
        if (executeCommand("service", "presto", "status") == 0) {
            if (!forceUninstall) {
                throw new PrestoManagerException("Presto is running");
            }
            LOGGER.warn("Presto is running; Presto will be forcibly stopped before attempting to uninstall.");
            terminatePresto();
        }
        int uninstallPackage;
        if (checkDependencies) {
            uninstallPackage = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-e", PRESTO_PACKAGE);
        }
        else {
            uninstallPackage = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-e", "--nodeps", PRESTO_PACKAGE);
        }
        if (uninstallPackage != 0) {
            throw new PrestoManagerException(format("Failed to uninstall package: %s", PRESTO_PACKAGE), uninstallPackage);
        }
        LOGGER.debug("Successfully uninstalled Presto");
    }
}
