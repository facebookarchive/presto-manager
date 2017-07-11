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

import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoUninstaller
        implements PrestoCommand
{
    private final PackageType packageType;
    private final boolean disableDependencyChecking;
    private final boolean ignoreNotInstalled;

    public PrestoUninstaller(PackageType packageType, boolean disableDependencyChecking, boolean ignoreNotInstalled)
    {
        this.packageType = requireNonNull(packageType);
        this.disableDependencyChecking = requireNonNull(disableDependencyChecking);
        this.ignoreNotInstalled = requireNonNull(ignoreNotInstalled);
    }

    public void runCommand()
            throws PrestoManagerException
    {
        switch (packageType) {
            case RPM:
                uninstallUsingRpm("presto-server-rpm", disableDependencyChecking, ignoreNotInstalled);
                break;
            case TARBALL:
                // TODO: Add tarball installation
                throw new UnsupportedOperationException("Tarball uninstall is not supported");
            default:
                throw new IllegalArgumentException(format("Unsupported package type %s", packageType));
        }
    }

    private static void uninstallUsingRpm(String packageName, boolean disableDependencyChecking, boolean ignoreErrors)
            throws PrestoManagerException
    {
        if (executeCommand(format("sudo rpm -q %s", packageName)) != 0) {
            if (!ignoreErrors) {
                throw new PrestoManagerException(format("Package %s is not installed", packageName));
            }
            return;
        }
        if (executeCommand("service presto status") == 0) {
            if (!ignoreErrors) {
                throw new PrestoManagerException("Presto is running");
            }
        }
        else {
            String nodeps = "";
            if (disableDependencyChecking) {
                nodeps = "--nodeps";
            }
            String uninstallPackage = format("sudo rpm -e %s %s", nodeps, packageName);
            if (executeCommand(uninstallPackage) != 0) {
                throw new PrestoManagerException(format("Failed to uninstall package %s", packageName));
            }
        }
    }
}
