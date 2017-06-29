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

import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static java.lang.String.format;

//  TODO: Add Logger
public final class PrestoInstaller
{
    private PrestoInstaller() {}

    public static void installPresto(String pathToRpm, boolean disableDependencyChecking)
            throws PrestoManagerException
    {
        String checkRpm = format("rpm -K --nosignature %s", pathToRpm);
        if (executeCommand(checkRpm) != 0) {
            throw new PrestoManagerException("Corrupted RPM");
        }
        String nodeps = "";
        if (disableDependencyChecking) {
            nodeps = "--nodeps";
        }
        String installRpm = format("sudo rpm -i %s %s", nodeps, pathToRpm);
        if (executeCommand(installRpm) != 0) {
            throw new PrestoManagerException("Failed to install presto");
        }
    }

    public static void uninstallPresto(String packageName, boolean disableDependencyChecking, boolean ignoreNotInstalled)
            throws PrestoManagerException
    {
        String packageInstalled = format("sudo rpm -q %s", packageName);
        if (executeCommand(packageInstalled) != 0) {
            if (!ignoreNotInstalled) {
                throw new PrestoManagerException(format("Package %s is not installed", packageName));
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

    public static void upgradePresto(String pathToRpm, boolean disableDependencyChecking)
            throws PrestoManagerException
    {
        String checkRpm = format("rpm -Kv --nosignature %s", pathToRpm);
        if (executeCommand(checkRpm) != 0) {
            throw new PrestoManagerException("Corrupted RPM");
        }
        String nodeps = "";
        if (disableDependencyChecking) {
            nodeps = "--nodeps";
        }
        String upgradeRpm = format("sudo rpm -U %s %s", nodeps, pathToRpm);
        if (executeCommand(upgradeRpm) != 0) {
            throw new PrestoManagerException("Failed to upgrade presto");
        }
    }
}
