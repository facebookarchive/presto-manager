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

import static com.teradata.prestomanager.agent.AgentFileUtils.downloadFile;
import static com.teradata.prestomanager.agent.CommandExecutor.execCommandResult;
import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static com.teradata.prestomanager.agent.PrestoConfigUtils.addConfigFiles;
import static com.teradata.prestomanager.agent.PrestoConfigUtils.addConnectors;
import static com.teradata.prestomanager.agent.PrestoConfigUtils.deployConfigFiles;
import static com.teradata.prestomanager.agent.PrestoConfigUtils.storeConfigFiles;
import static java.lang.String.format;

public class RpmController
        extends PackageController
{
    private static final Logger LOGGER = Logger.get(RpmController.class);

    public void installAsync(URL packageUrl, boolean checkDependencies)
            throws PrestoManagerException
    {
        File tempFile = getRpmPackage(packageUrl);
        try {
            int installRpm;
            if (checkDependencies) {
                installRpm = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-iv", tempFile.toString());
            }
            else {
                installRpm = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-iv", "--nodeps", tempFile.toString());
            }
            if (installRpm != 0) {
                throw new PrestoManagerException("Failed to install Presto", installRpm);
            }
            addConfigFiles(CONFIG_DIR);
            addConnectors(CATALOG_DIR);
            LOGGER.debug("Successfully installed Presto");
        }
        finally {
            if (!tempFile.delete()) {
                LOGGER.warn("Failed to delete the temporary file: %s", tempFile.toString());
            }
        }
    }

    private static File getRpmPackage(URL packageUrl)
            throws PrestoManagerException
    {
        Path tempFile;
        try {
            tempFile = downloadFile(packageUrl);
        }
        catch (IOException e) {
            throw new PrestoManagerException(format("Failed to download file: %s", packageUrl.toString()), e);
        }
        int checkRpm = executeCommand("rpm", "-Kv", "--nosignature", tempFile.toString());
        if (checkRpm != 0) {
            throw new PrestoManagerException("Corrupted RPM", checkRpm);
        }
        return tempFile.toFile();
    }

    public void uninstallAsync(boolean checkDependencies)
            throws PrestoManagerException
    {
        int uninstallPackage;
        if (checkDependencies) {
            uninstallPackage = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-e", "presto-server-rpm");
        }
        else {
            uninstallPackage = executeCommand(SUBPROCESS_TIMEOUT, "sudo", "rpm", "-e", "--nodeps", "presto-server-rpm");
        }
        if (uninstallPackage != 0) {
            throw new PrestoManagerException(format("Failed to uninstall package: %s", "presto-server-rpm"), uninstallPackage);
        }
        LOGGER.debug("Successfully uninstalled Presto");
    }

    public void upgradeAsync(URL packageUrl, boolean checkDependencies, boolean preserveConfig)
            throws PrestoManagerException
    {
        File tempPackage = getRpmPackage(packageUrl);
        try {
            if (preserveConfig) {
                File tempConfig = storeConfigFiles(CONFIG_DIR);
                try {
                    upgradePackage(tempPackage.toString(), checkDependencies);
                    deployConfigFiles(tempConfig, CONFIG_DIR);
                }
                finally {
                    if (!tempConfig.delete()) {
                        LOGGER.warn("Failed to delete the temporary file: %s", tempConfig.toString());
                    }
                }
            }
            else {
                upgradePackage(tempPackage.toString(), checkDependencies);
                addConfigFiles(CONFIG_DIR);
                addConnectors(CATALOG_DIR);
            }
        }
        finally {
            if (!tempPackage.delete()) {
                LOGGER.warn("Failed to delete the temporary file: %s", tempPackage.toString());
            }
        }
        LOGGER.debug("Successfully upgraded presto");
    }

    private void upgradePackage(String pathToRpm, boolean checkDependencies)
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

    public void startAsync()
            throws PrestoManagerException
    {
        int startPresto = executeCommand(SUBPROCESS_TIMEOUT, "service", "presto", "start");
        if (startPresto != 0) {
            throw new PrestoManagerException("Failed to start Presto", startPresto);
        }
    }

    public void terminate()
            throws PrestoManagerException
    {
        int prestoTerminate = executeCommand("service", "presto", "stop");
        if (prestoTerminate != 0) {
            throw new PrestoManagerException("Failed to stop presto", prestoTerminate);
        }
    }

    public void kill()
            throws PrestoManagerException
    {
        executeCommand("sudo", LAUNCHER_SCRIPT.toString(), "kill");
        int prestoKill = executeCommand("service", "presto", "status");
        if (prestoKill != 3) {
            throw new PrestoManagerException("Failed to stop presto", prestoKill);
        }
    }

    public void restartAsync()
            throws PrestoManagerException
    {
        int prestoRestart = executeCommand(SUBPROCESS_TIMEOUT, "service", "presto", "restart");
        if (prestoRestart != 0) {
            throw new PrestoManagerException("Failed to restart presto", prestoRestart);
        }
    }

    public String getVersion()
            throws PrestoManagerException
    {
        CommandExecutor.CommandResult commandResult = execCommandResult("rpm", "-q", "--qf", "%{VERSION}", "presto-server-rpm");
        if (commandResult.getExitValue() != 0) {
            throw new PrestoManagerException("Failed to retrieve Presto version", commandResult.getExitValue());
        }
        return commandResult.getOutput();
    }

    public boolean isInstalled()
            throws PrestoManagerException
    {
        return executeCommand("rpm", "-q", "presto-server-rpm") == 0;
    }

    public boolean isRunning()
            throws PrestoManagerException
    {
        return isInstalled() && executeCommand("service", "presto", "status") == 0;
    }
}
