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

import com.google.inject.Inject;
import com.teradata.prestomanager.agent.CommandExecutor.CommandResult;
import io.airlift.log.Logger;

import javax.ws.rs.client.Client;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;

import static com.teradata.prestomanager.agent.AgentFileUtils.downloadFile;
import static java.lang.String.format;

// TODO: Add helper functions for execute("sudo", "rpm", args) and execute("service", "presto", arg)
public class RpmController
        extends PackageController
{
    private static final Logger LOGGER = Logger.get(RpmController.class);

    private final Path configDir;
    private final Path catalogDir;
    private final Path dataDir;
    private final Path pluginDir;
    private final Path launcherScript;
    private final CommandExecutor executor;
    private final PrestoConfigUtils configUtils;

    // TODO: Inject less into here, if possible
    @Inject
    RpmController(PrestoConfig config, Client client,
            PrestoInformer informer, CommandExecutor executor,
            PrestoConfigUtils configUtils)
    {
        super(client, informer);
        configDir = config.getCatalogDirectory();
        catalogDir = config.getCatalogDirectory();
        dataDir = config.getDataDirectory();
        pluginDir = config.getPluginDirectory();
        launcherScript = config.getLauncherPath();
        this.executor = executor;
        this.configUtils = configUtils;
    }

    public void installAsync(URL packageUrl, boolean checkDependencies)
            throws PrestoManagerException
    {
        File tempFile = getRpmPackage(packageUrl);
        try {
            int installRpm;
            if (checkDependencies) {
                installRpm = executor.runLongCommand(
                        "sudo", "rpm", "-iv", tempFile.toString());
            }
            else {
                installRpm = executor.runLongCommand(
                        "sudo", "rpm", "-iv", "--nodeps", tempFile.toString());
            }
            if (installRpm != 0) {
                throw new PrestoManagerException("Failed to install Presto", installRpm);
            }
            configUtils.addConfigFiles(configDir, dataDir, pluginDir);
            configUtils.addConnectors(catalogDir);
            LOGGER.debug("Successfully installed Presto");
        }
        finally {
            if (!tempFile.delete()) {
                LOGGER.warn("Failed to delete the temporary file: %s", tempFile.toString());
            }
        }
    }

    private File getRpmPackage(URL packageUrl)
            throws PrestoManagerException
    {
        Path tempFile;
        try {
            tempFile = downloadFile(packageUrl);
        }
        catch (IOException e) {
            throw new PrestoManagerException(format("Failed to download file: %s", packageUrl.toString()), e);
        }
        int checkRpm = executor.runCommand(
                "rpm", "-Kv", "--nosignature", tempFile.toString());
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
            uninstallPackage = executor.runLongCommand(
                    "sudo", "rpm", "-e", "presto-server-rpm");
        }
        else {
            uninstallPackage = executor.runLongCommand(
                    "sudo", "rpm", "-e", "--nodeps", "presto-server-rpm");
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
                File tempConfig = configUtils.storeConfigFiles(configDir);
                try {
                    upgradePackage(tempPackage.toString(), checkDependencies);
                    configUtils.deployConfigFiles(tempConfig, configDir);
                }
                finally {
                    if (!tempConfig.delete()) {
                        LOGGER.warn("Failed to delete the temporary file: %s", tempConfig.toString());
                    }
                }
            }
            else {
                upgradePackage(tempPackage.toString(), checkDependencies);
                configUtils.addConfigFiles(configDir, dataDir, pluginDir);
                configUtils.addConnectors(catalogDir);
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
            upgradeRpm = executor.runLongCommand("sudo", "rpm", "-U", pathToRpm);
        }
        else {
            upgradeRpm = executor.runLongCommand("sudo", "rpm", "-U", "--nodeps", pathToRpm);
        }
        if (upgradeRpm != 0) {
            throw new PrestoManagerException("Failed to upgrade Presto", upgradeRpm);
        }
    }

    public void startAsync()
            throws PrestoManagerException
    {
        int startPresto = executor.runLongCommand("service", "presto", "start");
        if (startPresto != 0) {
            throw new PrestoManagerException("Failed to start Presto", startPresto);
        }
    }

    public void terminate()
            throws PrestoManagerException
    {
        int prestoTerminate = executor.runCommand("service", "presto", "stop");
        if (prestoTerminate != 0) {
            throw new PrestoManagerException("Failed to stop presto", prestoTerminate);
        }
    }

    public void kill()
            throws PrestoManagerException
    {
        executor.runCommand("sudo", launcherScript.toString(), "kill");
        int prestoKill = executor.runCommand("service", "presto", "status");
        if (prestoKill != 3) {
            throw new PrestoManagerException("Failed to stop presto", prestoKill);
        }
    }

    public void restartAsync()
            throws PrestoManagerException
    {
        int prestoRestart = executor.runLongCommand("service", "presto", "restart");
        if (prestoRestart != 0) {
            throw new PrestoManagerException("Failed to restart presto", prestoRestart);
        }
    }

    public String getVersion()
            throws PrestoManagerException
    {
        CommandResult commandResult = executor.getCommandResult(
                "rpm", "-q", "--qf", "%{VERSION}", "presto-server-rpm");
        if (commandResult.getExitValue() != 0) {
            throw new PrestoManagerException("Failed to retrieve Presto version", commandResult.getExitValue());
        }
        return commandResult.getOutput();
    }

    public boolean isInstalled()
            throws PrestoManagerException
    {
        return executor.runCommand("rpm", "-q", "presto-server-rpm") == 0;
    }

    public boolean isRunning()
            throws PrestoManagerException
    {
        return isInstalled() && executor.runCommand("service", "presto", "status") == 0;
    }
}
