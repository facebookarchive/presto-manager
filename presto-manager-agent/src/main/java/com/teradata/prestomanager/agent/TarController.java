package com.teradata.prestomanager.agent;

import com.google.inject.Inject;
import io.airlift.log.Logger;

import javax.ws.rs.client.Client;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.teradata.prestomanager.agent.AgentFileUtils.downloadFile;
import static com.teradata.prestomanager.agent.AgentFileUtils.updateProperty;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.isRegularFile;
import static java.util.Objects.requireNonNull;

public class TarController
        extends PackageController
{
    private static final Logger LOGGER = Logger.get(TarController.class);
    // TODO: Make this configurable
    private static final Path INSTALLATION_DIR = Paths.get("presto/");

    private final Path configDir;
    private final Path catalogDir;
    private final Path dataDir;
    // TODO: pluginDir should not be configurable
    private final Path pluginDir;
    private final CommandExecutor executor;
    private final PrestoConfigDeployer configDeployer;

    /**
     * These variables are set during {@link #postInstall()}
     */
    private Optional<Path> launcherScript = Optional.empty();
    private Optional<String> launcherConfig = Optional.empty();

    @Inject
    TarController(PrestoConfig config, Client client,
            PrestoInformer informer, CommandExecutor executor,
            PrestoConfigDeployer configDeployer)
    {
        super(client, informer);
        this.configDir = requireNonNull(config.getConfigurationDirectory());
        this.catalogDir = requireNonNull(config.getCatalogDirectory());
        this.dataDir = requireNonNull(config.getDataDirectory());
        this.pluginDir = requireNonNull(config.getPluginDirectory());
        this.executor = requireNonNull(executor);
        this.configDeployer = requireNonNull(configDeployer);
    }

    public void installAsync(URL packageUrl, boolean checkDependencies)
            throws PrestoManagerException
    {
        if (!checkDependencies) {
            throw new PrestoManagerException("Unsupported parameter 'checkDependencies' for tarball installation");
        }
        Path tempFile = getTarPackage(packageUrl);
        try {
            tarInstall(tempFile);
            configDeployer.deployDefaultConfig(configDir, dataDir, pluginDir);
            configDeployer.deployDefaultConnectors(catalogDir);
            postInstall();
            LOGGER.debug("Successfully installed Presto");
        }
        finally {
            deleteTempFile(tempFile);
        }
    }

    private Path getTarPackage(URL packageUrl)
            throws PrestoManagerException
    {
        Path tempFile;
        try {
            LOGGER.debug("Downloading file from url: %s", packageUrl);
            tempFile = downloadFile(packageUrl);
        }
        catch (IOException e) {
            throw new PrestoManagerException(format("Failed to download file: %s", packageUrl.toString()), e);
        }
        return tempFile;
    }

    private void tarInstall(Path tarFile)
            throws PrestoManagerException
    {
        try {
            if (isDirectory(INSTALLATION_DIR)) {
                throw new PrestoManagerException(format("Directory '%s' already exists", INSTALLATION_DIR.toString()));
            }
            else {
                createDirectories(INSTALLATION_DIR);
            }
            createDirectories(configDir);
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to create directory", e);
        }
        int untarResult = executor.runCommand("tar", "-xzvf", tarFile.toAbsolutePath().toString(),
                "-C", INSTALLATION_DIR.toAbsolutePath().toString());
        if (untarResult != 0) {
            throw new PrestoManagerException("Failed to install Presto", untarResult);
        }
    }

    private void postInstall()
            throws PrestoManagerException
    {
        try {
            Path prestoDir = Files.list(INSTALLATION_DIR)
                    .filter(path -> isDirectory(path))
                    .reduce((a, b) -> {
                        throw new IllegalStateException(format("Multiple directories within `%s`", INSTALLATION_DIR));
                    })
                    .get().getFileName();
            launcherScript = Optional.of(INSTALLATION_DIR.toAbsolutePath().resolve(prestoDir).resolve("bin/launcher"));
            updateProperty(configDir.toAbsolutePath().resolve("node.properties"), "plugin.dir",
                    INSTALLATION_DIR.toAbsolutePath().resolve(prestoDir).resolve("plugin").toString());
            /**
             * If there are extra spaces in this string, there will be problems
             * while executing commands using {@link #runLauncherCommand(String)}
             * TODO: Make --launcher-config configurable
             * TODO: Add --server-log-file --launcher-log-file
             */
            launcherConfig = Optional.of("--data-dir " + dataDir.toAbsolutePath()
                    + " --launcher-config " + INSTALLATION_DIR.toAbsolutePath().resolve(prestoDir).resolve("bin/launcher.properties")
                    + " --node-config " + configDir.toAbsolutePath().resolve("node.properties")
                    + " --jvm-config " + configDir.toAbsolutePath().resolve("jvm.config")
                    + " --config " + configDir.toAbsolutePath().resolve("config.properties"));
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to perform post install steps", e);
        }
    }

    public void uninstallAsync(boolean checkDependencies)
            throws PrestoManagerException
    {
        if (!checkDependencies) {
            throw new PrestoManagerException("Unsupported parameter 'checkDependencies' for tarball uninstall");
        }
        try {
            deleteRecursively(INSTALLATION_DIR);
            deleteRecursively(dataDir);
            deleteRecursively(catalogDir);
            deleteRecursively(configDir);
            launcherScript = Optional.empty();
            launcherConfig = Optional.empty();
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to uninstall Presto", e);
        }
        LOGGER.debug("Successfully uninstalled Presto");
    }

    public void upgradeAsync(URL packageUrl, boolean checkDependencies, boolean preserveConfig)
            throws PrestoManagerException
    {
        if (!checkDependencies) {
            throw new PrestoManagerException("Unsupported parameter 'checkDependencies' for tarball upgrade");
        }
        Path tempPackage = getTarPackage(packageUrl);
        try {
            if (preserveConfig) {
                Path tempConfigDir = configDeployer.backupDirectory(configDir);
                try {
                    uninstallAsync(checkDependencies);
                    tarInstall(tempPackage);
                    postInstall();
                    configDeployer.restoreDirectory(tempConfigDir, configDir);
                }
                finally {
                    deleteTempFile(tempConfigDir);
                }
            }
            else {
                uninstallAsync(checkDependencies);
                tarInstall(tempPackage);
                postInstall();
                configDeployer.deployDefaultConfig(configDir, dataDir, pluginDir);
                configDeployer.deployDefaultConnectors(catalogDir);
            }
        }
        finally {
            deleteTempFile(tempPackage);
        }
        LOGGER.debug("Successfully upgraded presto");
    }

    public void startAsync()
            throws PrestoManagerException
    {
        executeCommand("start");
    }

    public void terminate()
            throws PrestoManagerException
    {
        executeCommand("stop");
    }

    public void kill()
            throws PrestoManagerException
    {
        executeCommand("kill");
    }

    public void restartAsync()
            throws PrestoManagerException
    {
        executeCommand("restart");
    }

    private int runLauncherCommand(String command)
            throws PrestoManagerException
    {
        if (!isInstalled()) {
            throw new PrestoManagerException("Presto is not installed");
        }
        List<String> commandList = new ArrayList<>();
        commandList.add(launcherScript.get().toAbsolutePath().toString());
        commandList.add(command);
        launcherConfig.ifPresent((launcher) -> commandList.addAll(Arrays.asList(launcher.split(" "))));
        return executor.runCommand(commandList.toArray(new String[commandList.size()]));
    }

    private void executeCommand(String command)
            throws PrestoManagerException
    {
        int commandResult = runLauncherCommand(command);
        if (commandResult != 0) {
            throw new PrestoManagerException(format("Failed to %s Presto", command), commandResult);
        }
    }

    public Optional<String> getVersion()
            throws PrestoManagerException
    {
        return Optional.empty();
    }

    public boolean isInstalled()
            throws PrestoManagerException
    {
        return isDirectory(INSTALLATION_DIR) && launcherScript.isPresent() && isRegularFile(launcherScript.get());
    }

    private static void deleteTempFile(Path file)
    {
        try {
            delete(file);
        }
        catch (IOException e) {
            LOGGER.warn(e, "Failed to delete the temporary file: %s", file.toString());
        }
    }
}
