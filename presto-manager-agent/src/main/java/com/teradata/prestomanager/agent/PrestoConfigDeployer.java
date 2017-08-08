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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Properties;

import static com.google.common.io.MoreFiles.deleteDirectoryContents;
import static com.teradata.prestomanager.agent.AgentFileUtils.copyDir;
import static com.teradata.prestomanager.agent.AgentFileUtils.updateProperty;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.Files.write;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class PrestoConfigDeployer
{
    private final Path defaultConfig;
    private final Path defaultCatalog;

    private CommandExecutor executor;

    @Inject
    PrestoConfigDeployer(AgentConfig config, CommandExecutor executor)
    {
        defaultConfig = requireNonNull(config.getDefaultConfigurationDirectory());
        defaultCatalog = requireNonNull(config.getDefaultCatalogDirectory());
        this.executor = requireNonNull(executor);
    }

    /**
     * Backup the given directory in a temporary file.
     * <p>
     * The only guarantee about the returned {@link Path} is that it may be
     * restored with {@link #restoreDirectory(Path, Path)}.
     * <p>
     * The backup file may be created in the system's temporary file directory.
     *
     * @return The location of the backup file.
     */
    public Path backupDirectory(Path directory)
            throws PrestoManagerException
    {
        Path tempFile;
        try {
            tempFile = createTempFile("PrestoConfigs", ".tar.gz");
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to create temp file.", e);
        }
        int tarResult = executor
                .runCommand("tar", "-czvf", tempFile.toAbsolutePath().toString(),
                        "-C", directory.toString(), ".");
        if (tarResult != 0) {
            throw new PrestoManagerException("Failed to tar config files.", tarResult);
        }
        return tempFile;
    }

    /**
     * Restore the contents of the given directory from the given backup file.
     * <p>
     * The backup file should have been created by {@link #backupDirectory(Path)}.
     */
    public void restoreDirectory(Path backup, Path directory)
            throws PrestoManagerException
    {
        try {
            deleteDirectoryContents(directory);
        }
        catch (IOException e) {
            throw new PrestoManagerException(format("Failed to delete contents of the directory: %s", directory.toString()), e);
        }
        int untarResult = executor
                .runCommand("tar", "-xzvf", backup.toAbsolutePath().toString(),
                        "-C", directory.toString());
        if (untarResult != 0) {
            throw new PrestoManagerException("Failed to deploy config files", untarResult);
        }
    }

    /**
     * Add default Presto configuration in the given directories
     * <p>
     * If {@link #defaultConfig} exists, its contents will be used for
     * the configuration directory. Otherwise, non-configurable default
     * configuration files will be created.
     */
    public void deployDefaultConfig(Path configDir, Path dataDir, Path pluginDir)
            throws PrestoManagerException
    {
        try {
            if (isDirectory(defaultConfig)) {
                copyDir(defaultConfig, configDir);
                // TODO: Include node.id in install requests
                updateProperty(configDir.resolve("node.properties"),
                        "node.id", randomUUID().toString());
            }
            else {
                deleteDirectoryContents(configDir);
                createConfigPropertiess(configDir);
                createNodeProperties(configDir, pluginDir, dataDir);
                addJvmConfig(configDir);
            }
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to add config files", e);
        }
    }

    /**
     * Add default Presto connector configuration in the given directory
     * <p>
     * If {@link #defaultCatalog} exists, its contents will be used for
     * the configuration directory. Otherwise, non-configurable default
     * connector files will be created.
     */
    public void deployDefaultConnectors(Path catalogDir)
            throws PrestoManagerException
    {
        try {
            if (isDirectory(defaultCatalog)) {
                copyDir(defaultCatalog, catalogDir);
            }
            else {
                crateTpchCatalog(catalogDir);
            }
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to add connectors", e);
        }
    }

    private static void createPropertiesFile(Properties properties, Path path, String comments)
            throws PrestoManagerException
    {
        try (OutputStream output = newOutputStream(path, CREATE)) {
            properties.store(output, comments);
        }
        catch (IOException e) {
            throw new PrestoManagerException(format("Failed to create properties file: %s", path.getFileName()), e);
        }
    }

    /* The following functions all create configuration files */

    private static void crateTpchCatalog(Path catalogDir)
            throws IOException
    {
        if (!isDirectory(catalogDir)) {
            createDirectories(catalogDir);
            write(catalogDir.resolve("tpch.properties"), "connector.name=tpch".getBytes());
        }
    }

    private static void createConfigPropertiess(Path configDir)
            throws PrestoManagerException
    {
        // TODO: Add discovery.uri
        Properties properties = new Properties();
        properties.setProperty("coordinator", "false");
        properties.setProperty("http-server.http.port", "8080");
        properties.setProperty("query.max-memory", "50GB");
        properties.setProperty("query.max-memory-per-node", "8GB");
        createPropertiesFile(properties, configDir.resolve("config.properties"), "single node worker config");
    }

    private static void createNodeProperties(Path configDir, Path dataDir, Path pluginDir)
            throws PrestoManagerException
    {
        // TODO: Add node.server-log-file & node.launcher-log-file
        Properties properties = new Properties();
        properties.setProperty("node.environment", "presto");
        properties.setProperty("node.id", randomUUID().toString());
        properties.setProperty("node.data-dir", dataDir.toString());
        properties.setProperty("catalog.config-dir", configDir.resolve("catalog").toString());
        properties.setProperty("plugin.dir", pluginDir.toString());
        createPropertiesFile(properties, configDir.resolve("node.properties"), null);
    }

    private static void addJvmConfig(Path configDir)
            throws PrestoManagerException
    {
        String jvmConfig = "-server\n" +
                "-Xmx16G\n" +
                "-XX:-UseBiasedLocking\n" +
                "-XX:+UseG1GC\n" +
                "-XX:G1HeapRegionSize=32M\n" +
                "-XX:+ExplicitGCInvokesConcurrent\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+UseGCOverheadLimit\n" +
                "-XX:+ExitOnOutOfMemoryError\n" +
                "-XX:ReservedCodeCacheSize=512M\n" +
                "-DHADOOP_USER_NAME=hive";
        try {
            write(configDir.resolve("jvm.config"), jvmConfig.getBytes());
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to add jvm.config", e);
        }
    }
}
