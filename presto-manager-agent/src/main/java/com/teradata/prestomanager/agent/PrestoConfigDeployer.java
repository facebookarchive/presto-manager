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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Properties;

import static com.google.common.io.MoreFiles.deleteDirectoryContents;
import static com.teradata.prestomanager.agent.AgentFileUtils.copyDir;
import static com.teradata.prestomanager.agent.AgentFileUtils.updateProperty;
import static java.io.File.createTempFile;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
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

    public File storeConfigFiles(Path configDir)
            throws PrestoManagerException
    {
        File tempFile;
        try {
            tempFile = createTempFile("PrestoConfigs", ".tar.gz");
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to create temp file.", e);
        }
        int tarResult = executor
                .runCommand("tar", "-czvf", tempFile.getAbsolutePath(),
                        "-C", configDir.toString(), ".");
        if (tarResult != 0) {
            throw new PrestoManagerException("Failed to tar config files.", tarResult);
        }
        return tempFile;
    }

    public void deployConfigFiles(File configTar, Path configDir)
            throws PrestoManagerException
    {
        try {
            deleteDirectoryContents(configDir);
        }
        catch (IOException e) {
            throw new PrestoManagerException(format("Failed to delete contents of the directory: %s", configDir.toString()), e);
        }
        int untarResult = executor
                .runCommand("tar", "-xzvf", configTar.getAbsolutePath(),
                        "-C", configDir.toString());
        if (untarResult != 0) {
            throw new PrestoManagerException("Failed to deploy config files", untarResult);
        }
    }

    public void addConfigFiles(Path configDir, Path dataDir, Path pluginDir)
            throws PrestoManagerException
    {
        try {
            if (isDirectory(defaultConfig)) {
                copyDir(defaultConfig, configDir);
                updateProperty(configDir.resolve("node.properties"), "node.id", randomUUID().toString());
            }
            else {
                deleteDirectoryContents(configDir);
                addConfigProperties(configDir);
                addNodeProperties(configDir, pluginDir, dataDir);
                addJvmConfig(configDir);
            }
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to add config files", e);
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

    public void addConnectors(Path catalogDir)
            throws PrestoManagerException
    {
        try {
            if (isDirectory(defaultCatalog)) {
                copyDir(defaultCatalog, catalogDir);
            }
            else {
                addTpchCatalog(catalogDir);
            }
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to add connectors", e);
        }
    }

    private static void addTpchCatalog(Path catalogDir)
            throws IOException
    {
        if (!isDirectory(catalogDir)) {
            createDirectories(catalogDir);
            write(catalogDir.resolve("tpch.properties"), "connector.name=tpch".getBytes());
        }
    }

    private static void addConfigProperties(Path configDir)
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

    private static void addNodeProperties(Path configDir, Path dataDir, Path pluginDir)
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
