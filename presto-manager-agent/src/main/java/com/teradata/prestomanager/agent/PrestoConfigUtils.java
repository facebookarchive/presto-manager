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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static com.google.common.io.MoreFiles.deleteDirectoryContents;
import static com.teradata.prestomanager.agent.AgentFileUtils.copyDir;
import static com.teradata.prestomanager.agent.AgentFileUtils.updateProperty;
import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static java.io.File.createTempFile;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.Files.write;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.UUID.randomUUID;

public class PrestoConfigUtils
{
    private static final Path DEFAULT_CONFIG_DIR = Paths.get("/presto-manager/config");
    private static final Path DEFAULT_CATALOG_DIR = Paths.get("/presto-manager/config/catalog");
    private static final Path DATA_DIR = Paths.get("/var/lib/presto/data");
    private static final Path PLUGIN_DIR = Paths.get("/usr/lib/presto/lib/plugin");

    private PrestoConfigUtils() {}

    public static File storeConfigFiles(Path configDir)
            throws PrestoManagerException
    {
        File tempFile;
        try {
            tempFile = createTempFile("PrestoConfigs", ".tar.gz");
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to create temp file.", e);
        }
        int tarResult = executeCommand("tar", "-czvf", tempFile.getAbsolutePath(), "-C", configDir.toString(), ".");
        if (tarResult != 0) {
            throw new PrestoManagerException("Failed to tar config files.", tarResult);
        }
        return tempFile;
    }

    public static void deployConfigFiles(File configTar, Path configDir)
            throws PrestoManagerException
    {
        try {
            deleteDirectoryContents(configDir);
        }
        catch (IOException e) {
            throw new PrestoManagerException(format("Failed to delete contents of the directory: %s", configDir.toString()), e);
        }
        int untarResult = executeCommand("tar", "-xzvf", configTar.getAbsolutePath(), "-C", configDir.toString());
        if (untarResult != 0) {
            throw new PrestoManagerException("Failed to deploy config files", untarResult);
        }
    }

    public static void addConfigFiles(Path configDir)
            throws PrestoManagerException
    {
        try {
            if (isDirectory(DEFAULT_CONFIG_DIR)) {
                copyDir(DEFAULT_CONFIG_DIR, configDir);
                updateProperty(configDir.resolve("node.properties"), "node.id", randomUUID().toString());
            }
            else {
                deleteDirectoryContents(configDir);
                addWorkerConfig(configDir);
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

    public static void addConnectors(Path catalogDir)
            throws PrestoManagerException
    {
        try {
            if (isDirectory(DEFAULT_CATALOG_DIR)) {
                copyDir(DEFAULT_CATALOG_DIR, catalogDir);
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

    private static void addWorkerConfig(Path configDir)
            throws PrestoManagerException
    {
        addConfigProperties(configDir);
        addNodeProperties(configDir);
        addJvmConfig(configDir);
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

    private static void addNodeProperties(Path configDir)
            throws PrestoManagerException
    {
        // TODO: Add node.server-log-file & node.launcher-log-file
        Properties properties = new Properties();
        properties.setProperty("node.environment", "presto");
        properties.setProperty("node.id", randomUUID().toString());
        properties.setProperty("node.data-dir", DATA_DIR.toString());
        properties.setProperty("catalog.config-dir", configDir.resolve("catalog").toString());
        properties.setProperty("plugin.dir", PLUGIN_DIR.toString());
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
