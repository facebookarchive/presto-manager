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
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteDirectoryContents;
import static com.teradata.prestomanager.agent.CommandExecutor.executeCommand;
import static java.io.File.createTempFile;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.write;

public class PrestoConfigUtils
{
    private PrestoConfigUtils() {}

    public static void addTpchCatalog(Path catalogDir)
            throws PrestoManagerException
    {
        try {
            if (!isDirectory(catalogDir)) {
                createDirectories(catalogDir);
                write(catalogDir.resolve("tpch.properties"), "connector.name=tpch".getBytes());
            }
        }
        catch (IOException e) {
            throw new PrestoManagerException("Failed to add tpch catalog", e);
        }
    }

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
}
