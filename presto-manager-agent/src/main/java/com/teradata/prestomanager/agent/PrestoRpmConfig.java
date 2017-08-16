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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class PrestoRpmConfig
        implements PrestoConfig
{
    /**
     * These variables are not configurable for RPM installation
     * because they are hard-coded in presto-server-rpm init.d scripts
     */
    private static final Path INSTALLATION_DIRECTORY = Paths.get("/usr/lib/presto");
    private static final Path CONFIG_DIRECTORY = Paths.get("/etc/presto");

    private Path catalogDirectory = Paths.get("/etc/presto/catalog");
    private Path dataDirectory;
    private Path logDirectory = Paths.get("/var/log/presto");

    public Path getInstallationDirectory()
    {
        return INSTALLATION_DIRECTORY;
    }

    public Path getConfigDirectory()
    {
        return CONFIG_DIRECTORY;
    }

    @Config("catalog-dir")
    public PrestoRpmConfig setCatalogDirectory(String path)
    {
        catalogDirectory = Paths.get(path);
        return this;
    }

    public Path getCatalogDirectory()
    {
        return catalogDirectory;
    }

    @Config("data-dir")
    public PrestoRpmConfig setDataDirectory(String path)
    {
        dataDirectory = Paths.get(path);
        return this;
    }

    @NotNull
    public Path getDataDirectory()
    {
        return dataDirectory;
    }

    @Config("log-dir")
    public PrestoRpmConfig setLogDirectory(String path)
    {
        this.logDirectory = Paths.get(path);
        return this;
    }

    public Path getLogDirectory()
    {
        return logDirectory;
    }

    public Optional<Path> getLauncherPropertiesPath()
    {
        return Optional.empty();
    }
}
