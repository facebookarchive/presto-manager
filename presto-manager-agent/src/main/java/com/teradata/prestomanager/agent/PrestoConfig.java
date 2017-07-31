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

public class PrestoConfig
{
    private Path configurationDirectory;
    private Path catalogDirectory;
    private Path dataDirectory;
    private Path pluginDirectory;
    private Path logDirectory;
    private Path configProperties;
    private Path launcherScript;

    @Config("config-dir")
    public PrestoConfig setConfigurationDirectory(String path)
    {
        configurationDirectory = Paths.get(path);
        return this;
    }

    @NotNull
    public Path getConfigurationDirectory()
    {
        return configurationDirectory;
    }

    @Config("data-dir")
    public PrestoConfig setDataDirectory(String path)
    {
        dataDirectory = Paths.get(path);
        return this;
    }

    @NotNull
    public Path getDataDirectory()
    {
        return dataDirectory;
    }

    @Config("plugin-dir")
    public PrestoConfig setPluginDirectory(String path)
    {
        pluginDirectory = Paths.get(path);
        return this;
    }

    @NotNull
    public Path getPluginDirectory()
    {
        return pluginDirectory;
    }

    @Config("catalog-dir")
    public PrestoConfig setCatalogDirectory(String path)
    {
        catalogDirectory = Paths.get(path);
        return this;
    }

    @NotNull
    public Path getCatalogDirectory()
    {
        return catalogDirectory;
    }

    @Config("log-dir")
    public PrestoConfig setLogDirectory(String path)
    {
        this.logDirectory = Paths.get(path);
        return this;
    }

    @NotNull
    public Path getLogDirectory()
    {
        return logDirectory;
    }

    @Config("launcher")
    public PrestoConfig setLauncherPath(String path)
    {
        this.launcherScript = Paths.get(path);
        return this;
    }

    @NotNull
    public Path getLauncherPath()
    {
        return launcherScript;
    }
}
