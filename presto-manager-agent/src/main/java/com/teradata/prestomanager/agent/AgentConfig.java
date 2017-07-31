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

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;

public class AgentConfig
{
    private PackageType packageType;
    private Path defaultConfigurationDirectory;
    private Path defaultCatalogDirectory;
    private int longSubprocessTimeout;
    private int shortSubprocessTimeout;
    private String defaultLogEntry;
    private String logEntryPattern;

    @Config("packaging")
    public AgentConfig setPackageType(PackageType packageType)
    {
        this.packageType = packageType;
        return this;
    }
    @NotNull
    public PackageType getPackageType()
    {
        return packageType;
    }

    @Config("defaults.config-dir")
    public AgentConfig setDefaultConfigurationDirectory(String path)
    {
        defaultConfigurationDirectory = Paths.get(path);
        return this;
    }
    @NotNull
    public Path getDefaultConfigurationDirectory()
    {
        return defaultConfigurationDirectory;
    }

    @Config("defaults.catalog-dir")
    public AgentConfig setDefaultCatalogDirectory(String path)
    {
        defaultCatalogDirectory = Paths.get(path);
        return this;
    }
    @NotNull
    public Path getDefaultCatalogDirectory()
    {
        return defaultCatalogDirectory;
    }

    @Config("subprocess-timeout-seconds.long")
    public AgentConfig setLongSubprocessTimeout(int subprocessTimeout)
    {
        this.longSubprocessTimeout = subprocessTimeout;
        return this;
    }
    @Min(value = 0)
    public int getLongSubprocessTimeout()
    {
        return longSubprocessTimeout;
    }

    @Config("subprocess-timeout-seconds.short")
    public AgentConfig setShortSubprocessTimeout(int subprocessTimeout)
    {
        this.shortSubprocessTimeout = subprocessTimeout;
        return this;
    }
    @Min(value = 0)
    public int getShortSubprocessTimeout()
    {
        return shortSubprocessTimeout;
    }

    @Config("log-entry.default")
    public AgentConfig setDefaultLogEntry(String defaultLogEntry)
    {
        this.defaultLogEntry = defaultLogEntry;
        return this;
    }
    @NotNull
    public String getDefaultLogEntry()
    {
        return defaultLogEntry;
    }

    @Config("log-entry.pattern")
    public AgentConfig setLogEntryPattern(String logEntryPattern)
    {
        this.logEntryPattern = logEntryPattern;
        return this;
    }
    @NotNull
    public String getLogEntryPattern()
    {
        return logEntryPattern;
    }
}
