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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.teradata.prestomanager.common.json.JsonResponseReader;

import javax.ws.rs.client.Client;

import java.net.URL;
import java.util.List;
import java.util.Set;

// TODO: Add parameters to fail all operations with an exception
public class TestPackageController
        extends PackageController
{
    private boolean running;
    private boolean installed;
    private String version;

    @Inject
    TestPackageController(Client client,
            JsonResponseReader reader,
            PrestoInformer informer,
            @ForTesting List<String> args)
    {
        super(client, reader, informer);

        Set<String> params = ImmutableSet.copyOf(args);

        running = params.contains("running");
        installed = params.contains("installed");

        for (String param : params) {
            if (param.startsWith("v")) {
                version = param;
            }
        }
    }

    @Override
    protected void installAsync(URL url, boolean checkDependencies)
            throws PrestoManagerException
    {}

    @Override
    protected void uninstallAsync(boolean checkDependencies)
            throws PrestoManagerException
    {}

    @Override
    protected void upgradeAsync(URL url, boolean checkDependencies, boolean preserveConfig)
            throws PrestoManagerException
    {}

    @Override
    protected void startAsync()
            throws PrestoManagerException
    {}

    @Override
    protected void terminate()
            throws PrestoManagerException
    {}

    @Override
    protected void kill()
            throws PrestoManagerException
    {}

    @Override
    protected void restartAsync()
            throws PrestoManagerException
    {}

    @Override
    protected String getVersion()
            throws PrestoManagerException
    {
        return version;
    }

    @Override
    protected boolean isInstalled()
            throws PrestoManagerException
    {
        return installed;
    }

    @Override
    protected boolean isRunning()
            throws PrestoManagerException
    {
        return running;
    }
}
