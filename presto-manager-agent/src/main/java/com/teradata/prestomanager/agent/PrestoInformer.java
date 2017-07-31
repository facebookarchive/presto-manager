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
import io.airlift.log.Logger;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.nio.file.Path;

import static com.teradata.prestomanager.agent.AgentFileUtils.getFileProperty;
import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.UriBuilder.fromUri;

public class PrestoInformer
{
    private static final Logger LOGGER = Logger.get(PrestoInformer.class);

    private final Path configFile;
    private final Client client;

    @Inject
    PrestoInformer(Client client, PrestoConfig config)
    {
        this.client = client;
        this.configFile = config.getConfigProperties();
    }

    public int getPrestoPort()
            throws IOException
    {
        return Integer.valueOf(getFileProperty(configFile, "http-server.http.port"));
    }

    /**
     * @return whether Presto is running as a coordinator
     * @throws IOException If Presto port number could not be retrieved
     */
    public boolean isRunningCoordinator()
            throws IOException
    {
        try {
            UriBuilder uriBuilder = fromUri(format("http://localhost:%s", getPrestoPort())).path("/v1/info/coordinator");
            Response isCoordinator = client.target(uriBuilder.build()).request(TEXT_PLAIN).buildGet().invoke();
            return isCoordinator.getStatus() == 200;
        }
        catch (ProcessingException e) {
            LOGGER.warn(e, "Presto is not running");
        }
        return false;
    }
}
