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

import com.teradata.prestomanager.common.ServerBuilder;
import org.eclipse.jetty.server.Server;

import java.net.BindException;

class AgentServer
{
    // TODO: Replace placeholder constants with configurable properties
    private static final int PORT = 8081;
    private static final String URI = "http://localhost/";

    public static void main(String[] args) throws Exception
    {
        Server server = new ServerBuilder()
                .setURI(URI)
                .setPort(PORT)
                .build();

        try {
            server.start();
        }
        catch (BindException e) {
            // TODO: Log exception
        }
        try {
            server.join();
        }
        catch (InterruptedException e) {
            // TODO: Log exception
        }
        finally {
            server.stop();
        }
    }
}
