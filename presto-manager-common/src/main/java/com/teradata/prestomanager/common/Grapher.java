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
package com.teradata.prestomanager.common;

import java.io.IOException;
import java.io.File;
import java.io.PrintWriter;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.grapher.graphviz.GraphvizGrapher;
import com.google.inject.grapher.graphviz.GraphvizModule;

public class Grapher
{
    public static void graph(String filename, Injector demoInjector)
            throws IOException
    {
        PrintWriter out = new PrintWriter(new File(filename), "UTF-8");

        GraphvizGrapher grapher = Guice
                .createInjector(new GraphvizModule())
                .getInstance(GraphvizGrapher.class);

        grapher.setOut(out);
        grapher.setRankdir("TB");
        grapher.graph(demoInjector);
    }
}
