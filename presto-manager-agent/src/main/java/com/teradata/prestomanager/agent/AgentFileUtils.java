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

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public final class AgentFileUtils
{
    private AgentFileUtils() {}

    public static List<String> getFileNameList(String path)
    {
        File folder = new File(path);
        checkArgument(folder.isDirectory(), "%s is not a directory", path);
        ImmutableList.Builder<String> fileNames = ImmutableList.builder();
        for (int i = 0; i < folder.list().length; i++) {
            fileNames.add(folder.list()[i]);
        }
        return fileNames.build();
    }

    public static Properties getFile(String path)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(path)) {
            properties.load(inputStream);
        }
        return properties;
    }

    public static void replaceFile(String path, String url)
            throws IOException
    {
        URL website = new URL(url);
        File tempFile = File.createTempFile("PrestoMgrTemp", ".tmp");
        try (ReadableByteChannel readableByteChannel = Channels.newChannel(website.openStream());
                FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
            fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            Path originalCopy = Paths.get(path);
            Path newCopy = tempFile.toPath();
            Files.copy(newCopy, originalCopy, REPLACE_EXISTING);
        }
        finally {
            tempFile.delete();
        }
    }

    public static void updateProperty(String path, String property, String value)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(path)) {
            properties.load(inputStream);
        }
        properties.setProperty(property, value);
        try (OutputStream outputStream = new FileOutputStream(path)) {
            properties.store(outputStream, null);
        }
    }

    public static void deleteFile(String path)
            throws IOException
    {
        checkArgument(new File(path).isFile(), "%s is not a file", path);
        Files.delete(Paths.get(path));
    }

    public static String getFileProperty(String path, String property)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(path)) {
            properties.load(inputStream);
        }
        if (!properties.containsKey(property)) {
            throw new NoSuchElementException("Property does not exist");
        }
        return properties.getProperty(property);
    }

    public static void removePropertyFromFile(String path, String property)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(path)) {
            properties.load(inputStream);
            if (properties.remove(property) == null) {
                throw new NoSuchElementException("Property does not exist");
            }
        }
        try (OutputStream outputStream = new FileOutputStream(path)) {
            properties.store(outputStream, null);
        }
    }
}
