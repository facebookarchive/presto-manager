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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.collectingAndThen;

public final class AgentFileUtils
{
    private AgentFileUtils() {}

    public static List<String> getFileNameList(Path path)
            throws IOException
    {
        checkArgument(path.toFile().isDirectory(), "%s is not a directory", path);
        ImmutableList<String> fileNames;
        try (Stream<Path> stream = Files.list(path)) {
            fileNames = stream.filter(Files::isRegularFile).map(Path::getFileName).map(Path::toString)
                    .collect(collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
        }
        return fileNames;
    }

    public static String getFile(Path path)
            throws IOException
    {
        String line;
        StringJoiner stringJoiner = new StringJoiner("\n");
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path.toString()))) {
            while ((line = bufferedReader.readLine()) != null) {
                stringJoiner.add(line);
            }
        }
        return stringJoiner.toString();
    }

    public static void replaceFile(Path path, String url)
            throws IOException
    {
        URL website = new URL(url);
        File tempFile = File.createTempFile("PrestoMgrTemp", ".tmp");
        try (ReadableByteChannel readableByteChannel = Channels.newChannel(website.openStream());
                FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
            fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            Path originalCopy = path;
            Path newCopy = tempFile.toPath();
            Files.copy(newCopy, originalCopy, REPLACE_EXISTING);
        }
        finally {
            tempFile.delete();
        }
    }

    public static void updateProperty(Path path, String property, String value)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(path.toString())) {
            properties.load(inputStream);
        }
        properties.setProperty(property, value);
        try (OutputStream outputStream = new FileOutputStream(path.toString())) {
            properties.store(outputStream, null);
        }
    }

    public static void deleteFile(Path path)
            throws IOException
    {
        checkArgument(path.toFile().isFile(), "%s is not a file", path);
        Files.delete(path);
    }

    public static String getFileProperty(Path path, String property)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(path.toString())) {
            properties.load(inputStream);
        }
        if (!properties.containsKey(property)) {
            throw new NoSuchElementException("Property does not exist");
        }
        return properties.getProperty(property);
    }

    public static void removePropertyFromFile(Path path, String property)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(path.toString())) {
            properties.load(inputStream);
            if (properties.remove(property) == null) {
                throw new NoSuchElementException("Property does not exist");
            }
        }
        try (OutputStream outputStream = new FileOutputStream(path.toString())) {
            properties.store(outputStream, null);
        }
    }
}
