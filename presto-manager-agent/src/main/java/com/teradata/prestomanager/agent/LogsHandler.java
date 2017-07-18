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

import com.teradata.prestomanager.common.JaxrsParameter;
import org.apache.logging.log4j.Level;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Utility class managing retrieval and deletion of Presto logs
 */
public final class LogsHandler
{
    // TODO: Make _all_ of these 'private static final' values configurable
    private static final Path LOG_DIRECTORY = Paths.get("/var/log/presto");
    private static final Level LOG_ALL = Level.ALL;

    private static final DateTimeFormatter DATE_FORMAT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive().parseStrict()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .appendPattern("Z")
            .toFormatter().withChronology(IsoChronology.INSTANCE);

    // TODO: When these are configurable, check that the group names are present
    private static final String DATE_GROUP = "date";
    private static final String LEVEL_GROUP = "level";
    private static final Pattern LOG_REGEX = Pattern.compile(
            "^(?<date>[0-9]{4}-[0-9]{2}-[0-9]{2}T" +
                    "[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{3})?[+-][0-9]{4})" +
                    "\t(?<level>[A-Z]+)\t(?<thread>[^\t]+)" +
                    "\t(?<class>[^\t]+)\t(?<message>.*)$");
    private static final String DEFAULT_ENTRY = "0000-01-01T00:00:00.000+0000" +
            "\tALL\t[none]\t[none]\tThis log entry was not preceded by a header:";

    private LogsHandler() {}

    public static Response getLogList()
    {
        String fileList;
        try (Stream<java.nio.file.Path> files = Files.list(LOG_DIRECTORY)) {
            fileList = files.filter(Files::isRegularFile)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .collect(Collectors.joining("\r\n"));
        }
        catch (NoSuchFileException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Pre-configured log directory does not exist")
                    .build();
        }
        catch (NotDirectoryException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Pre-configured log directory is not a directory")
                    .build();
        }
        catch (IOException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("IOException getting file list")
                    .build();
        }
        return Response.status(Status.OK).entity(fileList).build();
    }

    /**
     * Method called in response to GET request
     */
    public static Response getLogs(String filename, JaxrsParameter<Instant> startDate,
            JaxrsParameter<Instant> endDate, JaxrsParameter<Level> logLevel,
            Integer maxEntries)
    {
        requireNonNull(startDate);
        requireNonNull(endDate);
        requireNonNull(logLevel);

        if (!startDate.isValid() || !endDate.isValid()) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid date format").build();
        }
        Instant start = startDate.get();
        Instant end = endDate.get();

        if (start != null && end != null) {
            if (maxEntries != null) {
                return Response.status(Status.BAD_REQUEST)
                        .entity("Can not provide date range and limit number of entries")
                        .build();
            }
            else if (start.isAfter(end)) {
                return Response.status(Status.BAD_REQUEST)
                        .entity("End of date range can not be before start")
                        .build();
            }
        }

        if (!logLevel.isValid()) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid log level").build();
        }
        Level level = logLevel.get();

        Path filePath;
        try {
            filePath = LOG_DIRECTORY.resolve(filename);
        }
        catch (InvalidPathException e) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid file name").build();
        }

        LogFilter logFilter;
        try {
            logFilter = LogFilter.builder()
                    .setFile(filePath)
                    .setPattern(LOG_REGEX)
                    .setDefaultEntry(DEFAULT_ENTRY)
                    .setLineSeparator("\r\n")
                    .setCapacity(maxEntries == null ? Integer.MAX_VALUE : maxEntries)
                    .keepFirst(start != null)
                    .addGroupFilter(DATE_GROUP, getFilter(start, end))
                    .addGroupFilter(LEVEL_GROUP, getFilter(level))
                    .build();
        }
        catch (FileNotFoundException e) {
            return Response.status(Status.NOT_FOUND)
                    .entity(Files.exists(filePath)
                            ? "Not a regular file"
                            : "File not found")
                    .build();
        }
        catch (IllegalArgumentException
                | IndexOutOfBoundsException | DateTimeParseException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Log parser configured incorrectly").build();
        }

        Stream<String> logEntries;
        try {
            logEntries = logFilter.streamEntries();
        }
        catch (IOException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("IOException while reading file").build();
        }
        catch (DateTimeParseException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Attempted to read malformed date in log").build();
        }

        String result = logEntries.collect(Collectors.joining("\r\n"));

        return Response.status(Status.OK).entity(result).build();
    }

    /**
     * Method called in response to DELETE request
     */
    public static Response deleteLogs(String filename, JaxrsParameter<Instant> endDate)
    {
        if (!endDate.isValid()) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid date format").build();
        }
        Instant end = endDate.get();

        Path filePath;
        try {
            filePath = LOG_DIRECTORY.resolve(filename);
        }
        catch (InvalidPathException e) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid file name").build();
        }

        if (end != null) {
            return deleteLogRange(filePath, end);
        }
        else {
            try {
                Files.write(filePath, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
                // TODO: Use Status.ACCEPTED for asynchronous response
                return Response.status(Status.NO_CONTENT).build();
            }
            catch (IOException e) {
                return Response.status(Status.INTERNAL_SERVER_ERROR)
                        .entity("IOException while writing file").build();
            }
        }
    }

    private static Response deleteLogRange(Path filePath, Instant end)
    {
        requireNonNull(filePath);
        requireNonNull(end);

        LogFilter logFilter;
        try {
            logFilter = LogFilter.builder()
                    .setFile(filePath)
                    .setPattern(LOG_REGEX)
                    .setDefaultEntry(DEFAULT_ENTRY)
                    .addGroupFilter(DATE_GROUP, getFilter(end, null))
                    .build();
        }
        catch (FileNotFoundException e) {
            return Response.status(Status.NOT_FOUND)
                    .entity(Files.exists(filePath)
                            ? "Not a regular file"
                            : "File not found")
                    .build();
        }
        catch (IllegalArgumentException
                | IndexOutOfBoundsException | DateTimeParseException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Log parser configured incorrectly").build();
        }

        Stream<String> logEntries;
        try {
            logEntries = logFilter.streamEntries();
        }
        catch (IOException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("IOException while reading file").build();
        }
        catch (DateTimeParseException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("Attempted to read malformed date in log").build();
        }

        try {
            Files.write(filePath, (Iterable<String>) logEntries::iterator);
        }
        catch (IOException e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                    .entity("IOException while writing file").build();
        }

        // TODO: Use Status.ACCEPTED for asynchronous response
        return Response.status(Status.NO_CONTENT).build();
    }

    private static Predicate<String> getFilter(Instant start, Instant end)
    {
        boolean nullStart = start == null;
        boolean nullEnd = end == null;
        if (nullStart && nullEnd) {
            return s -> true;
        }
        else if (nullStart) {
            return s -> DATE_FORMAT.parse(s, Instant::from).compareTo(end) <= 0;
        }
        else if (nullEnd) {
            return s -> DATE_FORMAT.parse(s, Instant::from).compareTo(start) >= 0;
        }
        else {
            return s -> {
                Instant date = DATE_FORMAT.parse(s, Instant::from);
                return date.compareTo(start) >= 0 && date.compareTo(end) <= 0;
            };
        }
    }

    private static Predicate<String> getFilter(Level level)
    {
        if (level == null || level == LOG_ALL) {
            return s -> true;
        }
        else {
            return s -> level.name().equals(s);
        }
    }
}
