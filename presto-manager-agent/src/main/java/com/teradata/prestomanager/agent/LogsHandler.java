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
import io.airlift.log.Logger;

import javax.ws.rs.core.Response;

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

import static com.teradata.prestomanager.common.SimpleResponses.badRequest;
import static com.teradata.prestomanager.common.SimpleResponses.notFound;
import static com.teradata.prestomanager.common.SimpleResponses.serverError;
import static java.util.Objects.requireNonNull;

/**
 * Utility class managing retrieval and deletion of Presto logs
 */
public final class LogsHandler
{
    private static final Logger LOG = Logger.get(LogsHandler.class);

    public static final String DEFAULT_LOG_LEVEL = "ALL";

    // TODO: Make most of these 'private static final' values configurable
    private static final Path LOG_DIRECTORY = Paths.get("/var/log/presto");

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
            LOG.error(e, "Configured log directory does not exist");
            return serverError("Configured log directory does not exist");
        }
        catch (NotDirectoryException e) {
            LOG.error(e, "Configured log directory is not a directory");
            return serverError("Configured log directory is not a directory");
        }
        catch (IOException e) {
            LOG.warn(e, "IOException getting file list");
            return serverError("IOException getting file list");
        }
        return Response.ok(fileList).build();
    }

    /**
     * Method called in response to GET request
     */
    public static Response getLogs(String filename, JaxrsParameter<Instant> startDate,
            JaxrsParameter<Instant> endDate, String logLevel, Integer maxEntries)
    {
        requireNonNull(startDate);
        requireNonNull(endDate);
        requireNonNull(logLevel);

        if (!startDate.isValid() || !endDate.isValid()) {
            return badRequest("Invalid date format");
        }
        Instant start = startDate.get();
        Instant end = endDate.get();

        if (start != null && end != null) {
            if (maxEntries != null) {
                return badRequest(
                        "Can not provide date range and limit number of entries");
            }
            else if (start.isAfter(end)) {
                return badRequest(
                        "End of date range (%s) is before start (%s)", start, end);
            }
        }

        Path filePath;
        try {
            filePath = LOG_DIRECTORY.resolve(filename);
        }
        catch (InvalidPathException e) {
            return badRequest("Invalid file name");
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
                    .addGroupFilter(LEVEL_GROUP, getFilter(logLevel))
                    .build();
        }
        catch (FileNotFoundException e) {
            return notFound(Files.exists(filePath)
                    ? "Not a regular file"
                    : "File not found");
        }
        catch (IllegalArgumentException e) {
            LOG.error(e, "Internal: Capturing group not present in log entry pattern");
            return serverError("Log parser configured incorrectly");
        }
        catch (DateTimeParseException e) {
            LOG.error(e, "Internal: Default log entry has invalid date");
            return serverError("Log parser configured incorrectly");
        }

        Stream<String> logEntries;
        try {
            logEntries = logFilter.streamEntries();
        }
        catch (IOException e) {
            LOG.warn(e, "IOException while reading file");
            return serverError("IOException while reading file");
        }
        catch (DateTimeParseException e) {
            LOG.warn(e, "Date in log file has invalid format");
            return serverError("Date in log file has invalid format");
        }

        String result = logEntries.collect(Collectors.joining("\r\n"));

        return Response.ok(result).build();
    }

    /**
     * Method called in response to DELETE request
     */
    public static Response deleteLogs(String filename, JaxrsParameter<Instant> endDate)
    {
        if (!endDate.isValid()) {
            return badRequest("Invalid date format");
        }
        Instant end = endDate.get();

        Path filePath;
        try {
            filePath = LOG_DIRECTORY.resolve(filename);
        }
        catch (InvalidPathException e) {
            return badRequest("Invalid file name");
        }

        if (end != null) {
            return deleteLogRange(filePath, end);
        }
        else {
            try {
                Files.write(filePath, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
                // TODO: Use Status.ACCEPTED for asynchronous response
                return Response.noContent().build();
            }
            catch (IOException e) {
                LOG.warn(e, "IOException while writing log file");
                return serverError("IOException while writing log file");
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
            return notFound(Files.exists(filePath)
                    ? "Not a regular file"
                    : "File not found");
        }
        catch (IllegalArgumentException e) {
            LOG.error(e, "Internal: Capturing group not present in log entry pattern");
            return serverError("Log parser configured incorrectly");
        }
        catch (DateTimeParseException e) {
            LOG.error(e, "Internal: Default log entry has invalid date");
            return serverError("Log parser configured incorrectly");
        }

        Stream<String> logEntries;
        try {
            logEntries = logFilter.streamEntries();
        }
        catch (IOException e) {
            LOG.warn(e, "IOException while reading file");
            return serverError("IOException while reading file");
        }
        catch (DateTimeParseException e) {
            LOG.warn(e, "Date in log file has invalid format");
            return serverError("Date in log file has invalid format");
        }

        try {
            Files.write(filePath, (Iterable<String>) logEntries::iterator);
        }
        catch (IOException e) {
            LOG.warn(e, "IOException while writing file");
            return serverError("IOException while writing file");
        }

        // TODO: Use Status.ACCEPTED for asynchronous response
        return Response.noContent().build();
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

    private static Predicate<String> getFilter(String logLevel)
    {
        if (logLevel == null || DEFAULT_LOG_LEVEL.equalsIgnoreCase(logLevel)) {
            return s -> true;
        }
        else {
            return logLevel::equalsIgnoreCase;
        }
    }
}
