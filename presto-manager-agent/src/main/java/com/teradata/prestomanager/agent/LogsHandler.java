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

import javax.ws.rs.core.Response;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.teradata.prestomanager.common.SimpleResponses.badRequest;
import static com.teradata.prestomanager.common.SimpleResponses.notFound;
import static com.teradata.prestomanager.common.SimpleResponses.serverError;
import static java.util.Objects.requireNonNull;

/**
 * Utility class managing retrieval and deletion of Presto logs
 */
public class LogsHandler
{
    private static final Logger LOG = Logger.get(LogsHandler.class);

    public static final String DEFAULT_LOG_LEVEL = "ALL";

    private static final DateTimeFormatter DATE_FORMAT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive().parseStrict()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .appendPattern("Z")
            .toFormatter().withChronology(IsoChronology.INSTANCE);

    private static final String DATE_GROUP = "date";
    private static final String LEVEL_GROUP = "level";

    private final Path logDirectory;
    private final Pattern logPattern;
    private final String defaultEntry;

    @Inject
    private LogsHandler(AgentConfig config, PrestoConfig prestoConfig)
    {
        logDirectory = requireNonNull(prestoConfig.getLogDirectory());
        defaultEntry = requireNonNull(config.getDefaultLogEntry());
        logPattern = Pattern.compile(requireNonNull(config.getLogEntryPattern()));

        Matcher matcher = logPattern.matcher(defaultEntry);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Default entry does not match log entry pattern");
        }
        try {
            matcher.group(DATE_GROUP);
            matcher.group(LEVEL_GROUP);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Log pattern should have groups named \"date\" and \"level\"", e);
        }
    }

    public Response getLogList()
    {
        List<String> fileList;
        try (Stream<Path> files = Files.list(logDirectory)) {
            fileList = files.filter(Files::isRegularFile)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .collect(toImmutableList());
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
    public Response getLogs(String filename, Instant start,
            Instant end, String logLevel, Integer maxEntries)
    {
        requireNonNull(logLevel);

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
            filePath = logDirectory.resolve(filename);
        }
        catch (InvalidPathException e) {
            return badRequest("Invalid file name");
        }

        LogFilter logFilter;
        try {
            logFilter = LogFilter.builder()
                    .setFile(filePath)
                    .setPattern(logPattern)
                    .setDefaultEntry(defaultEntry)
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

        List<String> result = logEntries.collect(toImmutableList());

        return Response.ok(result).build();
    }

    /**
     * Method called in response to DELETE request
     */
    public Response deleteLogs(String filename, Instant end)
    {
        Path filePath;
        try {
            filePath = logDirectory.resolve(filename);
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

    private Response deleteLogRange(Path filePath, Instant end)
    {
        requireNonNull(filePath);
        requireNonNull(end);

        LogFilter logFilter;
        try {
            logFilter = LogFilter.builder()
                    .setFile(filePath)
                    .setPattern(logPattern)
                    .setDefaultEntry(defaultEntry)
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
