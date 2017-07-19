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

import com.google.common.collect.ImmutableMap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Class for filtering the contents of log files
 * <p>
 * Uses a regular expression to match log entries, and treats
 * non-matching lines as part of the entry on the previous line.
 *
 * @see Builder
 */
public class LogFilter
{
    private final Path file;
    private final Pattern logPattern;
    private final String defaultEntry;
    private final String lineSeparator;
    private final ImmutableMap<String, Predicate<String>> namedGroupFilters;
    private final int maxEntries;
    private final boolean keepFirst;

    private LogFilter(Path file, Pattern logPattern,
            String defaultEntry, String lineSeparator,
            Map<String, Predicate<String>> namedGroupFilters,
            int maxEntries, boolean keepFirst)
            throws FileNotFoundException
    {
        this.file = requireNonNull(file);
        this.logPattern = requireNonNull(logPattern);
        this.defaultEntry = requireNonNull(defaultEntry);
        this.lineSeparator = requireNonNull(lineSeparator);
        this.namedGroupFilters = ImmutableMap.copyOf(requireNonNull(namedGroupFilters));
        this.maxEntries = maxEntries;
        this.keepFirst = keepFirst;
        if (!Files.isRegularFile(file)) {
            throw new FileNotFoundException(file.toString());
        }

        Matcher matcher = logPattern.matcher(defaultEntry);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Default entry does not match log entry pattern");
        }
        try {
            for (Map.Entry<String, Predicate<String>> f
                    : namedGroupFilters.entrySet()) {
                matcher.group(f.getKey());
            }
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid capturing group in log filter", e);
        }
    }

    public Stream<String> streamEntries()
            throws IOException
    {
        LogEntries logEntryBuilders = new LogEntries();
        Files.lines(file).forEachOrdered(logEntryBuilders::addLine);
        return logEntryBuilders.streamEntries().map(StringJoiner::toString);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * A builder for creating {@link LogFilter LogFilters}.
     * <p>
     * Builder instances can be safely reused; {@link #build()} may be called
     * multiple times to build multiple filters.
     */
    public static class Builder
    {
        private Path file;
        private Pattern logPattern;
        private String defaultEntry;
        private String lineSeparator = System.getProperty("line.separator");
        private ImmutableMap.Builder<String, Predicate<String>> namedGroupFilters;
        private int maxEntries = Integer.MAX_VALUE;
        private boolean keepFirst;

        private Builder()
        {
            namedGroupFilters = ImmutableMap.builder();
        }

        public LogFilter build()
                throws FileNotFoundException
        {
            return new LogFilter(file, logPattern,
                    defaultEntry, lineSeparator,
                    namedGroupFilters.build(),
                    maxEntries, keepFirst);
        }

        public Builder setFile(Path file)
        {
            this.file = requireNonNull(file);
            return this;
        }

        /**
         * Use the given regular expression to match each line of a log file
         */
        public Builder setPattern(Pattern pattern)
        {
            this.logPattern = requireNonNull(pattern);
            return this;
        }

        /**
         * Set an entry that will be inserted if the first entry does
         * not match the entry pattern.
         *
         */
        public Builder setDefaultEntry(String entry)
        {
            defaultEntry = requireNonNull(entry);
            return this;
        }

        /**
         * Add a filter to apply to a named capturing group in the
         * regular expression for log file lines.
         * <p>
         * A filter of `s -> true' will not have the same effect
         * as having no filter; only if there is no filter will
         * a mis-formatted initial log entry be included in results.
         */
        public Builder addGroupFilter(String groupName, Predicate<String> filter)
        {
            namedGroupFilters.put(
                    requireNonNull(groupName),
                    requireNonNull(filter));
            return this;
        }

        /**
         * Use this string to separate lines in the same entry
         */
        public Builder setLineSeparator(String separator)
        {
            lineSeparator = requireNonNull(separator);
            return this;
        }

        /**
         * Set the maximum number of entries to keep when filtering.
         */
        public Builder setCapacity(int capacity)
        {
            maxEntries = capacity;
            return this;
        }

        /**
         * When there is a maximum capacity, keep the first entries
         * instead of the last entries. Default false.
         */
        public Builder keepFirst(boolean shouldKeepFirst)
        {
            keepFirst = shouldKeepFirst;
            return this;
        }
    }

    /**
     * Class representing a collection of log entries
     */
    private final class LogEntries
    {
        private Deque<StringJoiner> entries;
        private Boolean keptLastLine;
        private Matcher matcher;

        private LogEntries()
        {
            entries = new ArrayDeque<>();
            matcher = logPattern.matcher("");
        }

        Stream<StringJoiner> streamEntries()
        {
            return entries.stream();
        }

        /**
         * Add a line from a log file to a deque of log entries
         * <p>
         * If the string does not match {@link #logPattern}, it will be
         * appended to the previous entry. If there is no previous entry, a new
         * entry will be created (using the default entry) only if the default
         * entry matches the filters.
         * <p>
         * If {@link #maxEntries} is non-null and the deque contains that many
         * entries, the oldest will be removed before the new entry is added.
         */
        void addLine(String line)
        {
            if (matcher.reset(line).matches()) {
                if (checkFilters(matcher)) {
                    keptLastLine = addNewEntry(line);
                }
                else {
                    keptLastLine = false;
                }
            }
            else if (keptLastLine == null) {
                if (matcher.reset(defaultEntry).matches() && checkFilters(matcher)) {
                    keptLastLine = addNewEntry(defaultEntry);
                    keptLastLine = addToLast(line);
                }
                else {
                    keptLastLine = false;
                }
            }
            else if (keptLastLine) {
                keptLastLine = addToLast(line);
            }
            else {
                keptLastLine = false;
            }
        }

        /**
         * Checks if the string currently in the given matcher
         * passes all of the filters. The matcher must already
         * matching state.
         */
        private boolean checkFilters(Matcher matcher)
        {
            boolean passing = true;
            for (ImmutableMap.Entry<String, Predicate<String>> f
                    : namedGroupFilters.entrySet()) {
                passing = passing && f.getValue().test(matcher.group(f.getKey()));
            }
            return passing;
        }

        /**
         * @return true if the line addition succeeded
         */
        private boolean addNewEntry(String line)
        {
            if (entries.size() >= maxEntries) {
                if (keepFirst) {
                    return false;
                }
                else if (maxEntries == 0) {
                    return true;
                }
                entries.removeFirst();
            }
            StringJoiner joiner = new StringJoiner(lineSeparator);
            joiner.add(line);
            entries.addLast(joiner);
            return true;
        }

        /**
         * @return true if the line addition succeeded
         * (equivalently, true if any entries already exist)
         */
        private boolean addToLast(String line)
        {
            boolean entriesExist = !entries.isEmpty();
            if (entriesExist) {
                entries.getLast().add(line);
            }
            return entriesExist;
        }
    }
}
