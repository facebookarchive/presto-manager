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
import java.util.ConcurrentModificationException;
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
 *
 * Uses a regular expression to match log entries, and treats
 * non-matching lines as part of the entry on the previous line.
 *
 * @see Builder
 */
public class LogFilter
{
    private final Path file;
    private final Pattern logPattern;
    private final String lineSeparator;
    private final ImmutableMap<String, Predicate<String>> namedGroupFilters;
    private final ImmutableMap<Integer, Predicate<String>> numberedGroupFilters;
    private final int maxEntries;
    private final boolean keepFirst;

    private LogFilter(Path file, Pattern logPattern, String lineSeparator,
            Map<String, Predicate<String>> namedGroupFilters,
            Map<Integer, Predicate<String>> numberedGroupFilters,
            int maxEntries, boolean keepFirst)
            throws FileNotFoundException
    {
        this.file = requireNonNull(file);
        this.logPattern = requireNonNull(logPattern);
        this.lineSeparator = requireNonNull(lineSeparator);
        this.namedGroupFilters = ImmutableMap.copyOf(requireNonNull(namedGroupFilters));
        this.numberedGroupFilters = ImmutableMap.copyOf(requireNonNull(numberedGroupFilters));
        this.maxEntries = maxEntries;
        this.keepFirst = keepFirst;
        if (!Files.exists(file)) {
            throw new FileNotFoundException(file.toString());
        }
    }

    public Stream<String> streamEntries()
            throws IOException, ConcurrentModificationException
    {
        LogEntries logEntryBuilders = Files.lines(file).collect(
                LogEntries::new,
                LogEntries::addLine,
                (a, b) -> {
                    throw new ConcurrentModificationException();
                });
        return logEntryBuilders.streamEntries().map(StringJoiner::toString);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * A builder for creating {@link LogFilter LogFilters}.
     *
     * Builder instances can be safely reused; {@link #build()} may be called
     *  multiple times to build multiple filters.
     */
    public static class Builder
    {
        private Path file;
        private Pattern logPattern;
        private String lineSeparator = System.getProperty("line.separator");
        private ImmutableMap.Builder<String, Predicate<String>> namedGroupFilters = ImmutableMap.builder();
        private ImmutableMap.Builder<Integer, Predicate<String>> numberedGroupFilters = ImmutableMap.builder();
        private int maxEntries = Integer.MAX_VALUE;
        private boolean keepFirst = false;

        private Builder() {}

        public LogFilter build()
                throws FileNotFoundException
        {
            return new LogFilter(file, logPattern, lineSeparator,
                    namedGroupFilters.build(),
                    numberedGroupFilters.build(),
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
         * Add a filter to apply to a named capturing group
         * in the regular expression for log file lines
         */
        public Builder addGroupFilter(String groupName, Predicate<String> filter)
        {
            namedGroupFilters.put(
                    requireNonNull(groupName),
                    requireNonNull(filter));
            return this;
        }

        /**
         * Add a filter to apply to a capturing group in
         * the regular expression for log file lines
         */
        public Builder addGroupFilter(Integer groupNumber, Predicate<String> filter)
        {
            numberedGroupFilters.put(
                    requireNonNull(groupNumber),
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
         *
         * If the string does not match {@link #logPattern}, it will be
         * appended to the previous entry. If there is no previous entry, a new
         * entry will be created only if entries are not being filtered.
         *
         * If {@link #maxEntries} is non-null and the deque contains that many
         * entries, the oldest will be removed before the new entry is added.
         */
        void addLine(String line)
        {
            matcher.reset(line);
            if (matcher.matches()) {
                /* Apply all filters */
                boolean passing = true;
                for (ImmutableMap.Entry<String, Predicate<String>> f
                        : namedGroupFilters.entrySet()) {
                    passing = passing && f.getValue().test(matcher.group(f.getKey()));
                }
                for (ImmutableMap.Entry<Integer, Predicate<String>> f
                        : numberedGroupFilters.entrySet()) {
                    passing = passing && f.getValue().test(matcher.group(f.getKey()));
                }

                if (passing) {
                    keptLastLine = addNewEntry(line);
                }
                else {
                    keptLastLine = false;
                }
            }
            else if (keptLastLine == null
                    && namedGroupFilters.isEmpty() && numberedGroupFilters.isEmpty()) {
                // If the first line does not match the pattern and there are no filters,
                // use the line as a new entry.
                keptLastLine = addNewEntry(line);
            }
            else if (keptLastLine != null && keptLastLine) {
                keptLastLine = addToLast(line);
            }
            else {
                keptLastLine = false;
            }
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
