/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collector;
import java.util.stream.IntStream;

public class CalculateAverage_broop {

    static void main(String[] args) throws IOException {
        String fileName = args.length > 0 ? args[0] : "measurements.txt";

        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
             FileChannel channel = file.getChannel()) {

            // Set up chunk processing
            long fileSize = channel.size();
            int numChunks = Runtime.getRuntime().availableProcessors();
            long chunkSize = fileSize / numChunks;

            String result = IntStream.range(0, numChunks)
                    .parallel()
                    .mapToObj(i -> { // mapToObj so we don't need to box or cast...
                        long start = i * chunkSize;
                        // make sure the final chunk includes everything
                        long end = (i == numChunks - 1) ? fileSize : (i + 1) * chunkSize;
                        return processChunk(fileName, start, end, fileSize);
                    })
                    .collect(chunkResultCollector());
            System.out.println(result);
        }
    }

    private static Collector<ChunkResult, ?, String> chunkResultCollector() {
        return Collector.of(
                MergedChunkResults::new, //supplier
                (merged, chunkResult) -> { //accumulator
                    merged.stationNames.putAll(chunkResult.stationNames);
                    chunkResult.stats.forEach((hash, stats) -> {
                        //we could use Map.merge's but this saves a bit
                        Stats current = merged.results.get(hash);
                        if (current != null) {
                            current.merge(stats);
                        } else {
                            merged.results.put(hash, stats);
                        }
                    });
                },
                (merged, merged2) -> { //combiner
                    merged.stationNames.putAll(merged2.stationNames);
                    merged2.results.forEach((hash, stats) -> {
                        //we could use Map.merge's but this saves a bit
                        Stats current = merged.results.get(hash);
                        if (current != null) {
                            current.merge(stats);
                        } else {
                            merged.results.put(hash, stats);
                        }
                    });
                    return merged;
                },
                merged -> { //finisher
                    StringJoiner joiner = new StringJoiner(", ", "{", "}");
                    // need this list for sorting the station names - could have used a TreeMap but this only happens once
                    List<Map.Entry<Integer, Stats>> sortedNames = new ArrayList<>(merged.results.entrySet());
                    sortedNames.sort(Comparator.comparing(e -> merged.stationNames.get(e.getKey())));
                    for (Map.Entry<Integer, Stats> entry : sortedNames) { //now build the output with sorted stations!
                        String stationName = merged.stationNames.get(entry.getKey());
                        Stats stats = entry.getValue();
                        //format: "station name=min/mean/max"
                        joiner.add(STR."\{stationName}=\{stats.toString()}");
                    }
                    return joiner.toString();
                });
    }

    /**
     * Process a chunk of the input file.
     * Adjusts for boundaries accordingly.
     */
    private static ChunkResult processChunk(String fileName, long start, long end, long fileSize) {
        ChunkResult result = new ChunkResult();

        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r");
             FileChannel channel = raf.getChannel()) {

            // adjust the boundaries to align with line breaks
            long adjustedStart = adjustStartToLineBoundary(raf, start);
            long adjustedEnd = adjustEndToLineBoundary(raf, end, fileSize);

            // Process in cache-friendly segments
            processSegments(channel, adjustedStart, adjustedEnd, result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Adjust the start position to the beginning of the next complete line.
     */
    private static long adjustStartToLineBoundary(RandomAccessFile raf, long start) throws IOException {
        if (start == 0)
            return start;
        raf.seek(start);
        int b;
        long pos = start;
        while ((b = raf.read()) != -1) {
            if (b == '\n')
                return pos + 1;
            pos++;
        }
        return start;
    }

    /**
     * Adjust the end position to include the complete line.
     */
    private static long adjustEndToLineBoundary(RandomAccessFile raf, long end, long fileSize) throws IOException {
        if (end >= fileSize)
            return end;
        raf.seek(end);
        int b;
        long pos = end;
        while ((b = raf.read()) != -1) {
            if (b == '\n')
                return pos + 1;
            pos++;
        }
        return end;
    }

    /**
     * Process chunk segments and handle the leftovers :)
     */
    private static void processSegments(FileChannel channel, long start, long end, ChunkResult result) throws IOException {
        final int SEGMENT_SIZE = 1024 * 1024; // 1MB

        long currentPos = start;
        byte[] leftover = new byte[0];

        while (currentPos < end) {
            int mapSize = (int) Math.min(SEGMENT_SIZE, end - currentPos);

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, currentPos, mapSize);
            byte[] segmentData = new byte[mapSize];
            buffer.get(segmentData);

            int segmentStart = 0;

            // Complete leftover line from previous segment
            if (leftover.length > 0) {
                int firstNewline = findFirstNewline(segmentData);
                byte[] completeLine = new byte[leftover.length + firstNewline];
                System.arraycopy(leftover, 0, completeLine, 0, leftover.length);
                System.arraycopy(segmentData, 0, completeLine, leftover.length, firstNewline);
                processSegment(completeLine, 0, completeLine.length, result);
                segmentStart = Math.min(firstNewline + 1, segmentData.length);
            }

            // Find last complete line
            int lastNewline = findLastNewline(segmentData, segmentStart);

            if (lastNewline >= segmentStart) {
                processSegment(segmentData, segmentStart, lastNewline + 1, result);
                leftover = extractBytes(segmentData, lastNewline + 1);
            } else {
                leftover = extractBytes(segmentData, segmentStart);
            }

            currentPos += mapSize;
        }

        // Process final leftover
        if (leftover.length > 0) {
            processSegment(leftover, 0, leftover.length, result);
        }
    }

    private static int findFirstNewline(byte[] data) {
        for (int i = 0; i < data.length; i++) {
            if (data[i] == '\n')
                return i;
        }
        return data.length;
    }

    private static int findLastNewline(byte[] data, int start) {
        for (int i = data.length - 1; i >= start; i--) {
            if (data[i] == '\n')
                return i;
        }
        return -1;
    }

    private static byte[] extractBytes(byte[] data, int start) {
        int len = data.length - start;
        if (len <= 0)
            return new byte[0];
        byte[] extracted = new byte[len];
        System.arraycopy(data, start, extracted, 0, len);
        return extracted;
    }

    // Process a segment of data containing complete lines - see processSegments()
    private static void processSegment(byte[] data, int start, int end, ChunkResult result) {
        int lineStart = start;

        // search for newlines
        for (int i = start; i < end; i++) {
            if (data[i] == '\n') {
                // Found end of line - process if non-empty
                if (i > lineStart) {
                    processLine(data, lineStart, i, result);
                }
                // Next line starts after the newline
                lineStart = i + 1;
            }
        }

        // Handle final line if it doesn't end with newline
        // (e.g., last line of file)
        if (lineStart < end) {
            processLine(data, lineStart, end, result);
        }
    }

    // parse single line and update the Stats - lots of side effects here - lots of side effects...
    private static void processLine(byte[] data, int start, int end, ChunkResult result) {
        // parsing in reverse - starting with the temp
        // the format is: -?\d{1,2}\.\d

        // end points one past the last char
        int endIndex = end - 1;

        // Tenths digit is always at the end
        int tenths = data[endIndex] - '0';
        // data[lastIdx - 1] is always '.', skip it
        int ones = data[endIndex - 2] - '0';
        int temp = ones * 10 + tenths;

        int semicolonPos;
        byte c = data[endIndex - 3];
        if (c == ';') {
            // Single digit temp like "3.7"
            semicolonPos = endIndex - 3;
        } else if (c == '-') {
            // Negative single digit like "-3.7"
            temp = -temp;
            semicolonPos = endIndex - 4;
        } else {
            // Two digit temp: c is the tens digit
            temp = (c - '0') * 100 + temp;
            if (data[endIndex - 4] == '-') {
                temp = -temp;
                semicolonPos = endIndex - 5;
            } else {
                semicolonPos = endIndex - 4;
            }
        }

        if (semicolonPos <= start)
            return;

        // very simple solution - could have used VarHandle as some other solutions used,
        // but that's a lot of complexity for very little (or no) improvement if you don't go Unsafe.
        // This is the original hashing code - good enough for the ~400 station names
        // int hash = 0;
        // for (int i = start; i < semicolonPos; i++) {
        // hash = 31 * hash + data[i];
        // }
        // I later replaced it with fnv-1a hashing which offers a tiny improvement in performance and better distribution.
        // http://www.isthe.com/chongo/tech/comp/fnv/
        int hash = 0x811c9dc5; // FNV 32bit offset basis
        for (int i = start; i < semicolonPos; i++) {
            hash ^= data[i];
            hash *= 0x01000193; // FNV 32bit prime
        }

        // Fast path with get() which is better than calling computeIfAbsent ~1 billion times :)
        Stats stats = result.stats.get(hash);
        if (stats != null) {
            stats.update(temp);
            return;
        }

        // Slow path - new station (only ~400 times total per chunk)
        stats = new Stats();
        stats.update(temp);
        result.stats.put(hash, stats);
        result.stationNames.put(hash, new String(data, start, semicolonPos - start, StandardCharsets.UTF_8));
    }

    static class MergedChunkResults {
        final Map<Integer, String> stationNames = new HashMap<>(512);
        final Map<Integer, Stats> results = new HashMap<>(512);
    }

    static class ChunkResult {
        final Map<Integer, String> stationNames = new HashMap<>(512);
        final Map<Integer, Stats> stats = new HashMap<>(512);
    }

    // Stats for a single weather station.
    static class Stats {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        long sum = 0;
        int count = 0;

        public void update(int temp) {
            //this is better than Math.min/max for - this gets called in processLine
            if (temp < min)
                min = temp;
            if (temp > max)
                max = temp;
            sum += temp;
            count++;
        }

        //only gets called in the collector
        public void merge(Stats other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
        }

        @Override
        public String toString() {
            if (count == 0) return "0.0/0.0/0.0";
            double mean = Math.round((double) sum / count) / 10.0;
            return STR."\{min / 10.0}/\{mean}/\{max / 10.0}";
        }
    }
}