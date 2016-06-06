/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.commons;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.io.LineIterator;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Iterators.peekingIterator;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.io.FileWriteMode.APPEND;
import static com.google.common.io.Files.asByteSink;
import static com.google.common.io.Files.move;
import static com.google.common.io.Files.newWriter;
import static java.io.File.createTempFile;
import static org.apache.commons.io.FileUtils.*;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.io.IOUtils.copyLarge;
import static org.apache.commons.io.LineIterator.closeQuietly;
import static org.apache.jackrabbit.oak.commons.sort.ExternalSort.mergeSortedFiles;
import static org.apache.jackrabbit.oak.commons.sort.ExternalSort.sortInBatch;

/**
 * Simple File utils
 */
public final class FileIOUtils {
    private FileIOUtils() {
    }

    public final static Comparator<String> lexComparator =
        new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return s1.compareTo(s2);
            }
        };

    /**
     * Sorts the given file externally using the {@link #lexComparator}.
     *
     * @param file file whose contents needs to be sorted
     */
    public static void sort(File file) throws IOException {
        File sorted = createTempFile("temp", null);
        merge(sortInBatch(file, lexComparator, true), sorted);
        move(sorted, file);
    }

    /**
     * Sorts the given file externally with the given comparator.
     *
     * @param file file whose contents needs to be sorted
     * @param comparator to compare
     * @throws IOException
     */
    public static void sort(File file, Comparator<String> comparator) throws IOException {
        File sorted = createTempFile("temp", null);
        merge(sortInBatch(file, comparator, true), sorted);
        move(sorted, file);
    }

    /**
     * Merges a list of files after sorting with the {@link #lexComparator}.
     *
     * @param files files to merge
     * @param output merge output file
     * @throws IOException
     */
    public static void merge(List<File> files, File output) throws IOException {
        mergeSortedFiles(
            files,
            output, lexComparator, true);
    }

    /**
     * Copies an input stream to a file.
     *
     * @param stream steam to copy
     * @return
     * @throws IOException
     */
    public static File copy(InputStream stream) throws IOException {
        File file = createTempFile("temp", null);
        OutputStream out = null;
        try {
            out = new FileOutputStream(file);
            IOUtils.copy(stream, out);
        } finally {
            closeQuietly(out);
        }
        return file;
    }

    public static Set<String> readStringsAsSet(InputStream stream) throws IOException {
        BufferedReader reader = null;
        Set<String> ids = newHashSet();

        try {
            reader = new BufferedReader(new InputStreamReader(stream));
            String line  = null;
            while ((line = reader.readLine()) != null) {
                ids.add(line);
            }
        } finally {
            closeQuietly(reader);
        }
        return ids;
    }

    /**
     * Appends the contents of the list of files to the given file and deletes the files
     * if the delete flag is enabled
     *
     * @param files
     * @param appendTo
     * @throws IOException
     */
    public static void append(List<File> files, File appendTo, boolean delete) throws IOException {
        OutputStream appendStream = null;
        try {
            appendStream = asByteSink(appendTo, APPEND).openBufferedStream();

            for (File f : files) {
                InputStream iStream = new FileInputStream(f);
                try {
                    copyLarge(iStream, appendStream);
                } finally {
                    closeQuietly(iStream);
                }
                if (delete) {
                    f.delete();
                }
            }
        } finally {
            closeQuietly(appendStream);
        }
    }

    /**
     * Writes the strings to the given file.
     * Does not close the iterator.
     *
     * @param iterator
     * @param file
     * @throws IOException
     */
    public static void writeStrings(Iterator<String> iterator, File file) throws IOException {
        BufferedWriter writer = newWriter(file, UTF_8);
        try {
            while (iterator.hasNext()) {
                writer.write(iterator.next());
                writer.newLine();
            }
        } finally {
            closeQuietly(writer);
        }
    }

    /**
     * Implements a {@link java.io.Closeable} wrapper over a {@link LineIterator}.
     * Also has a transformer to transform the output. If the underlying file is
     * provide then it deletes the file on {@link #close()}.
     *
     * @param <T> the type of elements in the iterator
     */
    public static class CloseableFileIterator<T> extends AbstractIterator<T> implements Closeable {
        private final LineIterator iterator;
        private final Function<String, T> transformer;
        private File backingFile;

        public CloseableFileIterator(LineIterator iterator, Function<String, T> transformer) {
            this.iterator = iterator;
            this.transformer = transformer;
        }

        public CloseableFileIterator(LineIterator iterator, File backingFile,
                Function<String, T> transformer) {
            this.iterator = iterator;
            this.transformer = transformer;
            this.backingFile = backingFile;
        }

        @Override
        protected T computeNext() {
            if (iterator.hasNext()) {
                return transformer.apply(iterator.next());
            }

            try {
                close();
            } catch (IOException e) {
            }
            return endOfData();
        }

        @Override
        public void close() throws IOException {
            closeQuietly(iterator);
            if (backingFile != null) {
                forceDelete(backingFile);
            }
        }

        public static CloseableFileIterator<String> wrap(LineIterator iter) {
            return new CloseableFileIterator<String>(iter, new Function<String, String>() {
                public String apply(String s) {
                    return s;
                }
            });
        }

        public static CloseableFileIterator<String> wrap(LineIterator iter, File backingFile) {
            return new CloseableFileIterator<String>(iter, backingFile,
                new Function<String, String>() {
                    public String apply(String s) {
                        return s;
                    }
            });
        }
    }

    /**
     * FileLineDifferenceIterator class which iterates over the difference of 2 files line by line.
     */
    public static class FileLineDifferenceIterator extends AbstractIterator<String> implements Closeable {
        private static final java.lang.String DELIM = ",";
        private final PeekingIterator<String> peekMarked;
        private final LineIterator marked;
        private final LineIterator all;

        public FileLineDifferenceIterator(File marked, File available) throws IOException {
            this(lineIterator(marked), lineIterator(available));
        }

        public FileLineDifferenceIterator(LineIterator marked, LineIterator available) throws IOException {
            this.marked = marked;
            this.peekMarked = peekingIterator(marked);
            this.all = available;
        }

        @Override
        protected String computeNext() {
            String diff = computeNextDiff();
            if (diff == null) {
                close();
                return endOfData();
            }
            return diff;
        }

        @Override
        public void close() {
            closeQuietly(marked);
            closeQuietly(all);
        }

        private String getKey(String row) {
            return row.split(DELIM)[0];
        }

        private String computeNextDiff() {
            if (!all.hasNext()) {
                return null;
            }

            //Marked finish the rest of all are part of diff
            if (!peekMarked.hasNext()) {
                return all.next();
            }

            String diff = null;
            while (all.hasNext() && diff == null) {
                diff = all.next();
                while (peekMarked.hasNext()) {
                    String marked = peekMarked.peek();
                    int comparisonResult = getKey(diff).compareTo(getKey(marked));
                    if (comparisonResult > 0) {
                        //Extra entries in marked. Ignore them and move on
                        peekMarked.next();
                    } else if (comparisonResult == 0) {
                        //Matching entry found in marked move past it. Not a
                        //dif candidate
                        peekMarked.next();
                        diff = null;
                        break;
                    } else {
                        //This entry is not found in marked entries
                        //hence part of diff
                        return diff;
                    }
                }
            }
            return diff;
        }
    }
}
