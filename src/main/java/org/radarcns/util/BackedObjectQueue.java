/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.util;

import com.crashlytics.android.Crashlytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A queue-like object queue that is backed by a file storage.
 * @param <T> type of objects to store.
 */
public class BackedObjectQueue<T> implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(BackedObjectQueue.class);

    private final Converter<T> converter;
    private final QueueFile queueFile;

    /**
     * Creates a new object queue from given file.
     * @param queueFile file to write objects to
     * @param converter way to parse from and to given objects
     */
    public BackedObjectQueue(QueueFile queueFile, Converter<T> converter) {
        this.queueFile = queueFile;
        this.converter = converter;
    }

    /** Number of elements in the queue. */
    public int size() {
        return this.queueFile.size();
    }

    /**
     * Add a new element to the queue.
     * @param entry element to add
     * @throws IOException if the backing file cannot be accessed, the queue is full, or the element
     *                     cannot be converted.
     * @throws IllegalArgumentException if given entry is not a valid object for serialization.
     */
    public void add(T entry) throws IOException {
        try (QueueFileOutputStream out = queueFile.elementOutputStream()) {
            converter.serialize(entry, out);
        }
    }

    /**
     * Add a collection of new element to the queue.
     * @param entries elements to add
     * @throws IOException if the backing file cannot be accessed, the queue is full or the element
     *                     cannot be converted.
     * @throws IllegalArgumentException if given entry is not a valid object for serialization.
     */
    public void addAll(Collection<? extends T> entries) throws IOException {
        try (QueueFileOutputStream out = queueFile.elementOutputStream()) {
            for (T entry : entries) {
                converter.serialize(entry, out);
                out.next();
            }
        }
    }

    /**
     * Get the front-most object in the queue. This does not remove the element.
     * @return front-most element or null if none is available
     * @throws IOException if the element could not be read or deserialized
     * @throws IllegalStateException if the element that was read was invalid.
     */
    public T peek() throws IOException {
        try (InputStream in = queueFile.peek()) {
            return converter.deserialize(in);
        }
    }

    /**
     * Get at most {@code n} front-most objects in the queue. This does not remove the elements.
     * Elements that were found to be invalid according to the current schema are
     * @param n number of elements to retrieve
     * @return list of elements, with at most {@code n} elements.
     * @throws IOException if the element could not be read or deserialized
     * @throws IllegalStateException if the element could not be read
     */
    public List<T> peek(int n) throws IOException {
        Iterator<InputStream> iter = queueFile.iterator();
        List<T> results = new ArrayList<>(n);
        for (int i = 0; i < n && iter.hasNext(); i++) {
            try (InputStream in = iter.next()) {
                try {
                    results.add(converter.deserialize(in));
                } catch (IllegalStateException ex) {
                    Crashlytics.logException(ex);
                    logger.warn("Invalid record ignored: {}", ex.getMessage());
                    results.add(null);
                }
            }
        }
        return results;
    }

    /**
     * Remove the first element from the queue.
     * @throws IOException when the element could not be removed
     * @throws NoSuchElementException if more than the available elements are requested to be removed
     */
    public void remove() throws IOException {
        remove(1);
    }

    /**
     * Remove the first {@code n} elements from the queue.
     *
     * @throws IOException when the elements could not be removed
     * @throws NoSuchElementException if more than the available elements are requested to be removed
     */
    public void remove(int n) throws IOException {
        queueFile.remove(n);
    }

    /** Returns {@code true} if this queue contains no entries. */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Close the queue. This also closes the backing file.
     * @throws IOException if the file cannot be closed.
     */
    @Override
    public void close() throws IOException {
        queueFile.close();
    }

    /** Converts streams into objects. */
    public interface Converter<T> {
        /**
         * Deserialize an object from given input stream.
         * @param in input, which will not be closed after this call.
         * @return deserialized object
         * @throws IOException if a valid object could not be deserialized from the stream
         */
        T deserialize(InputStream in) throws IOException;
        /**
         * Serialize an object to given output stream.
         * @param out output, which will not be closed after this call.
         * @throws IOException if a valid object could not be serialized to the stream
         */
        void serialize(T value, OutputStream out) throws IOException;
    }
}
