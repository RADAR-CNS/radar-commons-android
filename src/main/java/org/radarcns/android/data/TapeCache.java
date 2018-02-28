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

package org.radarcns.android.data;

import android.content.Context;
import android.content.Intent;
import android.os.Parcel;
import android.util.Pair;

import com.crashlytics.android.Crashlytics;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.android.util.SingleThreadExecutorFactory;
import org.radarcns.data.AvroEncoder;
import org.radarcns.data.Record;
import org.radarcns.data.SpecificRecordEncoder;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.BackedObjectQueue;
import org.radarcns.util.ListPool;
import org.radarcns.util.QueueFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.radarcns.android.device.DeviceService.CACHE_RECORDS_SENT_NUMBER;
import static org.radarcns.android.device.DeviceService.CACHE_RECORDS_UNSENT_NUMBER;
import static org.radarcns.android.device.DeviceService.CACHE_TOPIC;

/**
 * Caches measurement on a BackedObjectQueue. Internally, all data is first cached on a local queue,
 * before being written in batches to the BackedObjectQueue, using a single-threaded
 * ExecutorService. Data is retrieved and removed from the queue in a blocking way using that same
 * ExecutorService. Sent messages are not kept, they are immediately removed.
 *
 * @param <K> measurement key type
 * @param <V> measurement value type
 */
public class TapeCache<K extends SpecificRecord, V extends SpecificRecord> implements DataCache<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(TapeCache.class);
    private static final ListPool listPool = new ListPool(10);

    private final AvroTopic<K, V> topic;
    private final ScheduledExecutorService executor;
    private final List<Record<K, V>> measurementsToAdd;
    private final File outputFile;
    private final BackedObjectQueue.Converter<Record<K, V>> converter;
    private final Runnable flusher;
    private final int maxBytes;
    private QueueFile queueFile;

    private BackedObjectQueue<Record<K, V>> queue;
    private Future<?> addMeasurementFuture;
    private long timeWindowMillis;

    private final AtomicLong queueSize;

    /**
     * TapeCache to cache measurements with
     * @param context Android context to get the cache directory and broadcast the cache size.
     * @param topic Kafka Avro topic to write data for.
     * @param executorFactory factory to get a single-threaded {@link ScheduledExecutorService}
     *                        from.
     * @throws IOException if a BackedObjectQueue cannot be created.
     */
    public TapeCache(final Context context, AvroTopic<K, V> topic,
                     SingleThreadExecutorFactory executorFactory, SpecificData specificData)
            throws IOException {
        this.topic = topic;
        this.timeWindowMillis = 10_000L;
        this.maxBytes = 450_000_000;
        outputFile = new File(context.getCacheDir(), topic.getName() + ".tape");
        try {
            queueFile = QueueFile.newMapped(outputFile, maxBytes);
        } catch (IOException ex) {
            logger.error("TapeCache " + outputFile + " was corrupted. Removing old cache.");
            if (outputFile.delete()) {
                queueFile = QueueFile.newMapped(outputFile, maxBytes);
            } else {
                throw ex;
            }
        }
        this.queueSize = new AtomicLong(queueFile.size());

        this.executor = executorFactory.getScheduledExecutorService();

        // TODO: move to kafka data sender
        this.executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Intent numberCached = new Intent(CACHE_TOPIC);
                numberCached.putExtra(CACHE_TOPIC, getTopic().getName());
                numberCached.putExtra(CACHE_RECORDS_SENT_NUMBER, 0L);
                numberCached.putExtra(CACHE_RECORDS_UNSENT_NUMBER, queueSize.get());
                context.sendBroadcast(numberCached);
            }
        }, 10L, 10L, TimeUnit.SECONDS);

        this.measurementsToAdd = new ArrayList<>();

        this.converter = new TapeAvroConverter<>(topic, specificData);
        this.queue = new BackedObjectQueue<>(queueFile, converter);
        this.flusher = new Runnable() {
            @Override
            public void run() {
                doFlush();
            }
        };
    }

    @Override
    public List<Record<K, V>> unsentRecords(final int limit) throws IOException {
        logger.info("Trying to retrieve records from topic {}", topic);
        try {
            return listPool.get(executor.submit(new Callable<List<Record<K, V>>>() {
                @Override
                public List<Record<K, V>> call() throws Exception {
                    try {
                        return queue.peek(limit);
                    } catch (IOException | IllegalStateException ex) {
                        fixCorruptQueue();
                        return Collections.emptyList();
                    }
                }
            }).get());
        } catch (InterruptedException ex) {
            logger.warn("unsentRecords was interrupted, returning an empty list", ex);
            Thread.currentThread().interrupt();
            return listPool.get(Collections.<Record<K, V>>emptyList());
        } catch (ExecutionException ex) {
            logger.warn("Failed to retrieve records for topic {}", topic, ex);
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new IOException("Unknown error occurred", ex);
            }
        }
    }

    @Override
    public List<Record<K, V>> getRecords(int limit) throws IOException {
        return unsentRecords(limit);
    }

    @Override
    public Pair<Long, Long> numberOfRecords() {
        return new Pair<>(queueSize.get(), 0L);
    }

    @Override
    public synchronized void setTimeWindow(long timeWindowMillis) {
        this.timeWindowMillis = timeWindowMillis;
    }

    @Override
    public void setMaximumSize(final int numBytes) {
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    queueFile.setMaximumFileSize(numBytes);
                }
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Failed to update maximum size");
        }
    }

    @Override
    public int remove(final int number) throws IOException {
        try {
            return executor.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    int actualNumber = Math.min(number, queue.size());
                    if (actualNumber == 0) {
                        return 0;
                    }
                    logger.info("Removing {} records from topic {}", actualNumber, topic);
                    queue.remove(actualNumber);
                    queueSize.addAndGet(-actualNumber);
                    return actualNumber;
                }
            }).get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            logger.warn("Failed to mark record as sent. May resend those at a later time.", ex);
            return 0;
        } catch (ExecutionException ex) {
            logger.warn("Failed to mark sent records for topic {}", topic, ex);
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException) {
                return 0;
            } else if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new IOException("Unknown error occurred", ex);
            }
        }
    }

    @Override
    public synchronized void addMeasurement(final K key, final V value) {
        measurementsToAdd.add(new Record<>(key, value));

        if (addMeasurementFuture == null) {
            addMeasurementFuture = executor.schedule(flusher,
                    timeWindowMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public int removeBeforeTimestamp(long millis) {
        return 0;
    }

    @Override
    public AvroTopic<K, V> getTopic() {
        return topic;
    }

    @Override
    public void writeRecordsToParcel(Parcel dest, int limit) throws IOException {
        List<Record<K, V>> records = getRecords(limit);
        SpecificRecordEncoder specificEncoder = new SpecificRecordEncoder(true);
        AvroEncoder.AvroWriter<K> keyWriter = specificEncoder.writer(topic.getKeySchema(), topic.getKeyClass());
        AvroEncoder.AvroWriter<V> valueWriter = specificEncoder.writer(topic.getValueSchema(), topic.getValueClass());

        dest.writeInt(records.size());
        for (Record<K, V> record : records) {
            dest.writeByteArray(keyWriter.encode(record.key));
            dest.writeByteArray(valueWriter.encode(record.value));
        }
        returnList(records);
    }

    @Override
    public void returnList(List list) {
        listPool.add(list);
    }

    @Override
    public void close() throws IOException {
        flush();
        queue.close();
        listPool.clear();
    }

    @Override
    public void flush() {
        Future<?> flushFuture;
        synchronized (this) {
            if (addMeasurementFuture != null) {
                addMeasurementFuture.cancel(false);
                addMeasurementFuture = null;
            }
            // no measurements in cache
            if (measurementsToAdd.isEmpty()) {
                return;
            }
            flushFuture = executor.submit(this.flusher);
        }
        try {
            flushFuture.get();
        } catch (InterruptedException e) {
            logger.warn("Did not wait for adding measurements to complete.");
        } catch (ExecutionException ex) {
            logger.warn("Failed to execute flush task", ex);
        }
    }

    private void doFlush() {
        List<Record<K, V>> localList;

        synchronized (this) {
            addMeasurementFuture = null;
            localList = listPool.get(measurementsToAdd);
            measurementsToAdd.clear();
        }

        try {
            logger.info("Writing {} records to file in topic {}", localList.size(), topic);
            queue.addAll(localList);
            queueSize.addAndGet(localList.size());
        } catch (IOException ex) {
            logger.error("Failed to add records", ex);
            queueSize.set(queue.size());
            throw new RuntimeException(ex);
        } catch (IllegalArgumentException ex) {
            logger.error("Failed to validate all records; adding individual records instead: {}", ex.getMessage());
            try {
                logger.info("Writing {} records to file in topic {}", localList.size(), topic);
                for (Record<K, V> record : localList) {
                    try {
                        queue.add(record);
                    } catch (IllegalArgumentException ex2) {
                        Crashlytics.logException(ex2);
                    }
                }
                queueSize.addAndGet(localList.size());
            } catch (IOException ex2) {
                logger.error("Failed to add record", ex);
                queueSize.set(queue.size());
                throw new RuntimeException(ex);
            }
        }

        listPool.add(localList);
    }

    private void fixCorruptQueue() throws IOException {
        logger.error("Queue was corrupted. Removing cache.");
        try {
            queue.close();
        } catch (IOException ioex) {
            logger.warn("Failed to close corrupt queue", ioex);
        }
        if (outputFile.delete()) {
            queueFile = QueueFile.newMapped(outputFile, maxBytes);
            queueSize.set(queueFile.size());
            queue = new BackedObjectQueue<>(queueFile, converter);
        } else {
            throw new IOException("Cannot create new cache.");
        }
    }
}
