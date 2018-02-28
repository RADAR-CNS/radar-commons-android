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

package org.radarcns.android.kafka;

import android.os.Handler;
import android.os.HandlerThread;
import android.support.annotation.NonNull;
import org.radarcns.android.data.DataCache;
import org.radarcns.android.data.DataHandler;
import org.radarcns.data.AvroRecordData;
import org.radarcns.data.Record;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.producer.AuthenticationException;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.ListPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static android.os.Process.THREAD_PRIORITY_BACKGROUND;

/**
 * Separate thread to read from the database and send it to the Kafka server. It cleans the
 * database.
 *
 * It uses a set of timers to addMeasurement data and clean the databases.
 */
public class KafkaDataSubmitter<V> implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDataSubmitter.class);

    private final DataHandler<ObservationKey, V> dataHandler;
    private final KafkaSender sender;
    private final ConcurrentMap<AvroTopic<ObservationKey, V>, Queue<Record<ObservationKey, V>>> trySendCache;
    private final Map<AvroTopic<ObservationKey, V>, Runnable> trySendFuture;
    private final Map<AvroTopic<ObservationKey, V>, KafkaTopicSender<ObservationKey, V>> topicSenders;
    private final KafkaConnectionChecker connection;
    private static final ListPool listPool = new ListPool(1);
    private final AtomicInteger sendLimit;
    private final HandlerThread mHandlerThread;
    private final Handler mHandler;

    private Runnable uploadFuture;
    private Runnable uploadIfNeededFuture;
    /** Upload rate in milliseconds. */
    private long uploadRate;
    private String userId;

    public KafkaDataSubmitter(@NonNull DataHandler<ObservationKey, V> dataHandler, @NonNull
            KafkaSender sender, int sendLimit, long uploadRate, String userId) {
        this.dataHandler = dataHandler;
        this.sender = sender;
        this.userId = userId;
        trySendCache = new ConcurrentHashMap<>();
        trySendFuture = new HashMap<>();
        topicSenders = new HashMap<>();
        this.sendLimit = new AtomicInteger(sendLimit);

        mHandlerThread = new HandlerThread("data-submitter", THREAD_PRIORITY_BACKGROUND);
        mHandlerThread.start();
        mHandler = new Handler(mHandlerThread.getLooper());

        logger.info("Started data submission executor");

        connection = new KafkaConnectionChecker(sender, mHandler, dataHandler, uploadRate * 5);

        mHandler.post(new Runnable() {
            @Override
            public void run() {
                try {
                    if (KafkaDataSubmitter.this.sender.isConnected()) {
                        KafkaDataSubmitter.this.dataHandler.updateServerStatus(ServerStatusListener.Status.CONNECTED);
                        connection.didConnect();
                    } else {
                        KafkaDataSubmitter.this.dataHandler.updateServerStatus(ServerStatusListener.Status.DISCONNECTED);
                        connection.didDisconnect(null);
                    }
                } catch (AuthenticationException ex) {
                    connection.didDisconnect(ex);
                }
            }
        });

        synchronized (this) {
            uploadFuture = null;
            uploadIfNeededFuture = null;
            setUploadRate(uploadRate);
        }
        logger.info("Remote Config: Upload rate is '{}' sec per upload", uploadRate);
    }

    /** Set upload rate in seconds. */
    public final synchronized void setUploadRate(long period) {
        long newUploadRate = period * 1000L;
        if (this.uploadRate == newUploadRate) {
            return;
        }
        this.uploadRate = newUploadRate;
        if (uploadFuture != null) {
            mHandler.removeCallbacks(uploadFuture);
            mHandler.removeCallbacks(uploadIfNeededFuture);
        }
        // Get upload frequency from system property
        uploadFuture = new Runnable() {
            Set<AvroTopic<ObservationKey, ? extends V>> topicsToSend = Collections.emptySet();

            @Override
            public void run() {
                if (connection.isConnected()) {
                    if (topicsToSend.isEmpty()) {
                        topicsToSend = new HashSet<>(KafkaDataSubmitter.this.dataHandler.getCaches().keySet());
                    }
                    uploadCaches(topicsToSend);
                    // still more data to send, do that immediately
                    if (!topicsToSend.isEmpty()) {
                        mHandler.post(this);
                        return;
                    }
                } else {
                    topicsToSend.clear();
                }
                mHandler.postDelayed(this, uploadRate);
            }
        };
        mHandler.postDelayed(uploadFuture, uploadRate);

        uploadIfNeededFuture = new Runnable() {
            @Override
            public void run() {
                if (connection.isConnected()) {
                    boolean sendAgain = uploadCachesIfNeeded();
                    if (sendAgain) {
                        mHandler.post(this);
                        return;
                    }
                }
                mHandler.postDelayed(this, uploadRate / 5);
            }
        };
        mHandler.postDelayed(uploadIfNeededFuture, uploadRate / 5);
    }

    /** Upload rate in seconds. */
    private synchronized long getUploadRate() {
        return this.uploadRate / 1000L;
    }

    public void setSendLimit(int limit) {
        sendLimit.set(limit);
    }

    /**
     * Close the submitter eventually. This does not flush any caches.
     */
    @Override
    public synchronized void close() {
        mHandler.post(new Runnable() {
            @Override
            public void run() {
                mHandler.removeCallbacks(uploadFuture);
                mHandler.removeCallbacks(uploadIfNeededFuture);

                synchronized (trySendFuture) {
                    for (Runnable future : trySendFuture.values()) {
                        mHandler.removeCallbacks(future);
                    }
                    trySendFuture.clear();
                    trySendCache.clear();
                }

                for (Map.Entry<AvroTopic<ObservationKey, V>, KafkaTopicSender<ObservationKey, V>> topicSender : topicSenders.entrySet()) {
                    try {
                        topicSender.getValue().close();
                    } catch (IOException e) {
                        logger.warn("failed to close topicSender for topic {}", topicSender.getKey().getName(), e);
                    }
                }
                topicSenders.clear();

                try {
                    sender.close();
                } catch (IOException e1) {
                    logger.warn("failed to addMeasurement latest batches", e1);
                }
            }
        });
        mHandlerThread.quitSafely();
    }

    /** Get a sender for a topic. Per topic, only ONE thread may use this. */
    private KafkaTopicSender<ObservationKey, V> sender(AvroTopic<ObservationKey, V> topic) throws IOException {
        KafkaTopicSender<ObservationKey, V> topicSender = topicSenders.get(topic);
        if (topicSender == null) {
            topicSender = sender.sender(topic);
            topicSenders.put(topic, topicSender);
        }
        return topicSender;
    }

    /**
     * Check the connection status eventually.
     */
    public void checkConnection() {
        connection.check();
    }

    /**
     * Upload the caches if they would cause the buffer to overflow
     */
    private boolean uploadCachesIfNeeded() {
        boolean uploadingNotified = false;
        int currentSendLimit = sendLimit.get();
        boolean sendAgain = false;

        try {
            for (Map.Entry<AvroTopic<ObservationKey, ? extends V>, ? extends DataCache<ObservationKey, ? extends V>> entry
                    : dataHandler.getCaches().entrySet()) {
                long unsent = entry.getValue().numberOfRecords().first;
                if (unsent > currentSendLimit) {
                    //noinspection unchecked
                    int sent = uploadCache(
                            (AvroTopic<ObservationKey, V>) entry.getKey(), (DataCache<ObservationKey, V>) entry.getValue(),
                            currentSendLimit, uploadingNotified);
                    if (!uploadingNotified) {
                        uploadingNotified = true;
                    }
                    if (!sendAgain && unsent - sent > currentSendLimit && sent != -1) {
                        sendAgain = true;
                    }
                }
            }
            if (uploadingNotified) {
                dataHandler.updateServerStatus(ServerStatusListener.Status.CONNECTED);
                connection.didConnect();
            }
        } catch (IOException ex) {
            connection.didDisconnect(ex);
            sendAgain = false;
        }
        return sendAgain;
    }

    /**
     * Upload a limited amount of data stored in the database which is not yet sent.
     */
    private void uploadCaches(Set<AvroTopic<ObservationKey, ? extends V>> toSend) {
        boolean uploadingNotified = false;
        int currentSendLimit = sendLimit.get();
        try {
            for (Map.Entry<AvroTopic<ObservationKey, ? extends V>, ? extends DataCache<ObservationKey, ? extends V>> entry : dataHandler.getCaches().entrySet()) {
                if (!toSend.contains(entry.getKey())) {
                    continue;
                }
                @SuppressWarnings("unchecked") // we can upload any record
                int sent = uploadCache((AvroTopic<ObservationKey, V>)entry.getKey(), (DataCache<ObservationKey, V>)entry.getValue(), currentSendLimit, uploadingNotified);
                if (sent < currentSendLimit) {
                    toSend.remove(entry.getKey());
                }
                if (!uploadingNotified && sent > 0) {
                    uploadingNotified = true;
                }
            }
            if (uploadingNotified) {
                dataHandler.updateServerStatus(ServerStatusListener.Status.CONNECTED);
                connection.didConnect();
            }
        } catch (IOException ex) {
            connection.didDisconnect(ex);
        }
    }

    /**
     * Upload some data from a single table.
     * @return number of records sent.
     */
    private int uploadCache(AvroTopic<ObservationKey, V> topic, DataCache<ObservationKey, V> cache, int limit,
                            boolean uploadingNotified) throws IOException {
        List<Record<ObservationKey, V>> unfilteredMeasurements = cache.unsentRecords(limit);

        List<Record<ObservationKey, V>> measurements = listPool.get(Collections
                .<Record<ObservationKey,V>>emptyList());
        for (Record<ObservationKey, V> record : unfilteredMeasurements) {
            if (record != null && record.key.getUserId().equals(userId)) {
                measurements.add(record);
            }
        }

        int numberOfRecords = measurements.size();
        int totalSize = unfilteredMeasurements.size();

        try {
            if (numberOfRecords > 0) {
                KafkaTopicSender<ObservationKey, V> cacheSender = sender(topic);

                if (!uploadingNotified) {
                    dataHandler.updateServerStatus(ServerStatusListener.Status.UPLOADING);
                }

                try {
                    cacheSender.send(new AvroRecordData<>(topic, measurements));
                    cacheSender.flush();
                } catch (AuthenticationException ex) {
                    dataHandler.updateRecordsSent(topic.getName(), -1);
                    throw ex;
                } catch (IOException ioe) {
                    dataHandler.updateServerStatus(ServerStatusListener.Status.UPLOADING_FAILED);
                    dataHandler.updateRecordsSent(topic.getName(), -1);
                    throw ioe;
                }

                dataHandler.updateRecordsSent(topic.getName(), numberOfRecords);

                logger.debug("uploaded {} {} records", numberOfRecords, topic.getName());
            }
            cache.remove(totalSize);
        } finally {
            listPool.add(measurements);
            cache.returnList(unfilteredMeasurements);
        }

        return totalSize;
    }

    /**
     * Try to addMeasurement a message, without putting it in any permanent storage. Any failure may cause
     * messages to be lost. If the sender is disconnected, messages are immediately discarded.
     * @return whether the message was queued for sending.
     */
    public <W extends V> boolean trySend(final AvroTopic<ObservationKey, W> topic,
                                         final ObservationKey deviceId, final W record) {
        if (!connection.isConnected()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        final AvroTopic<ObservationKey, V> castTopic = (AvroTopic<ObservationKey, V>)topic;
        Queue<Record<ObservationKey, V>> records = trySendCache.get(castTopic);
        if (records == null) {
            records = new ConcurrentLinkedQueue<>();
            //noinspection unchecked
            trySendCache.put((AvroTopic<ObservationKey, V>)topic, records);
        }
        records.add(new Record<>(deviceId, (V)record));

        long period = getUploadRate();

        synchronized (trySendFuture) {
            if (!trySendFuture.containsKey(topic)) {
                Runnable future = new Runnable() {
                    @Override
                    public void run() {
                        if (!connection.isConnected()) {
                            return;
                        }

                        List<Record<ObservationKey, V>> localRecords;

                        synchronized (trySendFuture) {
                            Queue<Record<ObservationKey, V>> queue = trySendCache.get(topic);
                            trySendFuture.remove(topic);
                            localRecords = listPool.get(queue);
                        }

                        try {
                            doImmediateSend(castTopic, localRecords);
                            connection.didConnect();
                        } catch (IOException e) {
                            dataHandler.updateRecordsSent(topic.getName(), -1);
                            connection.didDisconnect(e);
                        } finally {
                            listPool.add(localRecords);
                        }
                    }
                };
                mHandler.postDelayed(future, period * 1000L);
                trySendFuture.put(castTopic, future);
            }
        }
        return true;
    }

    /** Immediately send given records, without any error recovery. */
    private void doImmediateSend(AvroTopic<ObservationKey, V> topic,
                                 List<Record<ObservationKey, V>> records)
            throws IOException {
        KafkaTopicSender<ObservationKey, V> localSender = sender(topic);
        localSender.send(new AvroRecordData<>(topic, records));
        dataHandler.updateRecordsSent(topic.getName(), records.size());
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
