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

package org.radarcns.android.device;

import android.support.annotation.CallSuper;

import com.crashlytics.android.Crashlytics;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.android.auth.AppSource;
import org.radarcns.android.data.TableDataHandler;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

/**
 * Abstract DeviceManager that handles some common functionality.
 *
 * @param <S> service type the manager is started by
 * @param <T> state type that the manager will update.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class AbstractDeviceManager<S extends DeviceService<T>, T extends BaseDeviceState>
        implements DeviceManager<T> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDeviceManager.class);

    private final TableDataHandler dataHandler;
    private final T deviceStatus;
    private String deviceName;
    private final S service;
    private boolean closed;
    private boolean didWarn;

    /**
     * AppSource manager initialization. After initialization, be sure to call
     * {@link #setName(String)}.
     *
     * @param service service that the manager is started by
     */
    public AbstractDeviceManager(S service) {
        this.service = service;
        this.dataHandler = service.getDataHandler();
        this.deviceName = android.os.Build.MODEL;
        this.deviceStatus = service.getState();
        closed = false;
        didWarn = false;
    }

    /**
     * Update the device status. The device status should be updated with the following meanings:
     * <ul>
     *     <li>DISABLED if the manager will not be able to record any data.</li>
     *     <li>READY if the manager is searching for a device to connect with.</li>
     *     <li>CONNECTING if the manager has found a device to connect with.</li>
     *     <li>CONNECTED if the manager is connected to a device.</li>
     *     <li>DISCONNECTED if the device has disconnected OR the manager is closed.</li>
     * </ul>
     * If DISABLED is set, no other status may be set.  Once DISCONNECTED has been set, no other
     * status may be set, and the manager will be closed by the service if not already closed.
     *
     * @param status status to set
     */
    protected void updateStatus(DeviceStatusListener.Status status) {
        this.deviceStatus.setStatus(status);
        if (status == DeviceStatusListener.Status.READY) {
            registerDeviceAtReady();
        }
        this.service.deviceStatusUpdated(this, status);
    }

    /**
     * Register the device with the management portal once it is ready. If this is not desired,
     * override with an empty implementation.
     */
    protected void registerDeviceAtReady() {
        service.registerDevice(deviceName, Collections.<String, String>emptyMap());
    }

    /**
     * Creates and registers an Avro topic
     * @param name The name of the topic
     * @param valueClass The value class
     * @param <V>
     * @return
     */
    protected <V extends SpecificRecord> AvroTopic<ObservationKey, V> createTopic(String name, Class<V> valueClass) {
        try {
            Method method = valueClass.getMethod("getClassSchema");
            Schema valueSchema = (Schema) method.invoke(null);
            AvroTopic<ObservationKey, V> topic = new AvroTopic<>(
                    name, ObservationKey.getClassSchema(), valueSchema, ObservationKey.class, valueClass);
            dataHandler.registerTopic(topic);
            return topic;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | IOException e) {
            logger.error("Error creating topic " + name, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Send a single record, using the cache to persist the data.
     * If the current device is not registered when this is called, the data will NOT be sent.
     */
    protected <V extends SpecificRecord> void send(AvroTopic<ObservationKey, V> topic, V value) {
        ObservationKey key = deviceStatus.getId();
        if (key.getSourceId() != null) {
            try {
                dataHandler.addMeasurement(topic, key, value);
            } catch (IllegalArgumentException ex) {
                Crashlytics.log("Cannot send measurement for " + getState().getId());
                Crashlytics.logException(ex);
                logger.error("Cannot send to topic {}: {}", topic, ex.getMessage());
            }
        } else if (!didWarn) {
            logger.warn("Cannot send data without a source ID from {}", getClass().getSimpleName());
            didWarn = true;
        }
    }

    /**
     * Try to send a single record without any caching mechanism.
     * If the current device is not registered when this is called, the data will NOT be sent.
     *
     * @deprecated use {@link #trySend(AvroTopic, SpecificRecord)} instead.
     */
    @Deprecated
    protected <V extends SpecificRecord> void trySend(AvroTopic<ObservationKey, V> topic, long offset, V value) {
        trySend(topic, value);
    }

    /**
     * Try to send a single record without any caching mechanism.
     * If the current device is not registered when this is called, the data will NOT be sent.
     */
    protected <V extends SpecificRecord> void trySend(AvroTopic<ObservationKey, V> topic, V value) {
        ObservationKey key = deviceStatus.getId();
        if (key.getSourceId() != null) {
            try {
                dataHandler.trySend(topic, key, value);
            } catch (IllegalArgumentException ex) {
                Crashlytics.log("Cannot send measurement for " + getState().getId());
                Crashlytics.logException(ex);
                logger.warn("Cannot try to send to topic {}: {}", topic, ex.getMessage());
            }
        } else if (!didWarn) {
            logger.warn("Cannot send data without a source ID from {}", getClass().getSimpleName());
            didWarn = true;
        }
    }

    /** Get the service that started this device manager. */
    public S getService() {
        return service;
    }

    /** Whether this device manager has been closed. */
    @Override
    public boolean isClosed() {
        return closed;
    }

    /** Get the current device state. */
    @Override
    public T getState() {
        return deviceStatus;
    }

    /** Set the device name. Be sure to do this as soon as possible. */
    protected void setName(String name) {
        this.deviceName = name;
    }

    /** Get the name of the device. */
    @Override
    public String getName() {
        return deviceName;
    }

    @Override
    @CallSuper
    public void didRegister(AppSource source) {
        deviceName = source.getSourceName();
        getState().getId().setSourceId(source.getSourceId());
    }

    /**
     * Close the manager, disconnecting any device if necessary. Override and call super if
     * additional resources should be cleaned up. This implementation calls updateStatus with status
     * DISCONNECTED.
     */
    @Override
    public void close() throws IOException {
        closed = true;
        updateStatus(DeviceStatusListener.Status.DISCONNECTED);
    }

    @Override
    public String toString() {
        return "DeviceManager{name='" + deviceName + "', status=" + deviceStatus + '}';
    }

    /** Tests equality based on the device ObservationKey. */
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null || !getClass().equals(other.getClass())) {
            return false;
        }

        AbstractDeviceManager otherDevice = ((AbstractDeviceManager) other);
        return deviceStatus.getId().equals((otherDevice.deviceStatus.getId()));
    }

    @Override
    public int hashCode() {
        return deviceStatus.getId().hashCode();
    }
}
