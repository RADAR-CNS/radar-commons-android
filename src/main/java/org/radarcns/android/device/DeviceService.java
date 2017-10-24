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

import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.bluetooth.BluetoothAdapter;
import android.content.*;
import android.content.pm.PackageManager;
import android.os.*;
import android.support.annotation.CallSuper;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.util.Pair;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.android.R;
import org.radarcns.android.RadarApplication;
import org.radarcns.android.RadarConfiguration;
import org.radarcns.android.auth.AppAuthState;
import org.radarcns.android.auth.AppSource;
import org.radarcns.android.auth.ManagementPortalService;
import org.radarcns.android.data.DataCache;
import org.radarcns.android.data.TableDataHandler;
import org.radarcns.android.kafka.ServerStatusListener;
import org.radarcns.config.ServerConfig;
import org.radarcns.data.Record;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.producer.rest.SchemaRetriever;
import org.radarcns.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.radarcns.android.RadarConfiguration.*;
import static org.radarcns.android.device.DeviceServiceProvider.NEEDS_BLUETOOTH_KEY;
import static org.radarcns.android.device.DeviceServiceProvider.SOURCE_KEY;

/**
 * A service that manages a DeviceManager and a TableDataHandler to send addToPreferences the data of a
 * wearable device and send it to a Kafka REST proxy.
 *
 * Specific wearables should extend this class.
 */
@SuppressWarnings("WeakerAccess")
public abstract class DeviceService<T extends BaseDeviceState> extends Service implements DeviceStatusListener, ServerStatusListener {
    private static final int ONGOING_NOTIFICATION_ID = 11;
    private static final int BLUETOOTH_NOTIFICATION_ID = 12;
    private static final String PREFIX = "org.radarcns.android.";
    public static final String SERVER_STATUS_CHANGED = PREFIX + "ServerStatusListener.Status";
    public static final String SERVER_RECORDS_SENT_TOPIC = PREFIX + "ServerStatusListener.topic";
    public static final String SERVER_RECORDS_SENT_NUMBER = PREFIX + "ServerStatusListener.lastNumberOfRecordsSent";
    public static final String CACHE_TOPIC = PREFIX + "DataCache.topic";
    public static final String CACHE_RECORDS_UNSENT_NUMBER = PREFIX + "DataCache.numberOfRecords.first";
    public static final String CACHE_RECORDS_SENT_NUMBER = PREFIX + "DataCache.numberOfRecords.second";
    public static final String DEVICE_SERVICE_CLASS = PREFIX + "DeviceService.getClass";
    public static final String DEVICE_STATUS_CHANGED = PREFIX + "DeviceStatusListener.Status";
    public static final String DEVICE_STATUS_NAME = PREFIX + "Devicemanager.getName";
    public static final String DEVICE_CONNECT_FAILED = PREFIX + "DeviceStatusListener.deviceFailedToConnect";
    private final ObservationKey key = new ObservationKey();

    /** Stops the device when bluetooth is disabled. */
    private final BroadcastReceiver mBluetoothReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            final String action = intent.getAction();

            if (action.equals(BluetoothAdapter.ACTION_STATE_CHANGED)) {
                final int state = intent.getIntExtra(
                        BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR);
                switch (state) {
                    case BluetoothAdapter.STATE_TURNING_OFF: case BluetoothAdapter.STATE_OFF:
                        logger.warn("Bluetooth is off");
                        RadarApplication app = (RadarApplication) context.getApplicationContext();

                        PackageManager pm = context.getPackageManager();

                        Intent launchIntent = pm.getLaunchIntentForPackage(context.getPackageName());
                        PendingIntent pendingIntent = PendingIntent.getActivity(context, 0, launchIntent, 0);

                        Notification notification = app.updateNotificationAppSettings(new Notification.Builder(app))
                                .setContentIntent(pendingIntent)
                                .setContentText("Need bluetooth for data collection.")
                                .setContentTitle("Bluetooth needed")
                                .build();

                        NotificationManager manager = (NotificationManager) app.getSystemService(NOTIFICATION_SERVICE);
                        manager.notify(BLUETOOTH_NOTIFICATION_ID, notification);

                        stopDeviceManager(unsetDeviceManager());
                        break;
                    default:
                        logger.debug("Bluetooth is in state {}", state);
                        break;
                }
            }
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(DeviceService.class);
    private TableDataHandler dataHandler;
    private DeviceManager<T> deviceScanner;
    private DeviceBinder mBinder;
    private final AtomicInteger numberOfActivitiesBound = new AtomicInteger(0);
    private boolean isInForeground;
    private boolean isConnected;
    private int latestStartId = -1;
    private boolean needsBluetooth;
    private AppSource source;
    private ServerConfig managementPortal;
    private AppAuthState authState;

    @CallSuper
    @Override
    public void onCreate() {
        logger.info("Creating DeviceService {}", this);
        super.onCreate();
        mBinder = createBinder();

        if (isBluetoothConnectionRequired()) {
            IntentFilter intentFilter = new IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED);
            registerReceiver(mBluetoothReceiver, intentFilter);
        }

        synchronized (this) {
            numberOfActivitiesBound.set(0);
            isInForeground = false;
            deviceScanner = null;
        }
    }

    @CallSuper
    @Override
    public void onDestroy() {
        logger.info("Destroying DeviceService {}", this);
        super.onDestroy();

        if (isBluetoothConnectionRequired()) {
            // Unregister broadcast listeners
            unregisterReceiver(mBluetoothReceiver);
        }
        stopDeviceManager(unsetDeviceManager());
        ((RadarApplication)getApplicationContext()).onDeviceServiceDestroy(this);

        try {
            dataHandler.close();
        } catch (IOException e) {
            // do nothing
        }
    }

    @CallSuper
    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        logger.info("Starting DeviceService {}", this);
        synchronized (this) {
            latestStartId = startId;
        }
        if (intent != null) {
            onInvocation(intent.getExtras());
        }
        // If we get killed, after returning from here, restart
        // keep all the configuration from the previous iteration
        return START_REDELIVER_INTENT;
    }

    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        onRebind(intent);
        return mBinder;
    }

    @CallSuper
    @Override
    public void onRebind(Intent intent) {
        logger.info("Received (re)bind in {}", this);
        boolean isNew = numberOfActivitiesBound.getAndIncrement() == 0;
        RadarApplication application = (RadarApplication)getApplicationContext();
        if (intent != null) {
            Bundle extras = intent.getExtras();
            onInvocation(extras);
            application.onDeviceServiceInvocation(this, extras, isNew);
        } else {
            application.onDeviceServiceInvocation(this, null, isNew);
        }
    }

    @Override
    public boolean onUnbind(Intent intent) {
        logger.info("Received unbind in {}", this);
        stopSelfIfUnconnected();
        return true;
    }

    @Override
    public void deviceFailedToConnect(String deviceName) {
        Intent statusChanged = new Intent(DEVICE_CONNECT_FAILED);
        statusChanged.putExtra(DEVICE_SERVICE_CLASS, getClass().getName());
        statusChanged.putExtra(DEVICE_STATUS_NAME, deviceName);
        sendBroadcast(statusChanged);
    }


    private void broadcastDeviceStatus(String name, DeviceStatusListener.Status status) {
        Intent statusChanged = new Intent(DEVICE_STATUS_CHANGED);
        statusChanged.putExtra(DEVICE_STATUS_CHANGED, status.ordinal());
        statusChanged.putExtra(DEVICE_SERVICE_CLASS, getClass().getName());
        if (name != null) {
            statusChanged.putExtra(DEVICE_STATUS_NAME, name);
        }
        sendBroadcast(statusChanged);
    }

    @Override
    public void deviceStatusUpdated(DeviceManager deviceManager, DeviceStatusListener.Status status) {
        switch (status) {
            case CONNECTED:
                synchronized (this) {
                    isConnected = true;
                }
                startBackgroundListener();
                break;
            case DISCONNECTED:
                stopBackgroundListener();
                stopDeviceManager(deviceManager);
                synchronized (this) {
                    deviceScanner = null;
                    isConnected = false;
                }
                stopSelfIfUnconnected();
                break;
            default:
                // do nothing
                break;
        }
        broadcastDeviceStatus(deviceManager.getName(), status);
    }

    /** Stop service if no devices or activities are connected to it. */
    protected void stopSelfIfUnconnected() {
        int startId;
        synchronized (this) {
            if (numberOfActivitiesBound.get() > 0 || isConnected) {
                return;
            }
            startId = latestStartId;
        }
        logger.info("Stopping self if latest start ID was {}", startId);
        stopSelf(startId);
    }

    /** Maintain current service in the background. */
    public void startBackgroundListener() {
        logger.info("Preventing {} to get stopped.", this);
        synchronized (this) {
            if (isInForeground) {
                return;
            }
            isInForeground = true;
        }

        PackageManager pm = getPackageManager();
        Intent intent = pm.getLaunchIntentForPackage(getPackageName());
        PendingIntent pendingIntent = PendingIntent.getActivity(this, 0, intent, 0);

        startForeground(ONGOING_NOTIFICATION_ID, createBackgroundNotification(pendingIntent));
    }

    protected Notification createBackgroundNotification(PendingIntent intent) {
        RadarApplication app = (RadarApplication) getApplicationContext();
        return app.updateNotificationAppSettings(new Notification.Builder(app))
                .setContentIntent(intent)
                .setTicker(getText(R.string.service_notification_ticker))
                .setContentText(getText(R.string.service_notification_text))
                .setContentTitle(getText(R.string.service_notification_title))
                .build();
    }

    /** Service no longer needs to be maintained in the background. */
    public void stopBackgroundListener() {
        logger.info("{} may be stopped.", this);
        synchronized (this) {
            if (!isInForeground) {
                return;
            }
            isInForeground = false;
        }
        stopForeground(true);
    }

    private synchronized DeviceManager unsetDeviceManager() {
        DeviceManager tmpManager = deviceScanner;
        deviceScanner = null;
        return tmpManager;
    }

    private void stopDeviceManager(DeviceManager deviceManager) {
        if (deviceManager != null) {
            if (!deviceManager.isClosed()) {
                try {
                    deviceManager.close();
                } catch (IOException e) {
                    logger.warn("Failed to close device scanner", e);
                }
            }
        }
    }

    @Override
    public void updateServerStatus(ServerStatusListener.Status status) {
        // TODO: if status == UNAUTHORIZED, start login activity
        // TODO: make sure that the AppAuthState gets propagated back to all services -> perhaps
        //       with a broadcast instead of going through MainActivity
        Intent statusIntent = new Intent(SERVER_STATUS_CHANGED);
        statusIntent.putExtra(SERVER_STATUS_CHANGED, status.ordinal());
        statusIntent.putExtra(DEVICE_SERVICE_CLASS, getClass().getName());
        sendBroadcast(statusIntent);
    }

    @Override
    public void updateRecordsSent(String topicName, int numberOfRecords) {
        Intent recordsIntent = new Intent(SERVER_RECORDS_SENT_TOPIC);
        // Signal that a certain topic changed, the key of the map retrieved by getRecordsSent().
        recordsIntent.putExtra(SERVER_RECORDS_SENT_TOPIC, topicName);
        recordsIntent.putExtra(SERVER_RECORDS_SENT_NUMBER, numberOfRecords);
        recordsIntent.putExtra(DEVICE_SERVICE_CLASS, getClass().getName());
        sendBroadcast(recordsIntent);
    }

    /**
     * New device manager for the current device.
     */
    protected abstract DeviceManager<T> createDeviceManager();

    protected T getState() {
        DeviceManager<T> localManager = getDeviceManager();
        if (localManager == null) {
            T state = getDefaultState();
            ObservationKey stateKey = state.getId();
            stateKey.setProjectId(key.getProjectId());
            stateKey.setUserId(key.getUserId());
            stateKey.setSourceId(key.getSourceId());
            return state;
        }
        return localManager.getState();
    }

    /**
     * Default state when no device manager is active.
     */
    protected abstract T getDefaultState();

    public BaseDeviceState startRecording(@NonNull Set<String> acceptableIds) {
        DeviceManager localManager = getDeviceManager();
        if (key.getUserId() == null) {
            throw new IllegalStateException("Cannot start recording: user ID is not set.");
        }
        if (localManager == null) {
            logger.info("Starting recording");
            synchronized (this) {
                if (deviceScanner == null) {
                    if (key.getSourceId() == null) {
                        key.setSourceId(RadarConfiguration.getOrSetUUID(
                                getApplicationContext(), SOURCE_ID_KEY));
                    }
                    deviceScanner = createDeviceManager();
                    deviceScanner.start(acceptableIds);
                }
            }
        }
        return getDeviceManager().getState();
    }

    public void stopRecording() {
        stopDeviceManager(unsetDeviceManager());
        logger.info("Stopped recording {}", this);
    }

    protected class DeviceBinder extends Binder implements DeviceServiceBinder {
        @Override
        public <V extends SpecificRecord> List<Record<ObservationKey, V>> getRecords(
                @NonNull AvroTopic<ObservationKey, V> topic, int limit) throws IOException {
            TableDataHandler localDataHandler = getDataHandler();
            if (localDataHandler == null) {
                return Collections.emptyList();
            }
            return localDataHandler.getCache(topic).getRecords(limit);
        }

        @Override
        public BaseDeviceState getDeviceStatus() {
            return getState();
        }

        @Override
        public String getDeviceName() {
            DeviceManager localManager = getDeviceManager();
            if (localManager == null) {
                return null;
            }
            return localManager.getName();
        }

        @Override
        public BaseDeviceState startRecording(@NonNull Set<String> acceptableIds) {
            return DeviceService.this.startRecording(acceptableIds);
        }

        @Override
        public void stopRecording() {
            DeviceService.this.stopRecording();
        }

        @Override
        public ServerStatusListener.Status getServerStatus() {
            TableDataHandler localDataHandler = getDataHandler();
            if (localDataHandler == null) {
                return ServerStatusListener.Status.DISCONNECTED;
            }
            return localDataHandler.getStatus();
        }

        @Override
        public Map<String,Integer> getServerRecordsSent() {
            TableDataHandler localDataHandler = getDataHandler();
            if (localDataHandler == null) {
                return Collections.emptyMap();
            }
            return localDataHandler.getRecordsSent();
        }

        @Override
        public void updateConfiguration(Bundle bundle) {
            onInvocation(bundle);
        }

        @Override
        public Pair<Long, Long> numberOfRecords() {
            long unsent = -1L;
            long sent = -1L;
            TableDataHandler localDataHandler = getDataHandler();
            if (localDataHandler != null) {
                for (DataCache<?, ?> cache : localDataHandler.getCaches().values()) {
                    Pair<Long, Long> pair = cache.numberOfRecords();
                    if (pair.first != -1L) {
                        if (unsent == -1L) {
                            unsent = pair.first;
                        } else {
                            unsent += pair.first;
                        }
                    }
                    if (pair.second != -1L) {
                        if (sent == -1L) {
                            sent = pair.second;
                        } else {
                            sent += pair.second;
                        }
                    }
                }
            }
            return new Pair<>(unsent, sent);
        }

        @Override
        public void setDataHandler(TableDataHandler dataHandler) {
            DeviceService.this.setDataHandler(dataHandler);
        }

        @Override
        public boolean onTransact(int code, Parcel data, Parcel reply, int flags) throws RemoteException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Override this function to get any parameters from the given intent.
     *
     * @param bundle intent extras that the activity provided.
     */
    @CallSuper
    protected void onInvocation(Bundle bundle) {
        authState = AppAuthState.Builder.from(bundle).build();

        source = bundle.getParcelable(SOURCE_KEY);
        if (source == null) {
            source = new AppSource(-1L, null, null, null, true);
        }
        if (source.getSourceId() != null) {
            key.setSourceId(source.getSourceId());
        }
        key.setUserId(authState.getUserId());
        String managementPortalString = bundle.getString(RADAR_PREFIX + MANAGEMENT_PORTAL_URL_KEY, null);
        if (managementPortalString != null) {
            try {
                managementPortal = new ServerConfig(managementPortalString);
            } catch (MalformedURLException ex) {
                logger.error("ManagementPortal url {} is invalid", managementPortalString, ex);
            }
        }

        boolean willNeedBluetooth = bundle.getBoolean(NEEDS_BLUETOOTH_KEY, false);
        if (!willNeedBluetooth && needsBluetooth) {
            unregisterReceiver(mBluetoothReceiver);
            needsBluetooth = false;
        } else if (willNeedBluetooth && !needsBluetooth) {
            // Register for broadcasts on BluetoothAdapter state change
            IntentFilter filter = new IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED);
            registerReceiver(mBluetoothReceiver, filter);
            needsBluetooth = true;
        }

        setHasBluetoothPermission(bundle.getBoolean(NEEDS_BLUETOOTH_KEY, false));
    }

    public synchronized TableDataHandler getDataHandler() {
        return dataHandler;
    }

    public synchronized void setDataHandler(TableDataHandler dataHandler) {
        this.dataHandler = dataHandler;
    }

    public synchronized DeviceManager<T> getDeviceManager() {
        return deviceScanner;
    }

    /** Get the service local binder. */
    @NonNull
    protected DeviceBinder createBinder() {
        return new DeviceBinder();
    }

    protected void setHasBluetoothPermission(boolean isRequired) {
        boolean oldBluetoothNeeded = isBluetoothConnectionRequired();
        needsBluetooth = isRequired;
        boolean newBluetoothNeeded = isBluetoothConnectionRequired();

        if (oldBluetoothNeeded && !newBluetoothNeeded) {
            unregisterReceiver(mBluetoothReceiver);
        } else if (!oldBluetoothNeeded && newBluetoothNeeded) {
            // Register for broadcasts on BluetoothAdapter state change
            IntentFilter filter = new IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED);
            registerReceiver(mBluetoothReceiver, filter);
        }
    }

    protected boolean hasBluetoothPermission() {
        return needsBluetooth;
    }

    protected boolean isBluetoothConnectionRequired() {
        return hasBluetoothPermission();
    }

    public ObservationKey getKey() {
        return key;
    }

    public AppSource getSource() {
        return source;
    }

    public void registerDevice(String name, Map<String, String> attributes) {
        registerDevice(null, name, attributes);
    }

    public void registerDevice(String sourceIdHint, String name, Map<String, String> attributes) {
        if (source.getSourceId() != null) {
            getDeviceManager().didRegister(source);;
        }
        source.setSourceName(name);
        source.setAttributes(attributes);
        if (managementPortal != null) {
            Intent intent = ManagementPortalService.createRequest(this, managementPortal,
                    source, authState, new ResultReceiver(new Handler(getMainLooper())) {
                @Override
                protected void onReceiveResult(int resultCode, Bundle result) {
                    AppSource updatedSource = result.getParcelable(SOURCE_KEY);
                    if (updatedSource == null) {
                        // TODO: more error handling?
                        logger.error("Failed to register source {}", source);
                        stopDeviceManager(unsetDeviceManager());
                        return;
                    }
                    AppAuthState auth = AppAuthState.Builder.from(result).build();
                    if (auth.isInvalidated()) {
                        logger.info("New source ID requires new OAuth2 JWT token.");
                        updateServerStatus(ServerStatusListener.Status.UNAUTHORIZED);
                    }
                    key.setProjectId(auth.getProjectId());
                    key.setUserId(auth.getUserId());
                    key.setSourceId(updatedSource.getSourceId());
                    source.setSourceId(updatedSource.getSourceId());
                    source.setSourceName(updatedSource.getSourceName());
                    source.setExpectedSourceName(updatedSource.getExpectedSourceName());
                    DeviceManager<T> localManager = getDeviceManager();
                    if (localManager != null) {
                        localManager.didRegister(source);
                    }
                }
            });
            startService(intent);
        } else {
            if (sourceIdHint == null) {
                source.setSourceId(RadarConfiguration.getOrSetUUID(this, SOURCE_ID_KEY));
            } else {
                source.setSourceId(sourceIdHint);
            }
            key.setSourceId(source.getSourceId());
            getDeviceManager().didRegister(source);
        }
    }

    @Override
    public String toString() {
        DeviceManager localManager = getDeviceManager();
        if (localManager == null) {
            return getClass().getSimpleName() + "<null>";
        } else {
            return getClass().getSimpleName() + "<" + localManager.getName() + ">";
        }
    }
}
