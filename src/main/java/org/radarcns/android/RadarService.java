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

package org.radarcns.android;

import android.app.AppOpsManager;
import android.app.Notification;
import android.app.PendingIntent;
import android.app.Service;
import android.bluetooth.BluetoothAdapter;
import android.content.*;
import android.content.pm.PackageManager;
import android.location.LocationManager;
import android.os.*;
import android.support.annotation.NonNull;
import android.support.v4.content.ContextCompat;
import android.widget.Toast;
import org.radarcns.android.auth.AppAuthState;
import org.radarcns.android.auth.AppSource;
import org.radarcns.android.auth.LoginActivity;
import org.radarcns.android.auth.portal.ManagementPortalService;
import org.radarcns.android.data.TableDataHandler;
import org.radarcns.android.device.DeviceServiceConnection;
import org.radarcns.android.device.DeviceServiceProvider;
import org.radarcns.android.device.DeviceStatusListener;
import org.radarcns.android.kafka.ServerStatusListener;
import org.radarcns.android.util.Boast;
import org.radarcns.android.util.BundleSerialization;
import org.radarcns.config.ServerConfig;
import org.radarcns.data.TimedInt;
import org.radarcns.producer.rest.SchemaRetriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static android.Manifest.permission.*;
import static org.radarcns.android.RadarConfiguration.*;
import static org.radarcns.android.auth.portal.GetSubjectParser.getHumanReadableUserId;
import static org.radarcns.android.auth.portal.ManagementPortalClient.MP_REFRESH_TOKEN_PROPERTY;
import static org.radarcns.android.auth.portal.ManagementPortalClient.SOURCES_PROPERTY;
import static org.radarcns.android.auth.portal.ManagementPortalService.MANAGEMENT_PORTAL_REFRESH;
import static org.radarcns.android.auth.portal.ManagementPortalService.MANAGEMENT_PORTAL_REFRESH_FAILED;
import static org.radarcns.android.device.DeviceService.DEVICE_CONNECT_FAILED;
import static org.radarcns.android.device.DeviceService.DEVICE_STATUS_NAME;
import static org.radarcns.android.device.DeviceService.SERVER_RECORDS_SENT_NUMBER;
import static org.radarcns.android.device.DeviceService.SERVER_RECORDS_SENT_TOPIC;
import static org.radarcns.android.device.DeviceService.SERVER_STATUS_CHANGED;

@SuppressWarnings("unused")
public class RadarService extends Service implements ServerStatusListener {
    private static final Logger logger = LoggerFactory.getLogger(RadarService.class);

    public static String RADAR_PACKAGE = RadarService.class.getPackage().getName();

    public static String EXTRA_MAIN_ACTIVITY = RADAR_PACKAGE + ".EXTRA_MAIN_ACTIVITY";
    public static String EXTRA_LOGIN_ACTIVITY = RADAR_PACKAGE + ".EXTRA_LOGIN_ACTIVITY";

    public static String ACTION_CHECK_PERMISSIONS = RADAR_PACKAGE + ".ACTION_CHECK_PERMISSIONS";
    public static String EXTRA_PERMISSIONS = RADAR_PACKAGE + ".EXTRA_PERMISSIONS";

    public static String ACTION_PERMISSIONS_GRANTED = RADAR_PACKAGE + ".ACTION_PERMISSIONS_GRANTED";
    public static String EXTRA_GRANT_RESULTS = RADAR_PACKAGE + ".EXTRA_GRANT_RESULTS";

    private final BroadcastReceiver permissionsBroadcastReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            onPermissionsGranted(intent.getStringArrayExtra(EXTRA_PERMISSIONS), intent.getIntArrayExtra(EXTRA_GRANT_RESULTS));
        }
    };

    private final BroadcastReceiver loginBroadcastReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            Bundle bundle = intent.getExtras();
            if (bundle == null) {
                return;
            }
            bundle.setClassLoader(RadarService.class.getClassLoader());
            updateAuthState(AppAuthState.Builder.from(bundle).build());
        }
    };

    private IBinder binder;

    private TableDataHandler dataHandler;
    private String mainActivityClass;
    private String loginActivityClass;
    private Handler mHandler;

    /** Filters to only listen to certain device IDs. */
    private final Map<DeviceServiceConnection, Set<String>> deviceFilters = new HashMap<>();

    /** Defines callbacks for service binding, passed to bindService() */
    private final BroadcastReceiver  bluetoothReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            final String action = intent.getAction();

            if (Objects.equals(action, BluetoothAdapter.ACTION_STATE_CHANGED)) {
                final int state = intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR);
                logger.info("Bluetooth state {}", state);
                // Upon state change, restart ui handler and restart Scanning.
                if (state == BluetoothAdapter.STATE_ON) {
                    logger.info("Bluetooth is on");
                    startScanning();
                } else if (state == BluetoothAdapter.STATE_OFF) {
                    logger.warn("Bluetooth is off");
                    startScanning();
                }
            }
        }
    };

    private final BroadcastReceiver deviceFailedReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, final Intent intent) {
            if (Objects.equals(intent.getAction(), DEVICE_CONNECT_FAILED)) {
                Boast.makeText(RadarService.this,
                        "Cannot connect to device " + intent.getStringExtra(DEVICE_STATUS_NAME),
                        Toast.LENGTH_SHORT).show();
            }
        }
    };

    private final BroadcastReceiver serverStatusReceiver = new BroadcastReceiver() {
        AtomicBoolean isMakingRequest = new AtomicBoolean(false);
        @Override
        public void onReceive(Context context, Intent intent) {
            serverStatus = Status.values()[intent.getIntExtra(SERVER_STATUS_CHANGED, -1)];
            if (serverStatus == Status.UNAUTHORIZED) {
                logger.info("Status unauthorized");
                if (!isMakingRequest.compareAndSet(false, true)) {
                    return;
                }
                final String refreshToken = (String) authState.getProperty(MP_REFRESH_TOKEN_PROPERTY);
                if (ManagementPortalService.isEnabled() && refreshToken != null) {
                    authState.invalidate(RadarService.this);
                    logger.info("Creating request to management portal");
                    ManagementPortalService.requestAccessToken(RadarService.this,
                            refreshToken, false, new ResultReceiver(mHandler) {
                        @Override
                        protected void onReceiveResult(int resultCode, Bundle result) {
                            if (resultCode == MANAGEMENT_PORTAL_REFRESH) {
                                authState = AppAuthState.Builder.from(result).build();
                                if (dataHandler != null) {
                                    dataHandler.setAuthState(authState);
                                }
                                isMakingRequest.set(false);
                                dataHandler.checkConnection();
                            } else if (resultCode == MANAGEMENT_PORTAL_REFRESH_FAILED && mHandler != null) {
                                logger.error("Failed to log in to management portal");
                                final ResultReceiver recv = this;
                                mHandler.postDelayed(new Runnable() {
                                    @Override
                                    public void run() {
                                        ManagementPortalService.requestAccessToken(RadarService.this, refreshToken, false, recv);
                                    }
                                }, ThreadLocalRandom.current().nextLong(1_000L, 120_000L));
                            } else {
                                isMakingRequest.set(false);
                            }
                        }
                    });
                } else {
                    synchronized (RadarService.this) {
                        // login already started, or was finished up to 3 seconds ago (give time to propagate new auth state.)
                        if (authState.isInvalidated() || authState.timeSinceLastUpdate() < 3_000L) {
                            return;
                        }
                        authState.invalidate(RadarService.this);
                    }
                    startLogin();
                }
            }
        }
    };

    private final BroadcastReceiver configChangedReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            configure();
        }
    };

    /** Connections. **/
    private final List<DeviceServiceProvider> mConnections = new ArrayList<>();

    /** An overview of how many records have been sent throughout the application. */
    private final TimedInt latestNumberOfRecordsSent = new TimedInt();

    /** Current server status. */
    private ServerStatusListener.Status serverStatus;
    private AppAuthState authState;

    private final LinkedHashSet<String> needsPermissions = new LinkedHashSet<>();
    private boolean requestedBt;


    @Override
    public IBinder onBind(Intent intent) {
        return binder;
    }

    @Override
    public void onCreate() {
        super.onCreate();

        binder = createBinder();
        mHandler = new Handler(getMainLooper());

        registerReceiver(permissionsBroadcastReceiver,
                new IntentFilter(ACTION_PERMISSIONS_GRANTED));
        registerReceiver(loginBroadcastReceiver,
                new IntentFilter(LoginActivity.ACTION_LOGIN_SUCCESS));
        registerReceiver(bluetoothReceiver, new IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED));
        registerReceiver(deviceFailedReceiver, new IntentFilter(DEVICE_CONNECT_FAILED));
        registerReceiver(serverStatusReceiver, new IntentFilter(SERVER_STATUS_CHANGED));
        registerReceiver(configChangedReceiver, new IntentFilter(RADAR_CONFIGURATION_CHANGED));
    }

    protected IBinder createBinder() {
        return new RadarBinder();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        Bundle extras = BundleSerialization.getPersistentExtras(intent, this);
        extras.setClassLoader(RadarService.class.getClassLoader());
        mainActivityClass = extras.getString(EXTRA_MAIN_ACTIVITY);
        loginActivityClass = extras.getString(EXTRA_LOGIN_ACTIVITY);

        if (intent == null) {
            authState = AppAuthState.Builder.from(this).build();
        } else {
            authState = AppAuthState.Builder.from(extras).build();
        }
        logger.info("Auth state: {}", authState);

        configure();

        new AsyncBindServices(false)
                .execute(mConnections.toArray(new DeviceServiceProvider[mConnections.size()]));

        checkPermissions();

        startForeground(1,
                new Notification.Builder(this)
                        .setContentTitle("RADAR")
                        .setContentText("Open RADAR app")
                        .setContentIntent(PendingIntent.getActivity(this, 0, new Intent().setComponent(new ComponentName(this, mainActivityClass)), 0))
                        .build());

        if (authState.getProperty(SOURCES_PROPERTY) == null && ManagementPortalService.isEnabled()) {
            ManagementPortalService.requestAccessToken(this, null, true, new ResultReceiver(mHandler) {
                @Override
                protected void onReceiveResult(int resultCode, Bundle resultData) {
                    super.onReceiveResult(resultCode, resultData);
                    if (resultCode == MANAGEMENT_PORTAL_REFRESH) {
                        authState = AppAuthState.Builder.from(resultData).build();
                        configure();
                    }
                }
            });
        }

        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        mHandler = null;
        unregisterReceiver(permissionsBroadcastReceiver);
        unregisterReceiver(loginBroadcastReceiver);
        unregisterReceiver(bluetoothReceiver);
        unregisterReceiver(deviceFailedReceiver);
        unregisterReceiver(serverStatusReceiver);

        for (DeviceServiceProvider provider : mConnections) {
            if (provider.isBound()) {
                logger.info("Unbinding service: {}", provider);
                provider.unbind();
            } else {
                logger.info("Already unbound: {}", provider);
            }
        }

        super.onDestroy();
    }

    protected void configure() {
        RadarConfiguration configuration = RadarConfiguration.getInstance();

        TableDataHandler localDataHandler;
        ServerConfig kafkaConfig = null;
        SchemaRetriever remoteSchemaRetriever = null;
        boolean unsafeConnection = configuration.getBoolean(UNSAFE_KAFKA_CONNECTION, false);

        if (configuration.has(KAFKA_REST_PROXY_URL_KEY)) {
            String urlString = configuration.getString(KAFKA_REST_PROXY_URL_KEY);
            if (!urlString.isEmpty()) {
                try {
                    ServerConfig schemaRegistry = new ServerConfig(configuration.getString(SCHEMA_REGISTRY_URL_KEY));
                    schemaRegistry.setUnsafe(unsafeConnection);
                    remoteSchemaRetriever = new SchemaRetriever(schemaRegistry, 30);
                    kafkaConfig = new ServerConfig(urlString);
                    kafkaConfig.setUnsafe(unsafeConnection);
                } catch (MalformedURLException ex) {
                    logger.error("Malformed Kafka server URL {}", urlString);
                    throw new IllegalArgumentException(ex);
                }
            }
        }

        boolean sendOnlyWithWifi = configuration.getBoolean(SEND_ONLY_WITH_WIFI, true);

        int maxBytes = configuration.getInt(MAX_CACHE_SIZE, Integer.MAX_VALUE);

        boolean newlyCreated;
        synchronized (this) {
            if (dataHandler == null) {
                try {
                    dataHandler = new TableDataHandler(
                            this, kafkaConfig, remoteSchemaRetriever, maxBytes,
                            sendOnlyWithWifi, authState);
                    newlyCreated = true;
                } catch (IOException ex) {
                    logger.error("Failed to instantiate Data Handler", ex);
                    throw new IllegalStateException(ex);
                }
            } else {
                newlyCreated = false;
            }
            localDataHandler = dataHandler;
        }

        if (!newlyCreated) {
            if (kafkaConfig == null) {
                localDataHandler.disableSubmitter();
            } else {
                localDataHandler.setKafkaConfig(kafkaConfig);
                localDataHandler.setSchemaRetriever(remoteSchemaRetriever);
            }
            localDataHandler.setMaximumCacheSize(maxBytes);
            localDataHandler.setAuthState(authState);
        }

        localDataHandler.setSendOnlyWithWifi(sendOnlyWithWifi);
        localDataHandler.setCompression(configuration.getBoolean(SEND_WITH_COMPRESSION, false));

        if (configuration.has(DATA_RETENTION_KEY)) {
            localDataHandler.setDataRetention(
                    configuration.getLong(DATA_RETENTION_KEY));
        }
        if (configuration.has(KAFKA_UPLOAD_RATE_KEY)) {
            localDataHandler.setKafkaUploadRate(
                    configuration.getLong(KAFKA_UPLOAD_RATE_KEY));
        }
        if (configuration.has(KAFKA_RECORDS_SEND_LIMIT_KEY)) {
            localDataHandler.setKafkaRecordsSendLimit(
                    configuration.getInt(KAFKA_RECORDS_SEND_LIMIT_KEY));
        }
        if (configuration.has(SENDER_CONNECTION_TIMEOUT_KEY)) {
            localDataHandler.setSenderConnectionTimeout(
                    configuration.getLong(SENDER_CONNECTION_TIMEOUT_KEY));
        }
        if (configuration.has( DATABASE_COMMIT_RATE_KEY)) {
            localDataHandler.setDatabaseCommitRate(
                    configuration.getLong(DATABASE_COMMIT_RATE_KEY));
        }
        if (configuration.has(KAFKA_UPLOAD_MINIMUM_BATTERY_LEVEL)) {
            localDataHandler.setMinimumBatteryLevel(configuration.getFloat(
                    KAFKA_UPLOAD_MINIMUM_BATTERY_LEVEL));
        }

        if (newlyCreated) {
            localDataHandler.addStatusListener(this);
            localDataHandler.start();
        } else if (kafkaConfig != null) {
            localDataHandler.enableSubmitter();
        }

        List<DeviceServiceProvider> connections = DeviceServiceProvider.loadProviders(this, RadarConfiguration.getInstance());

        Iterator<DeviceServiceProvider> iter = mConnections.iterator();
        while (iter.hasNext()) {
            DeviceServiceProvider provider = iter.next();
            if (!connections.contains(provider)) {
                provider.unbind();
                iter.remove();
            }
        }

        boolean useMp = configuration.getString(MANAGEMENT_PORTAL_URL_KEY, null) != null;

        boolean didAddProvider = false;
        for (DeviceServiceProvider provider : connections) {
            if (!mConnections.contains(provider)) {
                @SuppressWarnings("unchecked")
                List<AppSource> sources = (List<AppSource>) authState.getProperties().get(SOURCES_PROPERTY);
                if (sources != null) {
                    for (AppSource source : sources) {
                        if (provider.matches(source, false)) {
                            provider.setSource(source);
                            addProvider(provider);
                            didAddProvider = true;
                            break;
                        }
                    }
                } else if (!useMp) {
                    addProvider(provider);
                    didAddProvider = true;
                }
            }
        }

        for (DeviceServiceProvider provider : mConnections) {
            provider.updateConfiguration();
        }

        if (didAddProvider) {
            checkPermissions();
        }
    }

    private void addProvider(DeviceServiceProvider provider) {
        mConnections.add(provider);
        DeviceServiceConnection connection = provider.getConnection();
        deviceFilters.put(connection, Collections.<String>emptySet());
    }

    public TableDataHandler getDataHandler() {
        return dataHandler;
    }

    protected void requestPermissions(String[] permissions) {
        startActivity(new Intent()
                .setComponent(new ComponentName(this, mainActivityClass))
                .addFlags(Intent.FLAG_ACTIVITY_REORDER_TO_FRONT)
                .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                .setAction(ACTION_CHECK_PERMISSIONS)
                .putExtra(EXTRA_PERMISSIONS, permissions));
    }

    protected void onPermissionsGranted(String[] permissions, int[] grantResults) {
        for (int i = 0; i < permissions.length; i++) {
            if (grantResults[i] == PackageManager.PERMISSION_GRANTED) {
                logger.info("Granted permission {}", permissions[i]);
                needsPermissions.remove(permissions[i]);
            } else {
                logger.info("Denied permission {}", permissions[i]);
                return;
            }
        }
        // Permission granted.
        startScanning();
    }

    protected void startLogin() {
        startActivity(new Intent().setComponent(new ComponentName(this, loginActivityClass)));
    }

    protected void updateAuthState(AppAuthState authState) {
        this.authState = authState;
        RadarConfiguration.getInstance().put(RadarConfiguration.PROJECT_ID_KEY, authState.getProjectId());
        RadarConfiguration.getInstance().put(RadarConfiguration.USER_ID_KEY, getHumanReadableUserId(authState));
        configure();
    }

    public void serviceConnected(DeviceServiceConnection<?> connection) {
        ServerStatusListener.Status status = connection.getServerStatus();
        logger.info("Initial server status: {}", status);
        updateServerStatus(status);
        startScanning();
    }

    public void serviceDisconnected(final DeviceServiceConnection<?> connection) {
        new AsyncBindServices(true)
                .execute(getConnectionProvider(connection));
    }

    public void updateServerStatus(ServerStatusListener.Status serverStatus) {
        if (serverStatus == this.serverStatus) {
            return;
        }
        this.serverStatus = serverStatus;

        Intent statusIntent = new Intent(SERVER_STATUS_CHANGED);
        statusIntent.putExtra(SERVER_STATUS_CHANGED, serverStatus.ordinal());
        sendBroadcast(statusIntent);
    }

    @Override
    public void updateRecordsSent(String topicName, int numberOfRecords) {
        this.latestNumberOfRecordsSent.set(numberOfRecords);
        Intent recordsIntent = new Intent(SERVER_RECORDS_SENT_TOPIC);
        // Signal that a certain topic changed, the key of the map retrieved by getRecordsSent().
        recordsIntent.putExtra(SERVER_RECORDS_SENT_TOPIC, topicName);
        recordsIntent.putExtra(SERVER_RECORDS_SENT_NUMBER, numberOfRecords);
        sendBroadcast(recordsIntent);
    }

    public void deviceStatusUpdated(final DeviceServiceConnection<?> connection, final DeviceStatusListener.Status status) {
        new Handler(Looper.getMainLooper()).post(new Runnable() {
            @Override
            public void run() {
                int showRes = -1;
                switch (status) {
                    case READY:
                        showRes = R.string.device_ready;
                        break;
                    case CONNECTED:
                        showRes = R.string.device_connected;
                        break;
                    case CONNECTING:
                        showRes = R.string.device_connecting;
                        logger.info( "Device name is {} while connecting.", connection.getDeviceName());
                        break;
                    case DISCONNECTED:
                        showRes = R.string.device_disconnected;
                        startScanning();
                        break;
                    default:
                        break;
                }
                if (showRes != -1) {
                    Boast.makeText(RadarService.this, showRes).show();
                }
            }
        });
    }

    protected DeviceServiceProvider getConnectionProvider(DeviceServiceConnection<?> connection) {
        for (DeviceServiceProvider provider : mConnections) {
            if (provider.getConnection().equals(connection)) {
                return provider;
            }
        }
        logger.info("DeviceServiceConnection no longer enabled");
        return null;
    }


    protected void startScanning() {
        requestedBt = false;
        for (DeviceServiceProvider<?> provider : mConnections) {
            DeviceServiceConnection connection = provider.getConnection();
            if (!connection.hasService() || connection.isRecording() || !checkPermissions(provider)) {
                continue;
            }

            logger.info("Starting recording on connection {}", connection);
            AppSource source = provider.getSource();
            Set<String> filters;
            if (source != null && source.getExpectedSourceName() != null) {
                String[] expectedIds = source.getExpectedSourceName().split(",");
                filters = new HashSet<>(Arrays.asList(expectedIds));
            } else {
                filters = deviceFilters.get(connection);
            }
            connection.startRecording(filters);
        }
    }

    protected void checkPermissions() {
        Set<String> permissions = new HashSet<>(getServicePermissions());
        for (DeviceServiceProvider<?> provider : mConnections) {
            permissions.addAll(provider.needsPermissions());
        }

        needsPermissions.clear();

        for (String permission : permissions) {
            if (permission.equals(ACCESS_FINE_LOCATION) || permission.equals(ACCESS_COARSE_LOCATION)) {
                LocationManager locationManager = (LocationManager) getSystemService(Context.LOCATION_SERVICE);
                if (locationManager != null) {
                    boolean isGpsEnabled = locationManager.isProviderEnabled(LocationManager.GPS_PROVIDER);
                    boolean isNetworkEnabled = locationManager.isProviderEnabled(LocationManager.NETWORK_PROVIDER);

                    //Start your Activity if location was enabled:
                    if (!isGpsEnabled && !isNetworkEnabled) {
                        //needsPermissions.add(LOCATION_SERVICE);
                        //needsPermissions.add(permission);
                    }
                }
            }

            if (permission.equals(PACKAGE_USAGE_STATS)) {
                AppOpsManager appOps = (AppOpsManager) getSystemService(Context.APP_OPS_SERVICE);
                if (appOps != null) {
                    int mode = appOps.checkOpNoThrow(
                            "android:get_usage_stats", android.os.Process.myUid(), getPackageName());

                    if (mode != AppOpsManager.MODE_ALLOWED) {
                        needsPermissions.add(permission);
                    }
                }
            } else if (ContextCompat.checkSelfPermission(this, permission) != PackageManager
                    .PERMISSION_GRANTED) {
                logger.info("Need to request permission for {}", permission);
                needsPermissions.add(permission);
            }
        }

        if (!needsPermissions.isEmpty()) {
            requestPermissions(needsPermissions.toArray(new String[needsPermissions.size()]));
        }
    }

    protected List<String> getServicePermissions() {
        return Arrays.asList(ACCESS_NETWORK_STATE, INTERNET);
    }


    protected boolean checkPermissions(DeviceServiceProvider<?> provider) {
        List<String> providerPermissions = provider.needsPermissions();

        if (providerPermissions.contains(BLUETOOTH)) {
            if (requestedBt || requestEnableBt()) {
                logger.info("Cannot start scanning on service {} until bluetooth is turned on.",
                        provider.getConnection());
                requestedBt = true;
                return false;
            }
        }
        for (String permission : providerPermissions) {
            if (needsPermissions.contains(permission)) {
                // cannot start
                return false;
            }
        }
        return true;
    }

    /**
     * Sends an intent to request bluetooth to be turned on.
     * @return whether a request was sent
     */
    protected boolean requestEnableBt() {
        BluetoothAdapter btAdaptor = BluetoothAdapter.getDefaultAdapter();
        if (!btAdaptor.isEnabled()) {
            Intent btIntent = new Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE);
            btIntent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
            getApplicationContext().startActivity(btIntent);
            return true;
        } else {
            return false;
        }
    }

    /** Disconnect from all services. */
    protected void disconnect() {
        for (DeviceServiceProvider provider : mConnections) {
            disconnect(provider.getConnection());
        }
    }

    /** Disconnect from given service. */
    public void disconnect(DeviceServiceConnection connection) {
        if (connection.isRecording()) {
            connection.stopRecording();
        }
    }

    /** Configure whether a boot listener should start this application at boot. */
    protected void configureRunAtBoot(@NonNull Class<?> bootReceiver) {
        ComponentName receiver = new ComponentName(
                getApplicationContext(), bootReceiver);
        PackageManager pm = getApplicationContext().getPackageManager();

        boolean startAtBoot = RadarConfiguration.getInstance().getBoolean(RadarConfiguration.START_AT_BOOT, false);
        boolean isStartedAtBoot = pm.getComponentEnabledSetting(receiver) == PackageManager.COMPONENT_ENABLED_STATE_ENABLED;
        if (startAtBoot && !isStartedAtBoot) {
            logger.info("From now on, this application will start at boot");
            pm.setComponentEnabledSetting(receiver,
                    PackageManager.COMPONENT_ENABLED_STATE_ENABLED,
                    PackageManager.DONT_KILL_APP);
        } else if (!startAtBoot && isStartedAtBoot) {
            logger.info("Not starting application at boot anymore");
            pm.setComponentEnabledSetting(receiver,
                    PackageManager.COMPONENT_ENABLED_STATE_DISABLED,
                    PackageManager.DONT_KILL_APP);
        }
    }

    protected class RadarBinder extends Binder implements IRadarService {
        @Override
        public ServerStatusListener.Status getServerStatus() {
             return serverStatus;
        }

        @Override
        public TimedInt getLatestNumberOfRecordsSent() {
            return latestNumberOfRecordsSent;
        }

        @Override
        public List<DeviceServiceProvider> getConnections() {
            return Collections.unmodifiableList(mConnections);
        }

        @Override
        public AppAuthState getAuthState() {
            return authState;
        }

        @Override
        public void setAllowedDeviceIds(final DeviceServiceConnection connection, Set<String> allowedIds) {
            deviceFilters.put(connection, allowedIds);

            DeviceStatusListener.Status status = connection.getDeviceStatus();

            if (status == DeviceStatusListener.Status.READY
                    || status == DeviceStatusListener.Status.CONNECTING
                    || (status == DeviceStatusListener.Status.CONNECTED
                    && !connection.isAllowedDevice(allowedIds))) {
                new Handler(Looper.getMainLooper()).post(new Runnable() {
                    @Override
                    public void run() {
                        if (connection.isRecording()) {
                            connection.stopRecording();
                            // will restart recording once the status is set to disconnected.
                        }
                    }
                });
            }
        }
    }

    private static class AsyncBindServices extends AsyncTask<DeviceServiceProvider, Void, Void> {
        private final boolean unbindFirst;

        AsyncBindServices(boolean unbindFirst) {
            this.unbindFirst = unbindFirst;
        }

        @Override
        protected Void doInBackground(DeviceServiceProvider... params) {
            for (DeviceServiceProvider provider : params) {
                if (provider == null) {
                    continue;
                }
                if (unbindFirst) {
                    logger.info("Rebinding {} after disconnect", provider);
                    if (provider.isBound()) {
                        provider.unbind();
                    }
                }
                if (!provider.isBound()) {
                    logger.info("Binding to service: {}", provider);
                    provider.bind();
                } else {
                    logger.info("Already bound: {}", provider);
                }
            }
            return null;
        }
    }
}
