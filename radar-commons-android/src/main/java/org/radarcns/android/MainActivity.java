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

import android.Manifest;
import android.annotation.SuppressLint;
import android.app.AlertDialog;
import android.app.Service;
import android.arch.lifecycle.Lifecycle;
import android.bluetooth.BluetoothAdapter;
import android.content.ActivityNotFoundException;
import android.content.BroadcastReceiver;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.ServiceConnection;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.PowerManager;
import android.os.Process;
import android.provider.Settings;
import android.support.annotation.CallSuper;
import android.support.annotation.NonNull;
import android.support.annotation.RequiresApi;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.LocalBroadcastManager;
import android.support.v7.app.AppCompatActivity;

import com.crashlytics.android.Crashlytics;

import org.radarcns.android.auth.AppAuthState;
import org.radarcns.android.util.NetworkConnectedReceiver;
import org.radarcns.android.util.SafeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static android.Manifest.permission.REQUEST_IGNORE_BATTERY_OPTIMIZATIONS;
import static org.radarcns.android.RadarService.ACTION_BLUETOOTH_NEEDED_CHANGED;
import static org.radarcns.android.auth.LoginActivity.ACTION_LOGIN;
import static org.radarcns.android.auth.portal.ManagementPortalClient.MP_REFRESH_TOKEN_PROPERTY;

/** Base MainActivity class. It manages the services to collect the data and starts up a view. To
 * create an application, extend this class and override the abstract methods. */
@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class MainActivity extends AppCompatActivity implements NetworkConnectedReceiver.NetworkConnectedListener {
    private static final Logger logger = LoggerFactory.getLogger(MainActivity.class);

    private static final int REQUEST_ENABLE_PERMISSIONS = 2;
    private static final int LOGIN_REQUEST_CODE = 232619693;
    private static final int LOCATION_REQUEST_CODE = 232619694;
    private static final int USAGE_REQUEST_CODE = 232619695;
    private static final int BATTERY_OPT_CODE = 232619696;
    private static final long REQUEST_PERMISSION_TIMEOUT_MS = 86_400_000L; // 1 day

    private BroadcastReceiver configurationBroadcastReceiver;

    /** Time between refreshes. */
    private long uiRefreshRate;

    /**
     * Background handler thread, to do the service orchestration. Having this in the background
     * is important to avoid any lags in the UI. It is shutdown whenever the activity is not
     * running.
     */
    private HandlerThread mHandlerThread;
    /** Hander in the background. It is set to null whenever the activity is not running. */
    private SafeHandler mHandler;

    /** The UI to show the service data. */
    private MainActivityView mView;

    private AppAuthState authState;

    private Set<String> needsPermissions = Collections.emptySet();
    private final Set<String> isRequestingPermissions = new HashSet<>();
    private long isRequestingPermissionsTime = Long.MAX_VALUE;

    private boolean requestedBt;

    private IRadarBinder radarService;
    private boolean radarServiceIsStarted;

    /** Defines callbacks for service binding, passed to bindService() */
    private final BroadcastReceiver bluetoothReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            if (Objects.equals(intent.getAction(), BluetoothAdapter.ACTION_STATE_CHANGED)) {
                final int state = intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR);
                logger.debug("Bluetooth state {}", state);
                // Upon state change, restart ui handler and restart Scanning.
                if (state == BluetoothAdapter.STATE_OFF) {
                    requestEnableBt();
                }
            }
        }
    };

    private final BroadcastReceiver bluetoothNeededReceiverImpl = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            if (Objects.equals(intent.getAction(), ACTION_BLUETOOTH_NEEDED_CHANGED)) {
                testBindBluetooth();
            }
        }
    };
    private BroadcastReceiver bluetoothNeededReceiver;

    private volatile boolean bluetoothReceiverIsEnabled;
    protected RadarConfiguration configuration;

    /**
     * Sends an intent to request bluetooth to be turned on.
     */
    protected void requestEnableBt() {
        BluetoothAdapter btAdaptor = BluetoothAdapter.getDefaultAdapter();
        if (btAdaptor.isEnabled()) {
            return;
        }

        Intent btIntent = new Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE);
        btIntent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        getApplicationContext().startActivity(btIntent);
    }

    private NetworkConnectedReceiver networkReceiver;

    private final ServiceConnection radarServiceConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            synchronized (MainActivity.this) {
                radarService = (IRadarBinder) service;
                mView = createView();
            }
            testBindBluetooth();
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
            synchronized (MainActivity.this) {
                radarService = null;
            }
        }
    };

    private void testBindBluetooth() {
        boolean needsBluetooth = radarService != null && radarService.needsBluetooth();

        if (needsBluetooth == bluetoothReceiverIsEnabled) {
            return;
        }

        if (needsBluetooth) {
            registerReceiver(bluetoothReceiver,
                    new IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED));
            bluetoothReceiverIsEnabled = true;
            requestEnableBt();
        } else {
            bluetoothReceiverIsEnabled = false;
            unregisterReceiver(bluetoothReceiver);
        }
    }

    @Override
    @CallSuper
    protected void onSaveInstanceState(Bundle savedInstanceState) {
        super.onSaveInstanceState(savedInstanceState);

        savedInstanceState.putStringArrayList("isRequestingPermissions", new ArrayList<>(isRequestingPermissions));
        savedInstanceState.putLong("isRequestingPermissionsTime", isRequestingPermissionsTime);
    }

    @Override
    protected final void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        mHandler = new SafeHandler("Service connection", Process.THREAD_PRIORITY_BACKGROUND);
        bluetoothReceiverIsEnabled = false;
        if (savedInstanceState != null) {
            List<String> isRequesting = savedInstanceState.getStringArrayList("isRequestingPermissions");
            if (isRequesting != null) {
                isRequestingPermissions.addAll(isRequesting);
            }
            isRequestingPermissionsTime = savedInstanceState.getLong("isRequestingPermissionsTime", Long.MAX_VALUE);
        }

        configuration = ((RadarApplication)getApplication()).getConfiguration();

        if (getIntent() == null || getIntent().getExtras() == null) {
            authState = AppAuthState.Builder.from(this).build();
        } else {
            Bundle extras = getIntent().getExtras();
            extras.setClassLoader(MainActivity.class.getClassLoader());
            authState = AppAuthState.Builder.from(extras).build();
        }

        if (!authState.isValid()) {
            startLogin(false);
            return;
        }

        radarServiceIsStarted = false;

        networkReceiver = new NetworkConnectedReceiver(this, this);
        create();
    }

    @CallSuper
    protected void create() {
        logger.info("RADAR configuration at create: {}", configuration);
        onConfigChanged();

        configurationBroadcastReceiver = new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                onConfigChanged();
            }
        };
        LocalBroadcastManager.getInstance(this)
                .registerReceiver(configurationBroadcastReceiver,
                        new IntentFilter(RadarConfiguration.RADAR_CONFIGURATION_CHANGED));

        networkReceiver.register();

        // Start the UI thread
        uiRefreshRate = configuration.getLong(RadarConfiguration.UI_REFRESH_RATE_KEY);
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();

        if (configurationBroadcastReceiver != null) {
            LocalBroadcastManager.getInstance(this)
                    .unregisterReceiver(configurationBroadcastReceiver);
        }
        if (networkReceiver != null) {
            networkReceiver.unregister();
        }
    }

    /**
     * Called whenever the RadarConfiguration is changed. This can be at activity start or
     * when the configuration is updated from Firebase.
     */
    @CallSuper
    protected void onConfigChanged() {

    }

    /** Create a view to show the data of this activity. */
    protected abstract MainActivityView createView();

    @Override
    protected void onResume() {
        super.onResume();
        mHandler.postDelayed(() -> {
            if (getLifecycle().getCurrentState() != Lifecycle.State.RESUMED) {
                return false;
            }
            try {
                // Update all rows in the UI with the data from the connections
                MainActivityView localView = getView();
                if (localView != null) {
                    localView.update();
                }
                return true;
            } catch (Exception ex) {
                logger.error("Failed to update view");
                return true;
            }
        }, uiRefreshRate);
    }

    public final synchronized MainActivityView getView() {
        return mView;
    }

    @Override
    protected void onStart() {
        super.onStart();
        if (!authState.isValid()) {
            startLogin(true);
        }
        Class<? extends Service> radarServiceCls = ((RadarApplication) getApplication()).getRadarService();
        if (!radarServiceIsStarted) {
            Bundle extras = new Bundle();
            authState.addToBundle(extras);
            try {
                startService(new Intent(this, radarServiceCls).putExtras(extras));
                radarServiceIsStarted = true;
            } catch (IllegalStateException ex) {
                logger.error("Failed to start RadarService: activity is in background.", ex);
            }
        }

        mHandler.start();
        synchronized (this) {
            if (!isRequestingPermissions.isEmpty()) {
                long now = System.currentTimeMillis();
                long expires = isRequestingPermissionsTime + getRequestPermissionTimeoutMs();
                if (expires <= now) {
                    resetRequestingPermission();
                } else {
                    mHandler.postDelayed(this::resetRequestingPermission, expires - now);
                }
            }
        }
        if (radarServiceIsStarted) {
            bindService(new Intent(this, radarServiceCls), radarServiceConnection, 0);
        }
        testBindBluetooth();
        bluetoothNeededReceiver = bluetoothNeededReceiverImpl;
        LocalBroadcastManager.getInstance(this)
                .registerReceiver(bluetoothNeededReceiver,
                        new IntentFilter(ACTION_BLUETOOTH_NEEDED_CHANGED));
    }

    private synchronized void resetRequestingPermission() {
        isRequestingPermissions.clear();
        isRequestingPermissionsTime = Long.MAX_VALUE;
    }

    @Override
    protected void onNewIntent(Intent intent) {
        if (RadarService.ACTION_CHECK_PERMISSIONS.equals(intent.getAction())) {
            String[] permissions = intent.getStringArrayExtra(RadarService.EXTRA_PERMISSIONS);
            needsPermissions = new HashSet<>(Arrays.asList(permissions));
            checkPermissions();
        }

        super.onNewIntent(intent);
    }

    @Override
    public void onNetworkConnectionChanged(boolean isConnected, boolean isWifiOrEthernet) {
        if (isConnected && !authState.isValid()) {
            startLogin(false);
        }
    }

    @Override
    protected void onStop() {
        super.onStop();

        if (radarServiceIsStarted) {
            unbindService(radarServiceConnection);
        }

        synchronized (this) {
            mHandler = null;
        }
        mHandlerThread.quitSafely();
        mView = null;
        if (bluetoothNeededReceiver != null) {
            LocalBroadcastManager.getInstance(this).unregisterReceiver(bluetoothNeededReceiver);
            bluetoothNeededReceiver = null;
        }
        if (bluetoothReceiverIsEnabled) {
            bluetoothReceiverIsEnabled = false;
            unregisterReceiver(bluetoothReceiver);
        }
    }

    @Override
    public void onActivityResult(int requestCode, int resultCode, Intent result) {
        switch (requestCode) {
            case LOGIN_REQUEST_CODE: {
                if (resultCode != RESULT_OK) {
                    logger.error("Login should not be cancellable. Opening login again");
                    startLogin(true);
                }
                if (result != null && result.getExtras() != null) {
                    Bundle extras = result.getExtras();
                    extras.setClassLoader(MainActivity.class.getClassLoader());
                    authState = AppAuthState.Builder.from(extras).build();
                } else {
                    authState = AppAuthState.Builder.from(this).build();
                }
                onConfigChanged();
                break;
            }
            case LOCATION_REQUEST_CODE: {
                onPermissionRequestResult(LOCATION_SERVICE, resultCode == RESULT_OK);
                break;
            }
            case USAGE_REQUEST_CODE: {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                    onPermissionRequestResult(
                            Manifest.permission.PACKAGE_USAGE_STATS,
                            resultCode == RESULT_OK);
                }
                break;
            }
            case BATTERY_OPT_CODE: {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                    PowerManager powerManager = (PowerManager) getSystemService(POWER_SERVICE);
                    String packageName = getApplicationContext().getPackageName();
                    boolean granted = resultCode == RESULT_OK
                            || (powerManager != null
                            && powerManager.isIgnoringBatteryOptimizations(packageName));
                    onPermissionRequestResult(REQUEST_IGNORE_BATTERY_OPTIMIZATIONS, granted);
                }
                break;
            }
        }
    }

    private void onPermissionRequestResult(String permission, boolean granted) {
        needsPermissions.remove(permission);
        synchronized (this) {
            isRequestingPermissions.remove(permission);
        }

        int result = granted ? PackageManager.PERMISSION_GRANTED : PackageManager.PERMISSION_DENIED;
        LocalBroadcastManager.getInstance(this)
                .sendBroadcast(new Intent()
                        .setAction(RadarService.ACTION_PERMISSIONS_GRANTED)
                        .putExtra(RadarService.EXTRA_PERMISSIONS, new String[]{LOCATION_SERVICE})
                        .putExtra(RadarService.EXTRA_GRANT_RESULTS, new int[]{result}));

        checkPermissions();
    }

    protected void checkPermissions() {
        if (needsPermissions.isEmpty()) {
            LocalBroadcastManager.getInstance(this)
                    .sendBroadcast(new Intent()
                            .setAction(RadarService.ACTION_PERMISSIONS_GRANTED)
                            .putExtra(RadarService.EXTRA_PERMISSIONS, new String[0])
                            .putExtra(RadarService.EXTRA_GRANT_RESULTS, new int[0]));
            return;
        }

        Set<String> currentlyNeeded = new HashSet<>(needsPermissions);
        synchronized (this) {
            currentlyNeeded.removeAll(isRequestingPermissions);
        }
        if (currentlyNeeded.isEmpty()) {
            return;
        }
        if (currentlyNeeded.contains(LOCATION_SERVICE)) {
            addRequestingPermissions(LOCATION_SERVICE);
            requestLocationProvider();
        } else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M
                && currentlyNeeded.contains(Manifest.permission.PACKAGE_USAGE_STATS)) {
            addRequestingPermissions(Manifest.permission.PACKAGE_USAGE_STATS);
            requestPackageUsageStats();
        } else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M
                && currentlyNeeded.contains(REQUEST_IGNORE_BATTERY_OPTIMIZATIONS)) {
            addRequestingPermissions(REQUEST_IGNORE_BATTERY_OPTIMIZATIONS);
            requestDisableBatteryOptimization();
        } else {
            addRequestingPermissions(currentlyNeeded);
            ActivityCompat.requestPermissions(this,
                    currentlyNeeded.toArray(new String[0]), REQUEST_ENABLE_PERMISSIONS);
        }
    }

    private void addRequestingPermissions(String permission) {
        addRequestingPermissions(Collections.singleton(permission));
    }

    private synchronized void addRequestingPermissions(Set<String> permissions) {
        isRequestingPermissions.addAll(permissions);

        if (mHandler != null && isRequestingPermissionsTime != Long.MAX_VALUE) {
            isRequestingPermissionsTime = System.currentTimeMillis();
            mHandler.postDelayed(() -> {
                resetRequestingPermission();
                checkPermissions();
            }, getRequestPermissionTimeoutMs());
        }
    }

    private void requestLocationProvider() {
        AlertDialog.Builder builder = new AlertDialog.Builder(this, android.R.style.Theme_Material_Dialog_Alert);
        builder.setTitle(R.string.enable_location_title)
                .setMessage(R.string.enable_location)
                .setPositiveButton(android.R.string.ok, (dialog, which) -> {
                    dialog.cancel();
                    Intent intent = new Intent(Settings.ACTION_LOCATION_SOURCE_SETTINGS);
                    if (intent.resolveActivity(getPackageManager()) != null) {
                        startActivityForResult(intent, LOCATION_REQUEST_CODE);
                    }
                })
                .setIcon(android.R.drawable.ic_dialog_alert)
                .show();
    }

    @RequiresApi(api = Build.VERSION_CODES.M)
    @SuppressLint("BatteryLife")
    private void requestDisableBatteryOptimization() {
        Intent intent = new Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS);
        intent.setData(Uri.parse("package:" + getApplicationContext().getPackageName()));
        if (intent.resolveActivity(getPackageManager()) != null) {
            startActivityForResult(intent, BATTERY_OPT_CODE);
        }
    }

    private void requestPackageUsageStats() {
        AlertDialog.Builder builder = new AlertDialog.Builder(this, android.R.style.Theme_Material_Dialog_Alert);
        builder.setTitle(R.string.enable_package_usage_title)
                .setMessage(R.string.enable_package_usage)
                .setPositiveButton(android.R.string.ok, (dialog, which) -> {
                    dialog.cancel();
                    Intent intent = new Intent(Settings.ACTION_USAGE_ACCESS_SETTINGS);
                    if (intent.resolveActivity(getPackageManager()) == null) {
                        intent = new Intent(Settings.ACTION_SETTINGS);
                    }
                    try {
                        startActivityForResult(intent, USAGE_REQUEST_CODE);
                    } catch (ActivityNotFoundException ex) {
                        logger.error("Failed to ask for usage code", ex);
                        Crashlytics.logException(ex);
                    }
                })
                .setIcon(android.R.drawable.ic_dialog_alert)
                .show();
    }

    @Override
    public void onRequestPermissionsResult(final int requestCode, @NonNull final String[] permissions, @NonNull final int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);

        if (requestCode == REQUEST_ENABLE_PERMISSIONS) {
            LocalBroadcastManager.getInstance(this)
                    .sendBroadcast(new Intent()
                            .setAction(RadarService.ACTION_PERMISSIONS_GRANTED)
                            .putExtra(RadarService.EXTRA_PERMISSIONS, permissions)
                            .putExtra(RadarService.EXTRA_GRANT_RESULTS, grantResults));
        }
    }

    protected boolean hasPermission(String permissionName) {
        return !needsPermissions.contains(permissionName);
    }

    /**
     * Log out of the current application.
     * @param force if {@code true}, also remove any refresh tokens; if {@code false}, just remove
     *              the access token but allow the same user to automatically log in again if it is
     *              still valid.
     */
    protected void logout(boolean force) {
        IRadarBinder radarService = getRadarService();
        if (radarService != null) {
            authState = radarService.getAuthState();
        }
        authState.invalidate(this);
        if (force) {
            authState = authState.newBuilder()
                    .removeAttribute(MP_REFRESH_TOKEN_PROPERTY)
                    .build();
            authState.addToPreferences(this);
        }
        startLogin(false);
    }

    /**
     * Start the login activity. If the authentication can be automatically renewed, this will
     * immediately return to the current activity.
     * @param forResult if {@code true}, do not stop the current activity but wait for the result
     *                  of logging in; if {@code false}, halt the current activity.
     */
    protected void startLogin(boolean forResult) {
        Class<?> loginActivity = ((RadarApplication) getApplication()).getLoginActivity();
        Intent intent = new Intent(this, loginActivity);

        if (forResult) {
            intent.setAction(ACTION_LOGIN);
            startActivityForResult(intent, LOGIN_REQUEST_CODE);
        } else {
            startActivity(intent);
            finish();
        }
    }

    protected long getRequestPermissionTimeoutMs() {
        return REQUEST_PERMISSION_TIMEOUT_MS;
    }

    public IRadarBinder getRadarService() {
        return radarService;
    }

    public String getUserId() {
        return configuration.getString(RadarConfiguration.USER_ID_KEY, null);
    }

    public String getProjectId() {
        return configuration.getString(RadarConfiguration.PROJECT_ID_KEY, null);
    }
}
