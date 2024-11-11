package org.radarbase.android.util

import android.Manifest.permission.*
import android.annotation.SuppressLint
import android.app.Activity
import android.app.AlertDialog
import android.app.AppOpsManager
import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Context.LOCATION_SERVICE
import android.content.Intent
import android.content.pm.PackageManager.PERMISSION_DENIED
import android.content.pm.PackageManager.PERMISSION_GRANTED
import android.location.LocationManager
import android.net.Uri
import android.os.Bundle
import android.os.PowerManager
import android.os.Process
import android.provider.Settings
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import androidx.localbroadcastmanager.content.LocalBroadcastManager
import org.radarbase.android.R
import org.radarbase.android.RadarService
import org.radarbase.android.RadarService.Companion.ACCESS_BACKGROUND_LOCATION_COMPAT
import org.slf4j.LoggerFactory
import kotlin.time.Duration


open class PermissionHandler(
    private val activity: AppCompatActivity,
    private val mHandler: CoroutineTaskExecutor,
    private val requestPermissionTimeoutMs: Long,
) {
    private val broadcaster = LocalBroadcastManager.getInstance(activity)

    private val needsPermissions: MutableSet<String> = HashSet()
    private val isRequestingPermissions: MutableSet<String> = HashSet()
    private var isRequestingPermissionsTime = java.lang.Long.MAX_VALUE
    private var requestFuture: CoroutineTaskExecutor.CoroutineFutureHandle? = null

    private fun onPermissionRequestResult(permission: String, granted: Boolean) {
        mHandler.execute {
            needsPermissions.remove(permission)

            val result = if (granted) PERMISSION_GRANTED else PERMISSION_DENIED
            broadcaster.send(RadarService.ACTION_PERMISSIONS_GRANTED) {
                putExtra(RadarService.EXTRA_PERMISSIONS, arrayOf(permission))
                putExtra(RadarService.EXTRA_GRANT_RESULTS, intArrayOf(result))
            }

            isRequestingPermissions.remove(permission)
            requestPermissions()
        }
    }

    private fun requestPermissions() {
        mHandler.executeReentrant {
            requestFuture?.cancel()
            requestFuture = mHandler.delay(500L) {
                doRequestPermission()
                requestFuture = null
            }
        }
    }

    private fun doRequestPermission() {
        val externallyGrantedPermissions = needsPermissions.filterTo(HashSet()) { activity.isPermissionGranted(it) }

        if (externallyGrantedPermissions.isNotEmpty()) {
            broadcaster.send(RadarService.ACTION_PERMISSIONS_GRANTED) {
                putExtra(RadarService.EXTRA_PERMISSIONS,
                    externallyGrantedPermissions.toTypedArray())
                putExtra(RadarService.EXTRA_GRANT_RESULTS,
                    IntArray(externallyGrantedPermissions.size) { PERMISSION_GRANTED })
            }
            isRequestingPermissions -= externallyGrantedPermissions
            needsPermissions -= externallyGrantedPermissions
        }

        val currentlyNeeded = buildSet(needsPermissions.size) {
            addAll(needsPermissions)
            removeAll(isRequestingPermissions)
            if (contains(ACCESS_COARSE_LOCATION) || contains(ACCESS_FINE_LOCATION)) {
                remove(RadarService.ACCESS_BACKGROUND_LOCATION_COMPAT)
            }
        }

        when {
            currentlyNeeded.isEmpty() -> logger.info("Already requested all permissions.")
            ACCESS_COARSE_LOCATION in currentlyNeeded || ACCESS_FINE_LOCATION in currentlyNeeded -> {
                val locationPermissions = buildSet(2) {
                    if (ACCESS_COARSE_LOCATION in currentlyNeeded) {
                        add(ACCESS_COARSE_LOCATION)
                    }
                    if (ACCESS_FINE_LOCATION in currentlyNeeded) {
                        add(ACCESS_FINE_LOCATION)
                    }
                }
                addRequestingPermissions(locationPermissions)
                requestLocationPermissions(locationPermissions)
            }
            ACCESS_BACKGROUND_LOCATION_COMPAT in currentlyNeeded -> {
                addRequestingPermissions(ACCESS_BACKGROUND_LOCATION_COMPAT)
                requestBackgroundLocationPermissions()
            }
            LOCATION_SERVICE in currentlyNeeded -> {
                addRequestingPermissions(LOCATION_SERVICE)
                requestLocationProvider()
            }
            SYSTEM_ALERT_WINDOW in currentlyNeeded -> {
                addRequestingPermissions(SYSTEM_ALERT_WINDOW)
                requestSystemWindowPermissions()
            }
            PACKAGE_USAGE_STATS in currentlyNeeded -> {
                addRequestingPermissions(PACKAGE_USAGE_STATS)
                requestPackageUsageStats()
            }
            REQUEST_IGNORE_BATTERY_OPTIMIZATIONS in currentlyNeeded -> {
                addRequestingPermissions(REQUEST_IGNORE_BATTERY_OPTIMIZATIONS)
                requestDisableBatteryOptimization()
            }
            else -> {
                addRequestingPermissions(currentlyNeeded)
                requestPermissions(currentlyNeeded)
            }
        }
    }

    private fun requestBackgroundLocationPermissions() {
        requestPermissions(setOf(ACCESS_BACKGROUND_LOCATION_COMPAT))
    }

    private fun requestLocationPermissions(locationPermissions: Set<String>) {
        alertDialog {
            setView(R.layout.location_dialog)
            setPositiveButton(android.R.string.ok) { dialog, _ ->
                dialog.dismiss()
                requestPermissions(locationPermissions)
            }
            setNegativeButton(android.R.string.cancel) { dialog, _ ->
                dialog.cancel()
                requestPermissions()
            }
        }
    }

    private fun requestPermissions(permissions: Set<String>) {
        try {
            ActivityCompat.requestPermissions(
                activity,
                permissions.toTypedArray(),
                REQUEST_ENABLE_PERMISSIONS,
            )
        } catch (ex: IllegalStateException) {
            logger.warn("Cannot request permission on closing activity")
        }
    }

    private fun resetRequestingPermission() {
        isRequestingPermissions.clear()
        isRequestingPermissionsTime = java.lang.Long.MAX_VALUE
    }

    private fun addRequestingPermissions(permission: String) {
        addRequestingPermissions(setOf(permission))
    }

    private fun addRequestingPermissions(permissions: Set<String>) {
        isRequestingPermissions.addAll(permissions)

        if (isRequestingPermissionsTime != java.lang.Long.MAX_VALUE) {
            isRequestingPermissionsTime = System.currentTimeMillis()
            mHandler.delay(requestPermissionTimeoutMs) {
                resetRequestingPermission()
                requestPermissions()
            }
        }
    }

    private fun alertDialog(configure: AlertDialog.Builder.() -> Unit) {
        try {
            activity.runOnUiThread {
                AlertDialog.Builder(activity, android.R.style.Theme_Material_Dialog_Alert)
                    .apply(configure)
                    .show()
            }
        } catch (ex: IllegalStateException) {
            logger.warn("Cannot show dialog on closing activity")
        }
    }

    private fun requestLocationProvider() {
        alertDialog {
            setView(R.layout.location_dialog)
            setPositiveButton(android.R.string.ok) { dialog, _ ->
                dialog.dismiss()
                Intent(Settings.ACTION_LOCATION_SOURCE_SETTINGS)
                    .startActivityForResult(LOCATION_REQUEST_CODE)
            }
            setNegativeButton(android.R.string.cancel) { dialog, _ ->
                dialog.cancel()
                requestPermissions()
            }
        }
    }

    private fun requestSystemWindowPermissions() {
        // Show alert dialog to the user saying a separate permission is needed
        // Launch the settings activity if the user prefers
        alertDialog {
            setView(R.layout.system_window_dialog)
            setPositiveButton(android.R.string.ok) { dialog, _ ->
                dialog.dismiss()
                Intent(
                    Settings.ACTION_MANAGE_OVERLAY_PERMISSION,
                    Uri.parse("package:" + activity.packageName)
                ).startActivityForResult(ACTION_MANAGE_OVERLAY_PERMISSION_REQUEST_CODE)
            }
            setNegativeButton(android.R.string.cancel) { dialog, _ ->
                dialog.cancel()
                requestPermissions()
            }
        }
    }

    private fun Intent.startActivityForResult(code: Int) {
        resolveActivity(activity.packageManager) ?: return
        try {
            activity.startActivityForResult(this, code)
        } catch (ex: ActivityNotFoundException) {
            logger.error("Failed to ask for usage code", ex)
        } catch (ex: IllegalStateException) {
            logger.warn("Cannot start activity on closed app")
        }
    }

    @SuppressLint("BatteryLife")
    private fun requestDisableBatteryOptimization() {
        alertDialog {
            setView(R.layout.disable_battery_optimization_dialog)
            setPositiveButton(android.R.string.ok) { dialog, _ ->
                dialog.dismiss()
                Intent(
                    Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS,
                    Uri.parse("package:" + activity.packageName)
                ).startActivityForResult(BATTERY_OPT_CODE)
            }
            setNegativeButton(android.R.string.cancel) { dialog, _ ->
                dialog.cancel()
                requestPermissions()
            }
        }
    }

    private fun requestPackageUsageStats() {
        alertDialog {
            setView(R.layout.usage_tracking_dialog)
            setPositiveButton(android.R.string.ok) { dialog, _ ->
                dialog.dismiss()
                var intent = Intent(Settings.ACTION_USAGE_ACCESS_SETTINGS)
                if (intent.resolveActivity(activity.packageManager) == null) {
                    intent = Intent(Settings.ACTION_SETTINGS)
                }
                intent.startActivityForResult(USAGE_REQUEST_CODE)
            }
            setNegativeButton(android.R.string.cancel) { dialog, _ ->
                dialog.cancel()
                requestPermissions()
            }
        }
    }

    fun onActivityResult(requestCode: Int, resultCode: Int) {
        when (requestCode) {
            LOCATION_REQUEST_CODE -> onPermissionRequestResult(
                LOCATION_SERVICE,
                resultCode == Activity.RESULT_OK
            )
            USAGE_REQUEST_CODE -> onPermissionRequestResult(
                PACKAGE_USAGE_STATS,
                resultCode == Activity.RESULT_OK
            )
            BATTERY_OPT_CODE -> {
                val powerManager =
                    activity.getSystemService(Context.POWER_SERVICE) as PowerManager?
                val granted = resultCode == Activity.RESULT_OK
                        || powerManager?.isIgnoringBatteryOptimizations(activity.applicationContext.packageName) != false
                onPermissionRequestResult(
                    REQUEST_IGNORE_BATTERY_OPTIMIZATIONS,
                    granted
                )
            }
            ACTION_MANAGE_OVERLAY_PERMISSION_REQUEST_CODE -> onPermissionRequestResult(
                SYSTEM_ALERT_WINDOW,
                resultCode == Activity.RESULT_OK
            )
        }
    }

    fun invalidateCache() {
        mHandler.execute {
            if (isRequestingPermissions.isNotEmpty()) {
                val timeToExpire = isRequestingPermissionsTime + requestPermissionTimeoutMs - System.currentTimeMillis()
                if (timeToExpire <= 0L) {
                    resetRequestingPermission()
                } else {
                    mHandler.delay(timeToExpire, ::resetRequestingPermission)
                }
            }
        }
    }


    fun replaceNeededPermissions(newPermissions: Array<out String>?) {
        newPermissions ?: return
        mHandler.execute {
            needsPermissions.clear()

            needsPermissions += buildList(newPermissions.size + 1) {
                addAll(newPermissions)
                if (contains(ACCESS_FINE_LOCATION) || contains(ACCESS_COARSE_LOCATION)) {
                    add(LOCATION_SERVICE)
                }
            }.filter { it.isNotEmpty() && !activity.isPermissionGranted(it) }

            requestPermissions()
        }
    }

    fun permissionsGranted(requestCode: Int, permissions: Array<String>, grantResults: IntArray) {
        if (requestCode == REQUEST_ENABLE_PERMISSIONS) {
            broadcaster.send(RadarService.ACTION_PERMISSIONS_GRANTED) {
                putExtra(RadarService.EXTRA_PERMISSIONS, permissions)
                putExtra(RadarService.EXTRA_GRANT_RESULTS, grantResults)
            }
        }
    }

    fun saveInstanceState(savedInstanceState: Bundle) {
        savedInstanceState.putStringArrayList(
            "isRequestingPermissions", ArrayList(
                isRequestingPermissions
            )
        )
        savedInstanceState.putLong("isRequestingPermissionsTime", isRequestingPermissionsTime)
    }

    fun restoreInstanceState(savedInstanceState: Bundle) {
        savedInstanceState.getStringArrayList("isRequestingPermissions")
            ?.let { isRequestingPermissions += it }

        isRequestingPermissionsTime = savedInstanceState.getLong(
            "isRequestingPermissionsTime",
            java.lang.Long.MAX_VALUE
        )
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PermissionHandler::class.java)

        private const val REQUEST_ENABLE_PERMISSIONS = 2
        // can only use lower 16 bits for request code
        private const val LOCATION_REQUEST_CODE = 232619694 and 0xFFFF
        private const val USAGE_REQUEST_CODE = 232619695 and 0xFFFF
        private const val BATTERY_OPT_CODE = 232619696 and 0xFFFF
        private const val ACTION_MANAGE_OVERLAY_PERMISSION_REQUEST_CODE = 232619697 and 0xFFFF

        fun Context.isPermissionGranted(permission: String): Boolean = when (permission) {
            LOCATION_SERVICE -> applySystemService<LocationManager>(LOCATION_SERVICE) { locationManager ->
                locationManager.isProviderEnabled(LocationManager.GPS_PROVIDER)
                        || locationManager.isProviderEnabled(LocationManager.NETWORK_PROVIDER)
            } ?: true
            REQUEST_IGNORE_BATTERY_OPTIMIZATIONS -> applySystemService<PowerManager>(Context.POWER_SERVICE) { powerManager ->
                powerManager.isIgnoringBatteryOptimizations(applicationContext.packageName)
            } ?: true
            PACKAGE_USAGE_STATS -> applySystemService<AppOpsManager>(Context.APP_OPS_SERVICE) { appOps ->
                @Suppress("DEPRECATION")
                (AppOpsManager.MODE_ALLOWED == appOps.checkOpNoThrow("android:get_usage_stats", Process.myUid(), packageName))
            } ?: true
            SYSTEM_ALERT_WINDOW -> Settings.canDrawOverlays(this)
            else -> PERMISSION_GRANTED == ContextCompat.checkSelfPermission(this, permission)
        }
    }
}
