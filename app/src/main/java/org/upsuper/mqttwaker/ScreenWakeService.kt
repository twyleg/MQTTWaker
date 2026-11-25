package org.upsuper.mqttwaker

import android.app.Service
import android.content.Context
import android.content.Intent
import android.graphics.PixelFormat
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.PowerManager
import android.provider.Settings
import android.util.Log
import android.view.Gravity
import android.view.WindowManager
import android.view.WindowManager.LayoutParams
import android.widget.FrameLayout

class ScreenWakeService : Service() {
    companion object {
        private const val TAG = "ScreenWakeService"
    }
    private lateinit var windowManager: WindowManager
    private var overlayView: FrameLayout? = null
    private val handler = Handler(Looper.getMainLooper())
    private val cleanup = Runnable {
        removeOverlay()
        stopSelf()
    }

    override fun onCreate() {
        super.onCreate()
        windowManager = getSystemService(Context.WINDOW_SERVICE) as WindowManager
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        Log.d(TAG, "Screen wake service started")

        if (overlayView != null) {
            Log.d(TAG, "Ignoring wake command: overlay already active")
            return START_NOT_STICKY
        }
        if ((getSystemService(Context.POWER_SERVICE) as PowerManager).isInteractive) {
            Log.d(TAG, "Ignoring wake command: device already interactive")
            stopSelf(startId)
            return START_NOT_STICKY
        }
        if (!Settings.canDrawOverlays(this)) {
            Log.e(TAG, "No permission to draw overlays")
            stopSelf(startId)
            return START_NOT_STICKY
        }

        try {
            val view = FrameLayout(this)
            val params = LayoutParams(
                LayoutParams.MATCH_PARENT,
                LayoutParams.MATCH_PARENT,
                LayoutParams.TYPE_APPLICATION_OVERLAY,
                LayoutParams.FLAG_KEEP_SCREEN_ON or
                        LayoutParams.FLAG_LAYOUT_IN_SCREEN or
                        LayoutParams.FLAG_NOT_FOCUSABLE or
                        LayoutParams.FLAG_NOT_TOUCHABLE,
                PixelFormat.TRANSLUCENT
            ).apply {
                gravity = Gravity.CENTER
                // Android 12+ allows touch-through for fully transparent overlay windows.
                alpha = 0f
            }

            windowManager.addView(view, params)
            overlayView = view
            Log.d(TAG, "Wake overlay created")

            val wakeIntent = Intent(this, WakeActivity::class.java).apply {
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP)
            }
            startActivity(wakeIntent)

            handler.removeCallbacks(cleanup)
            handler.postDelayed(cleanup, 3000)
        } catch (e: Exception) {
            Log.e(TAG, "Error showing wake overlay", e)
            handler.removeCallbacks(cleanup)
            removeOverlay()
            stopSelf(startId)
        }

        return START_NOT_STICKY
    }

    private fun removeOverlay() {
        val view = overlayView ?: return
        try {
            windowManager.removeViewImmediate(view)
            Log.d(TAG, "Wake overlay removed")
            overlayView = null
        } catch (e: Exception) {
            Log.e(TAG, "Failed to remove wake overlay", e)
        }
    }

    override fun onDestroy() {
        handler.removeCallbacks(cleanup)
        removeOverlay()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null
}
