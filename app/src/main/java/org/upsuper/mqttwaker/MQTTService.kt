package org.upsuper.mqttwaker

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.app.admin.DevicePolicyManager
import android.content.BroadcastReceiver
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.net.Uri
import android.os.IBinder
import android.os.PowerManager
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.preference.PreferenceManager
import org.eclipse.paho.client.mqttv3.MqttAsyncClient
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import android.content.SharedPreferences
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.eclipse.paho.client.mqttv3.IMqttActionListener
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.IMqttToken
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttMessage
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.UUID
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import androidx.core.net.toUri

class MQTTService : Service() {
    companion object {
        private const val TAG = "MQTTService"
        private const val NOTIFICATION_ID = 1
        private const val CHANNEL_ID = "mqtt_waker_channel"
        private const val SCREEN_STATE_ON = "ON"
        private const val SCREEN_STATE_OFF = "OFF"
        private val worker = Executors.newSingleThreadScheduledExecutor()
    }

    private var mqttClient: MqttAsyncClient? = null
    @Volatile private var stopping = false
    private var generation = 0
    private var connecting = false
    private var wakeLock: PowerManager.WakeLock? = null
    private lateinit var prefs: SharedPreferences
    private val preferenceListener = SharedPreferences.OnSharedPreferenceChangeListener { _, key ->
        if (key == "prevent_cpu_sleep") updateWakeLock()
        else if (key in setOf("mqtt_server", "mqtt_sub_topic", "mqtt_pub_topic", "mqtt_username",
                "mqtt_password", "mqtt_client_id", "mqtt_ssl_use_custom_cert", "mqtt_ssl_cert_uri")) {
            worker.execute { replaceClient() }
        }
    }
    private var serverUri = ""
    private var subscribeTopic = ""
    private var publishTopic = ""
    private var clientId = ""
    private var username = ""
    private var password = ""
    private var browserUrl = ""
    private var useCustomCert = false
    private var customCertUri: Uri? = null
    private var screenStateReceiver: BroadcastReceiver? = null

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
        startForeground(NOTIFICATION_ID, createNotification("MQTT Waker Service Starting"))
        Log.i(TAG, "Foreground service created")
        prefs = PreferenceManager.getDefaultSharedPreferences(this)
        prefs.registerOnSharedPreferenceChangeListener(preferenceListener)
        updateWakeLock()
        registerScreenStateReceiver()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        Log.d(TAG, "onStartCommand called with action: ${intent?.action}")
        worker.execute { if (mqttClient == null) replaceClient() }
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onDestroy() {
        Log.i(TAG, "Foreground service destroyed")
        stopping = true
        prefs.unregisterOnSharedPreferenceChangeListener(preferenceListener)
        unregisterScreenStateReceiver()
        releaseWakeLock()
        worker.execute { destroyClient() }
        super.onDestroy()
    }

    private fun loadSettings() {
        val prefs = PreferenceManager.getDefaultSharedPreferences(this)
        serverUri = prefs.getString("mqtt_server", "") ?: ""
        subscribeTopic = prefs.getString("mqtt_sub_topic", "") ?: ""
        publishTopic = prefs.getString("mqtt_pub_topic", "") ?: ""
        username = prefs.getString("mqtt_username", "") ?: ""
        password = prefs.getString("mqtt_password", "") ?: ""
        clientId = prefs.getString("mqtt_client_id", "") ?: ""
        browserUrl = prefs.getString("browser_url", "") ?: ""
        useCustomCert = prefs.getBoolean("mqtt_ssl_use_custom_cert", false)
        customCertUri = prefs.getString("mqtt_ssl_cert_uri", null)?.toUri()

        if (clientId.isBlank()) {
            clientId = prefs.getString("mqtt_generated_client_id", null) ?: ("MQTTWaker_" + UUID.randomUUID().toString()).also {
                prefs.edit().putString("mqtt_generated_client_id", it).commit()
            }
        }

        Log.d(
            TAG,
            "Settings loaded - Server: $serverUri, Subscribe Topic: $subscribeTopic, Publish Topic: $publishTopic, Username: ${if (username.isNotBlank()) "set" else "not set"}, SSL: ${if (useCustomCert) "custom cert" else "system certs"}"
        )
    }

    private fun updateWakeLock() {
        if (prefs.getBoolean("prevent_cpu_sleep", false)) {
            if (wakeLock?.isHeld != true) {
                wakeLock = (getSystemService(Context.POWER_SERVICE) as PowerManager)
                    .newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "$packageName:MQTTService")
                    .apply { setReferenceCounted(false); acquire() }
                Log.i(TAG, "Partial wake lock acquired")
            }
        } else releaseWakeLock()
    }

    private fun releaseWakeLock() {
        wakeLock?.let { if (it.isHeld) { it.release(); Log.i(TAG, "Partial wake lock released") } }
        wakeLock = null
    }

    private fun replaceClient() {
        if (stopping) return
        destroyClient()
        loadSettings()
        if (serverUri.isBlank() || subscribeTopic.isBlank()) {
            updateNotification("Error: MQTT server or topic not configured")
            return
        }
        try {
            val client = MqttAsyncClient(serverUri, clientId, MemoryPersistence())
            mqttClient = client
            val current = generation
            Log.i(TAG, "MQTT client initialized: $clientId")
            client.setCallback(object : MqttCallbackExtended {
                override fun connectComplete(reconnect: Boolean, serverURI: String) {
                    worker.execute {
                        if (stopping || current != generation) return@execute
                        connecting = false
                        Log.i(TAG, "MQTT connected (reconnect=$reconnect) to $serverURI")
                        updateNotification("Connected to MQTT broker")
                        try {
                            client.subscribe(subscribeTopic, 0, null, object : IMqttActionListener {
                                override fun onSuccess(token: IMqttToken) { Log.i(TAG, "Subscribed to $subscribeTopic") }
                                override fun onFailure(token: IMqttToken, error: Throwable) { Log.e(TAG, "Subscription failed", error) }
                            })
                        } catch (e: Exception) { Log.e(TAG, "Subscription failed", e) }
                        publishInitialScreenState()
                    }
                }
                override fun connectionLost(cause: Throwable?) {
                    Log.e(TAG, "MQTT connection lost; automatic reconnect will retry", cause)
                }
                override fun messageArrived(topic: String, message: MqttMessage) {
                    val payload = String(message.payload)
                    worker.execute {
                        if (stopping || current != generation) return@execute
                        Log.d(TAG, "Message received on $topic: $payload")
                        when (payload.trim().uppercase()) {
                            "ON" -> wakeScreen()
                            "OFF" -> lockScreen()
                        }
                    }
                }
                override fun deliveryComplete(token: IMqttDeliveryToken) = Unit
            })
            val options = MqttConnectOptions().apply {
                isAutomaticReconnect = true
                // A fresh broker session prevents subscriptions from an old topic or
                // configuration surviving replacement of this client.
                isCleanSession = true
                keepAliveInterval = 60
                if (username.isNotBlank()) {
                    userName = username
                    password = this@MQTTService.password.toCharArray()
                }
                if (serverUri.startsWith("ssl://") && useCustomCert) {
                    val uri = customCertUri ?: throw IllegalArgumentException("Custom certificate not selected")
                    socketFactory = createCustomSSLContext(uri).socketFactory
                }
            }
            connect(client, options, current)
        } catch (e: Exception) {
            Log.e(TAG, "MQTT initialization failed", e)
            updateNotification("MQTT initialization failed")
            destroyClient()
        }
    }

    private fun connect(client: MqttAsyncClient, options: MqttConnectOptions, current: Int) {
        if (stopping || current != generation || connecting || client.isConnected) return
        connecting = true
        Log.i(TAG, "Connecting MQTT client $clientId")
        try {
            client.connect(options, null, object : IMqttActionListener {
                override fun onSuccess(token: IMqttToken) = Unit
                override fun onFailure(token: IMqttToken, error: Throwable) {
                    Log.e(TAG, "MQTT connection failed; retrying", error)
                    try {
                        worker.schedule({
                            if (!stopping && current == generation) {
                                connecting = false
                                connect(client, options, current)
                            }
                        }, 5, TimeUnit.SECONDS)
                    } catch (_: java.util.concurrent.RejectedExecutionException) { }
                }
            })
        } catch (e: Exception) {
            connecting = false
            Log.e(TAG, "MQTT connection attempt failed", e)
            worker.schedule({ connect(client, options, current) }, 5, TimeUnit.SECONDS)
        }
    }

    private fun destroyClient() {
        generation++
        connecting = false
        val client = mqttClient ?: return
        mqttClient = null
        try {
            client.setCallback(null)
            if (client.isConnected) client.disconnectForcibly(1000, 1000)
            else client.disconnectForcibly(0, 0, false)
        } catch (e: Exception) { Log.w(TAG, "Error disconnecting MQTT client", e) }
        try { client.close(true) } catch (e: Exception) { Log.w(TAG, "Error closing MQTT client", e) }
        Log.i(TAG, "MQTT client destroyed")
    }

    private fun wakeScreen() {
        Log.d(TAG, "Wake screen command received")

        if ((getSystemService(Context.POWER_SERVICE) as PowerManager).isInteractive) {
            Log.d(TAG, "Ignoring ON command: device already interactive")
            return
        }

        try {
            // Show a notification that we're waking the screen
            updateNotification("Waking screen")

            // Start the ScreenWakeService which will handle drawing over other apps
            // and then launching the WakeActivity
            val intent = Intent(this, ScreenWakeService::class.java)
            startService(intent)
            Log.d(TAG, "ScreenWakeService started")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to wake screen", e)
            updateNotification("Failed to wake screen: ${e.message}")
        }
    }

    private fun lockScreen() {
        Log.d(TAG, "Lock screen command received")

        val devicePolicyManager = getSystemService(Context.DEVICE_POLICY_SERVICE) as DevicePolicyManager
        val adminComponentName = ComponentName(this, MQTTWakerDeviceAdminReceiver::class.java)

        if (devicePolicyManager.isAdminActive(adminComponentName)) {
            // We have admin privileges, we can lock the device
            try {
                devicePolicyManager.lockNow()
                Log.d(TAG, "Device locked")
            } catch (e: Exception) {
                Log.e(TAG, "Failed to lock screen", e)
                updateNotification("Failed to lock screen: ${e.message}")
            }
        } else {
            // We don't have admin privileges, show a notification
            Log.d(TAG, "No device admin permission, going to settings")
            updateNotification("Device admin permission needed for screen locking")

            // Create intent to go to admin settings
            val intent = Intent(DevicePolicyManager.ACTION_ADD_DEVICE_ADMIN)
            intent.putExtra(DevicePolicyManager.EXTRA_DEVICE_ADMIN, adminComponentName)
            intent.putExtra(DevicePolicyManager.EXTRA_ADD_EXPLANATION,
                "MQTTWaker needs device admin permissions to lock your screen when requested via MQTT.")
            intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            startActivity(intent)
        }
    }

    private fun createNotificationChannel() {
        val channel = NotificationChannel(
            CHANNEL_ID,
            "MQTT Waker Service",
            NotificationManager.IMPORTANCE_LOW
        ).apply {
            description = "MQTT Waker Service notifications"
        }

        val notificationManager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        notificationManager.createNotificationChannel(channel)
    }

    private fun createNotification(message: String): Notification {
        val pendingIntent = PendingIntent.getActivity(
            this,
            0,
            Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE
        )

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("MQTT Waker")
            .setContentText(message)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentIntent(pendingIntent)
            .build()
    }

    private fun updateNotification(message: String) {
        val notificationManager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        notificationManager.notify(NOTIFICATION_ID, createNotification(message))
    }
    
    private fun registerScreenStateReceiver() {
        if (screenStateReceiver != null) {
            return
        }
        
        Log.d(TAG, "Registering screen state receiver")
        screenStateReceiver = object : BroadcastReceiver() {
            override fun onReceive(context: Context, intent: Intent) {
                val action = intent.action
                Log.d(TAG, "Received screen state broadcast: $action")
                
                when (action) {
                    Intent.ACTION_SCREEN_ON -> worker.execute { publishScreenState(SCREEN_STATE_ON) }
                    Intent.ACTION_SCREEN_OFF -> worker.execute { publishScreenState(SCREEN_STATE_OFF) }
                }
            }
        }
        
        val filter = IntentFilter().apply {
            addAction(Intent.ACTION_SCREEN_ON)
            addAction(Intent.ACTION_SCREEN_OFF)
        }
        registerReceiver(screenStateReceiver, filter)
        Log.d(TAG, "Screen state receiver registered")
    }
    
    private fun unregisterScreenStateReceiver() {
        worker.execute { publishScreenState("") }
        Log.d(TAG, "Unregistering screen state receiver")
        screenStateReceiver?.let {
            try {
                unregisterReceiver(it)
                Log.d(TAG, "Screen state receiver unregistered")
            } catch (e: Exception) {
                Log.e(TAG, "Error unregistering screen state receiver", e)
            }
        }
        screenStateReceiver = null
    }
    
    private fun publishInitialScreenState() {
        val powerManager = getSystemService(Context.POWER_SERVICE) as PowerManager
        val isScreenOn = powerManager.isInteractive
        val state = if (isScreenOn) SCREEN_STATE_ON else SCREEN_STATE_OFF
        
        publishScreenState(state)
        Log.d(TAG, "Published initial screen state: $state")
    }
    
    private fun publishScreenState(state: String) {
        val client = mqttClient
        if (client?.isConnected != true || publishTopic.isBlank()) {
            Log.d(TAG, "Cannot publish screen state: not connected or topic not set")
            return
        }
        
        try {
            val message = MqttMessage(state.toByteArray())
            message.qos = 1
            message.isRetained = true

            client.publish(publishTopic, message)
            Log.d(TAG, "Published screen state '$state' to topic '$publishTopic'")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to publish screen state", e)
        }
    }

    private fun createCustomSSLContext(certUri: Uri): SSLContext {
        // Check if we have permission to access this URI
        val hasPermission = contentResolver.persistedUriPermissions.any {
            it.uri == certUri && it.isReadPermission
        }

        if (!hasPermission) {
            throw SecurityException("No permission to access certificate file")
        }

        val certificateFactory = CertificateFactory.getInstance("X.509")
        val inputStream = contentResolver.openInputStream(certUri)
            ?: throw IllegalArgumentException("Cannot read certificate file")

        val certificate = inputStream.use { stream ->
            certificateFactory.generateCertificate(stream) as X509Certificate
        }

        // Create a KeyStore containing our certificate
        val keyStore = KeyStore.getInstance(KeyStore.getDefaultType())
        keyStore.load(null, null)
        keyStore.setCertificateEntry("ca", certificate)

        // Create a TrustManager that trusts the certificate in our KeyStore
        val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        trustManagerFactory.init(keyStore)

        // Create an SSLContext that uses our TrustManager
        val sslContext = SSLContext.getInstance("TLS")
        sslContext.init(null, trustManagerFactory.trustManagers, null)

        Log.d(TAG, "Custom SSL context created with certificate: ${certificate.subjectX500Principal}")
        return sslContext
    }
}
