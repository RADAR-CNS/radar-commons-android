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

package org.radarbase.passive.phone.audio.input

import android.Manifest
import android.content.Context
import android.content.SharedPreferences
import android.content.pm.PackageManager
import android.media.AudioDeviceInfo
import android.media.AudioDeviceInfo.TYPE_BLUETOOTH_A2DP
import android.media.AudioDeviceInfo.TYPE_BLUETOOTH_SCO
import android.media.AudioDeviceInfo.TYPE_USB_DEVICE
import android.media.AudioDeviceInfo.TYPE_USB_HEADSET
import android.media.AudioDeviceInfo.TYPE_WIRED_HEADSET
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.AudioRecord.STATE_INITIALIZED
import android.os.Handler
import android.os.Looper
import android.widget.Toast
import androidx.core.content.ContextCompat
import androidx.lifecycle.Observer
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.Boast
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.passive.phone.audio.input.PhoneAudioInputService.Companion.LAST_RECORDED_AUDIO_FILE
import org.radarbase.passive.phone.audio.input.PhoneAudioInputService.Companion.PHONE_AUDIO_INPUT_SHARED_PREFS
import org.radarbase.passive.phone.audio.input.utils.AudioDeviceUtils
import org.radarbase.passive.phone.audio.input.utils.AudioTypeFormatUtil
import org.radarbase.passive.phone.audio.input.utils.AudioTypeFormatUtil.toLogFriendlyType
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.audio.PhoneAudioInput
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.RandomAccessFile

class PhoneAudioInputManager(service: PhoneAudioInputService) : AbstractSourceManager<PhoneAudioInputService,
        PhoneAudioInputState>(service), PhoneAudioInputState.AudioRecordManager, PhoneAudioInputState.AudioRecordingManager {
    private val audioInputTopic: DataCache<ObservationKey, PhoneAudioInput> = createCache(
        "android_phone_audio_input",
        PhoneAudioInput()
    )

    private var audioRecord: AudioRecord? = null
    private var randomAccessWriter: RandomAccessFile? = null
    private var buffer: ByteArray = byteArrayOf()
    private val audioRecordingHandler = CoroutineTaskExecutor("AudioRecordExecutor")
    private val recordProcessingHandler: CoroutineTaskExecutor = CoroutineTaskExecutor(this::class.simpleName!!)
    private val mainHandler = Handler(Looper.getMainLooper())
    private val preferences: SharedPreferences =
        service.getSharedPreferences(PHONE_AUDIO_INPUT_SHARED_PREFS, Context.MODE_PRIVATE)

    var audioSource: Int
        get() = state.audioSource.get()
        set(value) { state.audioSource.set(value) }
    var sampleRate: Int
        get() = state.sampleRate.get()
        set(value) { state.sampleRate.set(value) }
    var channel: Int
        get() = state.channel.get()
        set(value) { state.channel.set(value) }
    var audioFormat: Int
        get() = state.audioFormat.get()
        set(value) { state.audioFormat.set(value) }
    /**
     * The total number of bytes needed to hold the audio data for one `framePeriod`.
     */
    var bufferSize: Int
        get() = state.bufferSize.get()
        set(value) { state.bufferSize.set(value) }

    /**
     * The framePeriod is calculated as the number of samples in `TIMER_INTERVAL` milliseconds.
     * It represents how many samples correspond to the given interval.
     */
    private var framePeriod: Int
    private var bitsPerSample: Short
    private var numChannels: Short
    private val audioDir: File?
    private var recordingFile: File? = null
    private var payloadSize: Int = 0
    @Volatile
    private var currentlyRecording: Boolean = false

    private val connectedMicrophonesObserver: Observer<List<AudioDeviceInfo>> = Observer { connectedMicrophones ->
        audioRecordingHandler.execute {
            if (connectedMicrophones?.size == 0) {
                logger.warn("No connected microphone")
            }
            if (state.microphonePrioritized && state.finalizedMicrophone.value !in connectedMicrophones) {
                state.microphonePrioritized = false
                logger.info("Microphone prioritized: false")
            }
            logger.info("PhoneAudioInputManager: Connected microphones: {}",
                connectedMicrophones.map { it.productName })

            if (!state.microphonePrioritized) {
                connectedMicrophones.also(::runDeviceSelectionLogic)
            } else {
                state.finalizedMicrophone.value?.let(setPreferredDeviceAndUpdate)
            }
        }
    }

    init {
        name = service.getString(R.string.phone_audio_input_display_name)
        bitsPerSample = if (audioFormat == AudioFormat.ENCODING_PCM_16BIT) 16 else 8
        numChannels = if (channel == AudioFormat.CHANNEL_IN_MONO) 1 else 2
        framePeriod = sampleRate * TIMER_INTERVAL/1000

        val internalDirs = service.filesDir
        status = if (internalDirs != null) {
            audioDir = File(internalDirs, "org.radarbase.passive.phone.audio.input")
            val dirCreated = audioDir.mkdirs()
            val directoryExists = audioDir.exists()
            logger.debug("Directory for saving audio file, created: {}, exists: {}", dirCreated, directoryExists)
            clearAudioDirectory()
            SourceStatusListener.Status.READY
        } else {
            audioDir = null
            SourceStatusListener.Status.UNAVAILABLE
        }
    }

    override fun start(acceptableIds: Set<String>) {
        register()
        audioRecordingHandler.start()
        recordProcessingHandler.start()
        createRecorder()
        state.audioRecordManager = this
        state.audioRecordingManager = this
    }

    private val setPreferredDeviceAndUpdate: (AudioDeviceInfo) -> Unit = { microphone ->
        audioRecord?.preferredDevice = microphone
        state.finalizedMicrophone.postValue(audioRecord?.preferredDevice)
    }


    private fun createRecorder() {
        audioRecordingHandler.execute {
            if (ContextCompat.checkSelfPermission(service, Manifest.permission.RECORD_AUDIO) ==
                PackageManager.PERMISSION_GRANTED
            ) {
                status = SourceStatusListener.Status.CONNECTING
                // If using default values: framePeriod = 16000 * 0.12 = 1920 samples.
                framePeriod = sampleRate * TIMER_INTERVAL / 1000
                // For default values: bufferSize = 1920 * 16 * 1 * 2 / 8 = 7680 bytes.
                bufferSize = framePeriod * bitsPerSample * numChannels * 2 / 8
                logger.info("Calculated buffer size: $bufferSize (bytes), and frame period: $framePeriod")

                val calculatedBufferSize: Int = AudioRecord.getMinBufferSize(sampleRate, channel, audioFormat)
                if (calculatedBufferSize != AudioRecord.ERROR_BAD_VALUE) {
                    if (bufferSize < calculatedBufferSize) {
                        bufferSize = calculatedBufferSize
                        framePeriod = bufferSize / (2 * bitsPerSample * numChannels / 8)
                        logger.info("Updating buffer size to: $bufferSize, and frame period to: $framePeriod")
                    }
                    buffer = ByteArray(framePeriod * bitsPerSample / 8 * numChannels)
                    try {
                        audioRecord = AudioRecord(
                            audioSource, sampleRate, channel, audioFormat, bufferSize
                        )
                        if (audioRecord?.state != STATE_INITIALIZED) {
                            disconnect()
                        } else if (audioRecord?.state == STATE_INITIALIZED) {
                            logger.info("Successfully initialized AudioRecord")
                            status = SourceStatusListener.Status.CONNECTED
                            mainHandler.post(::observeMicrophones)
                        }
                    } catch (ex: IllegalArgumentException) {
                        logger.error("Invalid parameters passed to AudioRecord constructor. ", ex)
                    } catch (ex: Exception) {
                        logger.error("Exception while initializing AudioRecord. ", ex)
                    }
                } else {
                    logger.error("Error in calculating buffer size")
                    disconnect()
                }
            } else {
                logger.error("Permission not granted for RECORD_AUDIO, disconnecting now")
                disconnect()
            }
        }
    }

    override fun startRecording() {
        startAudioRecording()
    }

    override fun pauseRecording() {
        state.isPaused.value = true
    }

    override fun resumeRecording() {
        state.isPaused.value = false
    }

    override fun stopRecording() {
        stopAudioRecording()
    }

    override fun clear() {
        clearAudioDirectory()
    }

    override fun send() {
        state.finalizedMicrophone.value?.let { mic ->
            recordingFile?.let { wavFile ->
                var audioDuration: Long = -1L
                try {
                    audioDuration = AudioDeviceUtils.getAudioDuration(wavFile) ?: throw RuntimeException("Can't retrieve the duration of audio file")
                    if (audioDuration == -1L) {
                        throw RuntimeException("Audio length not retrieved")
                    }
                } catch (ex: Exception) {
                    logger.error("Cannot retrieve audio file duration. Discarding sending data")
                }
                recordProcessingHandler.execute {
                    send(
                        audioInputTopic, PhoneAudioInput(
                            currentTime,
                            currentTime,
                            "will be set after s3 functionality is added",
                            "after data sending to s3 is enabled",
                            mic.productName.toString(),
                            mic.id.toString(),
                            mic.sampleRates.joinToString(" "),
                            mic.encodings.joinToString(" ") {
                                AudioTypeFormatUtil.toLogFriendlyEncoding(it)
                            },
                            mic.type.toLogFriendlyType(),
                            mic.channelCounts.joinToString(" "),
                            audioDuration,
                            wavFile.length(),
                            state.isRecordingPlayed,
                            wavFile.extension,
                            sampleRate,
                            AudioTypeFormatUtil.toLogFriendlyEncoding(audioFormat)
                        )
                    )
                }
                // Dummy Toast, will be removed after file uploading to s3 will be enabled.
                Boast.makeText(service, "Sending last recorded audio. Is played? : ${state.isRecordingPlayed}", Toast.LENGTH_LONG).show()

            }

        }

        // After the data is sent:
        state.isRecordingPlayed = false
    }

    override fun setPreferredMicrophone(microphone: AudioDeviceInfo) {
        state.microphonePrioritized = true
        microphone.let(setPreferredDeviceAndUpdate)
    }

    private fun observeMicrophones() {
        if (!state.connectedMicrophones.hasActiveObservers()) {
            state.connectedMicrophones.observeForever(connectedMicrophonesObserver)
        }
    }

    private fun runDeviceSelectionLogic(connectedMicrophones: List<AudioDeviceInfo>) {
        logger.info("Running device selection logic")
       (arrayOf(TYPE_USB_DEVICE, TYPE_USB_HEADSET).let { deviceTypes ->
            connectedMicrophones.run { preferByDeviceType(deviceTypes) }
        }?: arrayOf(TYPE_WIRED_HEADSET).let { deviceTypes ->
           connectedMicrophones.run { preferByDeviceType(deviceTypes) }
        } ?: arrayOf(TYPE_BLUETOOTH_A2DP, TYPE_BLUETOOTH_SCO).let { deviceTypes ->
            connectedMicrophones.run { preferByDeviceType(deviceTypes) }
        } ?: connectedMicrophones.firstOrNull())?.also(setPreferredDeviceAndUpdate)
    }

    private fun List<AudioDeviceInfo>.preferByDeviceType(types: Array<Int>): AudioDeviceInfo? =
        types.firstNotNullOfOrNull { deviceType ->
            this.firstOrNull { deviceType == it.type }
        }

    private fun clearAudioDirectory() {
        payloadSize = 0
        audioDir?.let { audioDir ->
            audioDir.parentFile
                ?.list { _, name -> name.startsWith("phone_audio_input") && name.endsWith(".wav") }
                ?.forEach {
                    File(audioDir.parentFile, it).delete()
                    logger.debug("Deleted audio file: {}", it)
                }

            audioDir.walk()
                .filter { it.name.startsWith("phone_audio_input") && it.path.endsWith(".wav") }
                .forEach {
                    it.delete()
                    logger.debug("Deleted file: {}", it)
                }
        }
    }

    private fun startAudioRecording() {
        audioRecordingHandler.execute {
            setupRecording()
            if ((audioRecord?.state == STATE_INITIALIZED) && (recordingFile != null)) {
                if (!state.microphonePrioritized) {
                    state.connectedMicrophones.value?.also(::runDeviceSelectionLogic)
                } else {
                    state.finalizedMicrophone.value?.let(setPreferredDeviceAndUpdate)
                }
                audioRecord?.startRecording()
                mainHandler.post { state.isRecording.value = true }
                recordProcessingHandler.execute{
                    audioRecord?.read(buffer, 0, buffer.size)
                    currentlyRecording = true
                    state.finalizedMicrophone.postValue(audioRecord?.routedDevice)
                    logger.info("Finalized routed device: {}", state.finalizedMicrophone.value?.productName)
                }
                logger.trace("Started recording")
                audioRecord?.setRecordPositionUpdateListener(updateListener)
                audioRecord?.setPositionNotificationPeriod(framePeriod)
            } else {
                logger.error("Trying to start recording on uninitialized AudioRecord or filePath is null, state: ${audioRecord?.state}, $recordingFile")
                disconnect()
            }
        }
    }

    private fun setupRecording() {
            clearAudioDirectory()
            setRecordingPath()
            writeFileHeaders()
    }

    private fun setRecordingPath() {
        recordingFile = File(audioDir, "phone_audio_input" + System.currentTimeMillis() + ".wav")

        preferences.edit()
            .putString(LAST_RECORDED_AUDIO_FILE, recordingFile!!.absolutePath)
            .apply()
        randomAccessWriter = RandomAccessFile(recordingFile, "rw")
    }

    private fun writeFileHeaders() {

        randomAccessWriter?.apply {
            setLength(0) // Set file length to 0, to prevent unexpected behavior in case the file already existed

            val header = ByteArray(44)
            AudioDeviceUtils.setWavHeaders(header, numChannels, sampleRate, bitsPerSample)
            write(header, 0, 44)
        }
    }

    private val updateListener = object : AudioRecord.OnRecordPositionUpdateListener {
        override fun onMarkerReached(recorder: AudioRecord?) {
            // No Action
        }

        override fun onPeriodicNotification(recorder: AudioRecord?) {
            if (currentlyRecording && !state.isPaused.value!!) {
                audioRecordingHandler.execute {
                    audioRecord?.let {
                        val dataRead = it.read(buffer, 0, buffer.size)
                        randomAccessWriter?.write(buffer)
                        payloadSize += dataRead
                        logger.debug("onPeriodicNotification: Recording Audio")
                    }
                }
            } else if (state.isPaused.value!!) {
                // Triggering a dummy read to keep the callback active
                audioRecordingHandler.execute { audioRecord?.read(buffer, 0, buffer.size) }
                logger.debug("Callback: onPeriodicNotification: recording is paused.")
            }
            else {
                logger.debug("Callback: onPeriodicNotification after recording is stopped.")
            }
        }
    }

    private fun stopAudioRecording() {
        logger.debug("Stopping Recording: Saving data")
        mainHandler.post { state.isRecording.value = false }
        audioRecordingHandler.execute {
            currentlyRecording = false
            audioRecord?.apply {
                setRecordPositionUpdateListener(null)
                stop()
            }
            randomAccessWriter?.apply {
                val chunkSize = 36 + payloadSize
                seek(4)
                write(chunkSize and 0xff)
                write((chunkSize shr 8) and 0xff)
                write((chunkSize shr 16) and 0xff)
                write((chunkSize shr 24) and 0xff)

                seek(40)
                write(payloadSize and 0xff)
                write((payloadSize shr 8) and 0xff)
                write((payloadSize shr 16) and 0xff)
                write((payloadSize shr 24) and 0xff)
                close()
                payloadSize = 0
            }
        }
    }

    override fun onClose() {
        audioRecordingHandler.stop{
            audioRecord?.release()
            clearAudioDirectory()
        }
        mainHandler.post { state.connectedMicrophones.removeObserver(connectedMicrophonesObserver) }
        recordProcessingHandler.stop()
    }
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(PhoneAudioInputManager::class.java)

        /** The interval(ms) in which the recorded samples are output to the file */
        private const val TIMER_INTERVAL = 120
    }
}