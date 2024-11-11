/*
 * Copyright 2017 Universität Passau and The Hyve
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

package org.radarbase.passive.audio

import android.util.Base64
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.BaseSourceState
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.OfflineProcessor
import org.radarbase.passive.audio.OpenSmileAudioService.Companion.DEFAULT_RECORD_RATE
import org.radarbase.passive.audio.opensmile.SmileJNI
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.opensmile.OpenSmile2PhoneAudio
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.util.concurrent.TimeUnit

/** Manages Phone sensors  */
class OpensmileAudioManager(service: OpenSmileAudioService) : AbstractSourceManager<OpenSmileAudioService, BaseSourceState>(service) {
    private val audioTopic: DataCache<ObservationKey, OpenSmile2PhoneAudio> = createCache("android_processed_audio", OpenSmile2PhoneAudio())

    @get:Synchronized
    @set:Synchronized
    lateinit var config: AudioConfiguration

    private val processor: OfflineProcessor
    private var isRunning: Boolean = false
    private val dataDirectory: File?
    private val audioTaskExecutor = CoroutineTaskExecutor(this::class.simpleName!!)

    init {
        name = service.getString(R.string.header_audio_status)

        processor = OfflineProcessor(service) {
            process = listOf(this@OpensmileAudioManager::processAudio)
            requestCode = AUDIO_REQUEST_CODE
            requestName = AUDIO_REQUEST_NAME
            interval(DEFAULT_RECORD_RATE, TimeUnit.SECONDS)
            wake = true
        }
        val externalDirectory = service.filesDir
        status = if (externalDirectory != null) {
            dataDirectory = File(externalDirectory, "org.radarbase.passive.audio")
            dataDirectory.mkdirs()
            clearDataDirectory()
            SourceStatusListener.Status.READY
        } else {
            dataDirectory = null
            SourceStatusListener.Status.UNAVAILABLE
        }
    }

    override fun start(acceptableIds: Set<String>) {
        audioTaskExecutor.start()
        isRunning = true
        processor.start {
            SmileJNI.init(service)
            register()
        }
        // Audio recording
        status = SourceStatusListener.Status.CONNECTED
    }

    private suspend fun processAudio() {
        status = SourceStatusListener.Status.CONNECTED
        logger.info("Setting up audio recording")
        val dataPath = File(dataDirectory, "audio_" + System.currentTimeMillis() + ".bin")
        //openSMILE.clas.SMILExtractJNI(conf,1,dataPath);
        val localConfig = config
        val smileJNI = SmileJNI(localConfig.configFile, dataPath.absolutePath, localConfig.recordDurationMillis)
        logger.info("Starting audio recording with configuration {} and duration {}, stored to {}",
                localConfig.configFile, localConfig.recordDurationMillis, dataPath)
        val startTime = currentTime
        try {
            smileJNI.run()
            logger.info("Finished recording audio to file {}", dataPath)
        } catch (ex: RuntimeException) {
            logger.error("Failed to record audio", ex)
            return
        }

        try {
            if (dataPath.exists()) {
                send(audioTopic, OpenSmile2PhoneAudio(
                        startTime,
                        currentTime,
                        localConfig.configFile,
                        Base64.encodeToString(dataPath.readBytes(), Base64.DEFAULT)))
                dataPath.delete()
                status = SourceStatusListener.Status.READY
            } else {
                logger.warn("Failed to read audio file")
            }
        } catch (e: IOException) {
            logger.error("Failed to read audio file")
        }
    }

    override fun onClose() {
        if (isRunning) {
            audioTaskExecutor.stop {
                processor.stop()
            }
        }
        clearDataDirectory()
    }

    fun setRecordRate(audioRecordRateMs: Long) {
        processor.interval(audioRecordRateMs, TimeUnit.SECONDS)
    }

    data class AudioConfiguration(val configFile: String, val recordDuration: Long, val unit: TimeUnit) {
        val recordDurationMillis: Long
            get() = unit.toMillis(recordDuration)
    }

    private fun clearDataDirectory() {
        dataDirectory?.let { audioDir ->
            audioDir.parentFile
                    ?.list { _, name -> name.startsWith("audio_") && name.endsWith(".bin") }
                    ?.forEach { File(audioDir.parentFile, it).delete() }

            audioDir.walk().filter { it.startsWith("audio_") && it.path.endsWith(".bin") }
                    .forEach { it.delete() }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(OpensmileAudioManager::class.java)
        private const val AUDIO_REQUEST_CODE = 130102
        private const val AUDIO_REQUEST_NAME = "org.radarcns.audio.AudioDeviceManager"
    }
}
