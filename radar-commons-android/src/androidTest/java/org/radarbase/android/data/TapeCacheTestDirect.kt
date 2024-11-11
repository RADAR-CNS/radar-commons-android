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

package org.radarbase.android.data

import android.os.Process.THREAD_PRIORITY_BACKGROUND
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.test.runTest
import org.apache.avro.generic.GenericRecord
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.radarbase.android.data.serialization.TapeAvroSerializationFactory
import org.radarbase.android.util.SafeHandler
import org.radarbase.topic.AvroTopic
import org.radarcns.kafka.ObservationKey
import org.radarcns.monitor.application.ApplicationUptime
import org.radarcns.util.ActiveAudioRecording
import org.slf4j.impl.HandroidLoggerAdapter
import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom

@RunWith(AndroidJUnit4::class)
class TapeCacheTestDirect {
    private lateinit var tapeCache: TapeCache<ObservationKey, ApplicationUptime>
    private lateinit var key: ObservationKey
    private lateinit var value: ApplicationUptime
    private val serializationFactory = TapeAvroSerializationFactory()

    @Rule @JvmField
    var folder = TemporaryFolder()

    private val audioCache: TapeCache<ObservationKey, ActiveAudioRecording>
        @Throws(IOException::class)
        get() {
            val topic = AvroTopic("test",
                    ObservationKey.getClassSchema(), ActiveAudioRecording.getClassSchema(),
                    ObservationKey::class.java, ActiveAudioRecording::class.java)
            val outputTopic = AvroTopic("test",
                    ObservationKey.getClassSchema(), ActiveAudioRecording.getClassSchema(),
                    Any::class.java, Any::class.java)

            return TapeCache(
                    folder.newFile(), topic, outputTopic, serializationFactory,
                    CacheConfiguration(100L))
        }

    @Before
    @Throws(IOException::class)
    fun setUp() {
        HandroidLoggerAdapter.APP_NAME = "Test"
        val topic = AvroTopic("test",
                ObservationKey.getClassSchema(), ApplicationUptime.getClassSchema(),
                ObservationKey::class.java, ApplicationUptime::class.java)
        val outputTopic = AvroTopic("test",
                ObservationKey.getClassSchema(), ApplicationUptime.getClassSchema(),
                Any::class.java, Any::class.java)

        tapeCache = TapeCache(folder.newFile(), topic,
                outputTopic, serializationFactory,
                CacheConfiguration(100, 4096, CacheConfiguration.QueueFileFactory.DIRECT))

        key = ObservationKey("test", "a", "b")
        val time = System.currentTimeMillis() / 1000.0
        value = ApplicationUptime(time, System.nanoTime() / 1_000_000_000.0)
    }

    @After
    @Throws(IOException::class)
    fun tearDown() = runTest {
        tapeCache.stop()
    }

    fun addMultipleMeasurements() = runTest {
        assertNull(tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT))
        assertEquals(0L, tapeCache.numberOfRecords)
        assertNull(tapeCache.getRecords(100))

        repeat(50) {
            tapeCache.addMeasurement(key, value)
        }

        Thread.sleep(100)

        assertEquals(50L, tapeCache.numberOfRecords)
        var unsent = tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT)!!
        assertEquals(50, unsent.size().toLong())
        tapeCache.remove(50)
        assertEquals(0L, tapeCache.numberOfRecords)

        repeat(50) {
            tapeCache.addMeasurement(key, value)
        }
        tapeCache.flush()
        assertEquals(50L, tapeCache.numberOfRecords)
        unsent = tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT)!!
        assertEquals(50, unsent.size().toLong())
        tapeCache.remove(50)
        assertEquals(0L, tapeCache.numberOfRecords)
    }

    @Test
    @Throws(Exception::class)
    fun addMeasurement() = runTest {
        assertNull(tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT))
        assertEquals(0L, tapeCache.numberOfRecords)
        assertNull(tapeCache.getRecords(100))

        tapeCache.addMeasurement(key, value)

        assertNull(tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT))
        assertEquals(0L, tapeCache.numberOfRecords)

        Thread.sleep(100)

        var unsent = tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT)!!
        assertEquals(1, unsent.size().toLong())
        assertEquals(1L, tapeCache.numberOfRecords)
        unsent = tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT)!!
        assertEquals(1, unsent.size().toLong())
        assertEquals(1L, tapeCache.numberOfRecords)
        val actualValue = unsent.iterator().next() as GenericRecord
        assertEquals(key.getSourceId(), (unsent.key as GenericRecord).get("sourceId"))
        assertEquals(value.getUptime(), actualValue.get("uptime"))
        tapeCache.remove(1)
        assertEquals(0L, tapeCache.numberOfRecords)
        val emptyRecords = tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT)
        if (emptyRecords != null) {
            assertTrue("Contains ${emptyRecords.key}-${emptyRecords.toList()}", false)
        }

        tapeCache.addMeasurement(key, value)
        tapeCache.addMeasurement(key, value)

        Thread.sleep(100)

        unsent = tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT)!!
        assertEquals(2, unsent.size().toLong())
        assertEquals(2L, tapeCache.numberOfRecords)
    }

    @Test
    @Throws(IOException::class)
    fun testBinaryObject() = runTest {
        val localTapeCache = audioCache

        val localValue = getRecording(176482)

        localTapeCache.addMeasurement(key, localValue)
        localTapeCache.flush()
        val records = localTapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT)

        assertNotNull(records)
        assertEquals(1, records!!.size().toLong())
        val firstRecord = records.iterator().next() as GenericRecord
        assertEquals(key.getSourceId(), (records.key as GenericRecord).get("sourceId"))
        assertEquals(localValue.getData(), firstRecord.get("data"))
    }

    private fun getRecording(size: Int): ActiveAudioRecording {
        val random = ThreadLocalRandom.current()
        val data = ByteArray(size)
        random.nextBytes(data)
        val buffer = ByteBuffer.wrap(data)
        return ActiveAudioRecording(buffer)
    }


    @Test
    @Throws(IOException::class)
    fun testMaxUnsentObject() = runTest {
        val localTapeCache = audioCache

        localTapeCache.addMeasurement(key, getRecording(100000))
        localTapeCache.addMeasurement(key, getRecording(100000))
        localTapeCache.flush()
        // fit two times header (8) + key (13) + value (100,000)
        var records = localTapeCache.getUnsentRecords(100, 200042)!!
        assertEquals(2, records.size().toLong())

        records = localTapeCache.getUnsentRecords(100, 200041)!!
        assertEquals(1, records.size().toLong())

        records = localTapeCache.getUnsentRecords(100, 1)!!
        assertEquals(1, records.size().toLong())
    }

    @Test
    @Throws(Exception::class)
    fun flush() = runTest {
        assertNull(tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT))
        assertEquals(0L, tapeCache.numberOfRecords)
        assertNull(tapeCache.getRecords(100))

        tapeCache.addMeasurement(key, value)

        assertNull(tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT))
        assertEquals(0L, tapeCache.numberOfRecords)

        tapeCache.flush()

        val unsent = tapeCache.getUnsentRecords(100, SIZE_LIMIT_DEFAULT)
        assertNotNull(unsent)
        assertEquals(1, unsent?.size())
        assertEquals(1L, tapeCache.numberOfRecords)
    }

    companion object {
        private const val SIZE_LIMIT_DEFAULT = 5_000_000L
    }
}
