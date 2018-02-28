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

package org.radarcns.android.data;

import android.util.Pair;

import org.apache.avro.specific.SpecificData;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.radarcns.android.util.AndroidThreadFactory;
import org.radarcns.android.util.SharedSingleThreadExecutorFactory;
import org.radarcns.data.Record;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.monitor.application.ApplicationUptime;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.ActiveAudioRecording;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.RuntimeEnvironment;
import org.robolectric.annotation.Config;
import org.slf4j.impl.HandroidLoggerAdapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static android.os.Process.THREAD_PRIORITY_BACKGROUND;
import static org.junit.Assert.assertEquals;

@RunWith(RobolectricTestRunner.class)
@Config(manifest = Config.NONE)
public class TapeCacheTest {
    private SharedSingleThreadExecutorFactory executorFactory;
    private TapeCache<ObservationKey, ApplicationUptime> tapeCache;
    private ObservationKey key;
    private ApplicationUptime value;
    private SpecificData specificData = CacheStore.getInstance().getSpecificData();

    @BeforeClass
    public static void setUpClass() {
        System.err.println("Setting up TapeCacheTest tests");
        HandroidLoggerAdapter.APP_NAME = "Test";
        HandroidLoggerAdapter.LOG_TO_CRASHLYTICS = false;
    }

    @Before
    public void setUp() throws IOException {
        System.err.println("log to crashlytics " + HandroidLoggerAdapter.LOG_TO_CRASHLYTICS);

        AvroTopic<ObservationKey, ApplicationUptime> topic = new AvroTopic<>("test",
                ObservationKey.getClassSchema(), ApplicationUptime.getClassSchema(),
                ObservationKey.class, ApplicationUptime.class);
        executorFactory = new SharedSingleThreadExecutorFactory(
                new AndroidThreadFactory("test", THREAD_PRIORITY_BACKGROUND));
        tapeCache = new TapeCache<>(
                RuntimeEnvironment.application.getApplicationContext(),
                topic, executorFactory, specificData);
        tapeCache.setMaximumSize(4096);
        tapeCache.setTimeWindow(100);

        key = new ObservationKey("test", "a", "b");
        double time = System.currentTimeMillis() / 1000d;
        value = new ApplicationUptime(time, System.nanoTime() / 1_000_000_000d);
    }

    @After
    public void tearDown() throws IOException {
        tapeCache.close();
        executorFactory.close();
    }

    @Test
    public void addMeasurement() throws Exception {
        assertEquals(Collections.emptyList(), tapeCache.unsentRecords(100));
        assertEquals(new Pair<>(0L, 0L), tapeCache.numberOfRecords());
        assertEquals(Collections.emptyList(), tapeCache.getRecords(100));

        tapeCache.addMeasurement(key, value);

        assertEquals(Collections.emptyList(), tapeCache.unsentRecords(100));
        assertEquals(new Pair<>(0L, 0L), tapeCache.numberOfRecords());

        Thread.sleep(100);

        List<Record<ObservationKey, ApplicationUptime>> unsent = tapeCache.unsentRecords(100);
        assertEquals(1, unsent.size());
        assertEquals(new Pair<>(1L, 0L), tapeCache.numberOfRecords());
        unsent = tapeCache.unsentRecords(100);
        assertEquals(1, unsent.size());
        assertEquals(new Pair<>(1L, 0L), tapeCache.numberOfRecords());
        Record<ObservationKey, ApplicationUptime> record = unsent.get(0);
        assertEquals(key, record.key);
        assertEquals(value, record.value);
        tapeCache.remove(1);
        assertEquals(Collections.emptyList(), tapeCache.unsentRecords(100));
        assertEquals(new Pair<>(0L, 0L), tapeCache.numberOfRecords());

        tapeCache.addMeasurement(key, value);
        tapeCache.addMeasurement(key, value);

        Thread.sleep(100);

        unsent = tapeCache.unsentRecords(100);
        assertEquals(2, unsent.size());
        assertEquals(new Pair<>(2L, 0L), tapeCache.numberOfRecords());
    }

    @Test
    public void testBinaryObject() throws IOException {
        AvroTopic<ObservationKey, ActiveAudioRecording> topic = new AvroTopic<>("test",
                ObservationKey.getClassSchema(), ActiveAudioRecording.getClassSchema(),
                ObservationKey.class, ActiveAudioRecording.class);
        TapeCache<ObservationKey, ActiveAudioRecording> localTapeCache = new TapeCache<>(
                RuntimeEnvironment.application.getApplicationContext(),
                topic, executorFactory, specificData);

        localTapeCache.setMaximumSize(45000000);
        localTapeCache.setTimeWindow(100);

        Random random = new Random();
        byte[] data = new byte[176482];
        random.nextBytes(data);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        ActiveAudioRecording localValue = new ActiveAudioRecording(buffer);

        localTapeCache.addMeasurement(key, localValue);
        localTapeCache.flush();
        List<Record<ObservationKey, ActiveAudioRecording>> records = localTapeCache.unsentRecords(100);

        assertEquals(1, records.size());
        Record<ObservationKey, ActiveAudioRecording> firstRecord = records.get(0);
        assertEquals(firstRecord.key, key);
        assertEquals(firstRecord.value, localValue);
    }

    @Test
    public void flush() throws Exception {
        assertEquals(Collections.emptyList(), tapeCache.unsentRecords(100));
        assertEquals(new Pair<>(0L, 0L), tapeCache.numberOfRecords());
        assertEquals(Collections.emptyList(), tapeCache.getRecords(100));

        tapeCache.addMeasurement(key, value);

        assertEquals(Collections.emptyList(), tapeCache.unsentRecords(100));
        assertEquals(new Pair<>(0L, 0L), tapeCache.numberOfRecords());

        tapeCache.flush();

        assertEquals(1, tapeCache.unsentRecords(100).size());
        assertEquals(new Pair<>(1L, 0L), tapeCache.numberOfRecords());
    }
}
