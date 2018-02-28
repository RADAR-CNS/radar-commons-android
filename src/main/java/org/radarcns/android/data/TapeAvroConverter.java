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

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.data.Record;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.BackedObjectQueue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Converts records from an AvroTopic for Tape
 */
public class TapeAvroConverter<K extends SpecificRecord, V extends SpecificRecord>
        implements BackedObjectQueue.Converter<Record<K, V>> {
    private static final byte[] EMPTY_HEADER = {0, 0, 0, 0, 0, 0, 0, 0};
    private final EncoderFactory encoderFactory;
    private final DecoderFactory decoderFactory;
    private final SpecificDatumWriter<K> keyWriter;
    private final SpecificDatumWriter<V> valueWriter;
    private final SpecificDatumReader<K> keyReader;
    private final SpecificDatumReader<V> valueReader;
    private final SpecificData specificData;
    private final String topicName;
    private final Schema keySchema;
    private final Schema valueSchema;
    private BinaryEncoder encoder;
    private BinaryDecoder decoder;

    public TapeAvroConverter(AvroTopic<K, V> topic, SpecificData specificData) throws IOException {
        this.specificData = specificData;
        topicName = topic.getName();
        encoderFactory = EncoderFactory.get();
        decoderFactory = DecoderFactory.get();
        keySchema = topic.getKeySchema();
        valueSchema = topic.getValueSchema();
        keyWriter = new SpecificDatumWriter<>(keySchema, specificData);
        valueWriter = new SpecificDatumWriter<>(valueSchema, specificData);
        keyReader = new SpecificDatumReader<>(keySchema, keySchema, specificData);
        valueReader = new SpecificDatumReader<>(valueSchema, valueSchema, specificData);
        encoder = null;
        decoder = null;
    }

    public Record<K, V> deserialize(InputStream in) throws IOException {
        // for backwards compatibility
        int numRead = 0;
        do {
            numRead += in.skip(8 - numRead);
        } while (numRead < 8);

        decoder = decoderFactory.binaryDecoder(in, decoder);

        K key;
        V value;
        try {
            key = keyReader.read(null, decoder);
            value = valueReader.read(null, decoder);
        } catch (RuntimeException ex) {
            throw new IOException("Failed to deserialize object", ex);
        }

        if (!specificData.validate(keySchema, key)
                || !specificData.validate(valueSchema, value)) {
            throw new IllegalArgumentException("Failed to validate given record in topic "
                    + topicName + "\n\tkey: " + key + "\n\tvalue: " + value);
        }
        return new Record<>(key, value);
    }

    public void serialize(Record<K, V> o, OutputStream out) throws IOException {
        // for backwards compatibility
        out.write(EMPTY_HEADER, 0, 8);
        encoder = encoderFactory.binaryEncoder(out, encoder);
        keyWriter.write(o.key, encoder);
        valueWriter.write(o.value, encoder);
        encoder.flush();
    }
}
