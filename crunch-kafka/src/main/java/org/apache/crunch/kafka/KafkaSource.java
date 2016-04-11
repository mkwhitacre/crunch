/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.kafka;


import org.apache.crunch.Pair;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.impl.FileTableSourceImpl;
import org.apache.crunch.kafka.inputformat.KafkaInputFormat;
import org.apache.crunch.types.PTableType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class KafkaSource<K, V> extends FileTableSourceImpl<K, V> {

    public KafkaSource(Properties kafkaConnectionProperties,
                       PTableType<K, V> tableType,
                       Class keyDeserializerClass,
                       Class valueDeserializerClass,
                       Map<TopicPartition, Pair<Long, Long>> offsets) {
        super(new Path("kafkasource"),
                tableType, createFormatBundle(kafkaConnectionProperties, keyDeserializerClass, valueDeserializerClass,
                        offsets));


    }

    @Override
    public long getSize(Configuration configuration) {
        // TODO something smarter here.
        return 1000L * 1000L * 1000L;
    }

    private static <K, V> FormatBundle createFormatBundle(Properties kafkaConnectionProperties,
                                                          Class<? extends Deserializer<K>> keyDeserializerClass,
                                                          Class<? extends Deserializer<V>> valueDeserializerClass,
                                                          Map<TopicPartition, Pair<Long, Long>> offsets ){

        FormatBundle<KafkaInputFormat> bundle = FormatBundle.forInput(KafkaInputFormat.class)
                .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getName())
                .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());

        KafkaInputFormat.writeOffsetsToBundle(offsets, bundle);

        for(String name: kafkaConnectionProperties.stringPropertyNames()){
            bundle.set(name, kafkaConnectionProperties.getProperty(name));
        }

        return bundle;
    }


    @Override
    protected Iterable<Pair<K, V>> read(Configuration conf, FileReaderFactory<Pair<K, V>> readerFactory) throws IOException {
        //TODO implement this b/c it currently causes failures.
        return super.read(conf, readerFactory);
    }





}
