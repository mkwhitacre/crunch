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
import org.apache.crunch.ReadableData;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.mr.run.CrunchInputFormat;
import org.apache.crunch.io.CrunchInputs;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileTableSourceImpl;
import org.apache.crunch.kafka.inputformat.KafkaInputFormat;
import org.apache.crunch.kafka.inputformat.KafkaUtils;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class KafkaSource<K, V> implements TableSource<K, V>, ReadableSource<Pair<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

    private final FormatBundle inputBundle;
    private final PTableType<K, V> type;
    private final Properties props;
    private final Map<TopicPartition, Pair<Long, Long>> offsets;

    public KafkaSource(Properties kafkaConnectionProperties,
                       PTableType<K, V> tableType,
                       Class keyDeserializerClass,
                       Class valueDeserializerClass,
                       Map<TopicPartition, Pair<Long, Long>> offsets) {
        this.type = tableType;
        inputBundle = createFormatBundle(kafkaConnectionProperties, keyDeserializerClass, valueDeserializerClass,
                        offsets);




        this.props = kafkaConnectionProperties;
        this.offsets = offsets;
    }

    @Override
    public Source<Pair<K, V>> inputConf(String key, String value) {
        inputBundle.set(key, value);
        return this;
    }

    @Override
    public PType<Pair<K, V>> getType() {
        return type;
    }

    @Override
    public Converter<?, ?, ?, ?> getConverter() {
        return type.getConverter();
    }

    @Override
    public PTableType<K, V> getTableType() {
        return type;
    }

    @Override
    public long getSize(Configuration configuration) {
        // TODO something smarter here.
        return 1000L * 1000L * 1000L;
    }

    @Override
    public long getLastModifiedAt(Configuration configuration) {
        LOG.warn("Cannot determine last modified time for source: {}", toString());
        return -1;
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
    public Iterable<Pair<K, V>> read(Configuration conf) throws IOException {
        Properties props = KafkaUtils.getKafkaConnectionProperties(conf);
        //TODO how to close this out?
        Consumer<K, V> consumer = new KafkaConsumer<K, V>(props);
        Map<TopicPartition, Pair<Long, Long>> offsets = KafkaInputFormat.getOffsets(conf);

        return new KafkaRecordsIterable<>(consumer, offsets, props);
    }


    @Override
    public void configureSource(Job job, int inputId) throws IOException {
        // Use Crunch to handle the combined input splits
        job.setInputFormatClass(CrunchInputFormat.class);
        CrunchInputs.addInputPaths(job, paths, inputBundle, inputId);


    }

    @Override
    public ReadableData<Pair<K, V>> asReadable() {
        return new KafkaData<>(props, offsets);
    }
}