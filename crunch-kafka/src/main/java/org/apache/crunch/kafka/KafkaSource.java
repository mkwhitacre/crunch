/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.crunch.impl.mr.run.CrunchMapper;
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
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

public class KafkaSource
    implements TableSource<BytesWritable, BytesWritable>, ReadableSource<Pair<BytesWritable, BytesWritable>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  private final FormatBundle inputBundle;
  private final Properties props;
  private final Map<TopicPartition, Pair<Long, Long>> offsets;

  private static PTableType<BytesWritable, BytesWritable> KAFKA_SOURCE_TYPE =
      Writables.tableOf(Writables.writables(BytesWritable.class), Writables.writables(BytesWritable.class));


  public KafkaSource(Properties kafkaConnectionProperties, Map<TopicPartition, Pair<Long, Long>> offsets) {
    this.props = copyAndSetProperties(kafkaConnectionProperties);

    inputBundle = createFormatBundle(props, offsets);

    this.offsets = offsets;
  }

  @Override
  public Source<Pair<BytesWritable, BytesWritable>> inputConf(String key, String value) {
    inputBundle.set(key, value);
    return this;
  }

  @Override
  public PType<Pair<BytesWritable, BytesWritable>> getType() {
    return KAFKA_SOURCE_TYPE;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter() {
    return KAFKA_SOURCE_TYPE.getConverter();
  }

  @Override
  public PTableType<BytesWritable, BytesWritable> getTableType() {
    return KAFKA_SOURCE_TYPE;
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
                                                        Map<TopicPartition, Pair<Long, Long>> offsets) {

    FormatBundle<KafkaInputFormat> bundle = FormatBundle.forInput(KafkaInputFormat.class);

    KafkaInputFormat.writeOffsetsToBundle(offsets, bundle);

    for (String name : kafkaConnectionProperties.stringPropertyNames()) {
      bundle.set(name, kafkaConnectionProperties.getProperty(name));
    }

    return bundle;
  }

  private static <K, V> Properties copyAndSetProperties(Properties kakfaConnectionProperties) {

    Properties props = new Properties();
    props.putAll(kakfaConnectionProperties);

    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());


    return props;
  }


  @Override
  public Iterable<Pair<BytesWritable, BytesWritable>> read(Configuration conf) throws IOException {
    //TODO how to close this out?
    Consumer<BytesWritable, BytesWritable> consumer = new KafkaConsumer<>(props);
    return new KafkaRecordsIterable<BytesWritable, BytesWritable>(consumer, offsets, props);
  }


  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    Configuration conf = job.getConfiguration();
    if (inputId == -1) {
      job.setMapperClass(CrunchMapper.class);
      job.setInputFormatClass(inputBundle.getFormatClass());
      inputBundle.configure(conf);
    } else {
      Path dummy = new Path("/kafka/" + inputId);
      CrunchInputs.addInputPath(job, dummy, inputBundle, inputId);
    }
  }

  @Override
  public ReadableData<Pair<BytesWritable, BytesWritable>> asReadable() {
    return new KafkaData<>(props, offsets);
  }


  public static class BytesDeserializer implements Deserializer<BytesWritable> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public BytesWritable deserialize(String s, byte[] bytes) {
      return new BytesWritable(bytes);
    }

    @Override
    public void close() {

    }
  }
}