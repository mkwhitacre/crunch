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
package org.apache.crunch.kafka.inputformat;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.crunch.kafka.inputformat.KafkaUtils.getKafkaConnectionProperties;

/**
 * A {@link RecordReader} for pulling data from Kafka.
 * @param <K> the key of the records from Kafka
 * @param <V> the value of the records from Kafka
 */
public class KafkaRecordReader<K, V> extends RecordReader<K, V> {

  /**
   * Constant to indicate how long the reader wait before timing out when retrieving data from Kafka.
   */
  public static final String CONSUMER_POLL_TIMEOUT_KEY = "org.apache.crunch.kafka.consumer.poll.timeout";

  /**
   * Default timeout value for {@link #CONSUMER_POLL_TIMEOUT_KEY} of 1 second.
   */
  public static final long CONSUMER_POLL_TIMEOUT_DEFAULT = 1000L;

  private Consumer<K, V> consumer;
  private ConsumerRecord<K, V> record;
  private long endingOffset;
  private Iterator<ConsumerRecord<K, V>> recordIterator;
  private long consumerPollTimeout;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    consumer = new KafkaConsumer<>(getKafkaConnectionProperties(taskAttemptContext.getConfiguration()));
    KafkaInputSplit split = (KafkaInputSplit) inputSplit;
    TopicPartition topicPartition = split.getTopicPartition();
    consumer.assign(Collections.singletonList(topicPartition));
    //suggested hack to gather info without gathering data
    consumer.poll(0);
    //now seek to the desired start location
    consumer.seek(topicPartition, split.getStartingOffset());
    endingOffset = split.getEndingOffset();

    consumerPollTimeout = taskAttemptContext.getConfiguration()
        .getLong(CONSUMER_POLL_TIMEOUT_KEY, CONSUMER_POLL_TIMEOUT_DEFAULT);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    recordIterator = getRecords();
    record = recordIterator.hasNext() ? recordIterator.next() : null;
    return record != null && record.offset() < endingOffset;
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return record.key();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return record.value();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    //not most accurate but gives reasonable estimate
    return record == null ? 0.0f : record.offset() / endingOffset;
  }

  private Iterator<ConsumerRecord<K, V>> getRecords() {
    if (recordIterator == null || !recordIterator.hasNext()) {
      ConsumerRecords<K, V> records = null;
      records = consumer.poll(consumerPollTimeout);
      return records != null ? records.iterator() : ConsumerRecords.<K, V>empty().iterator();
    }
    return recordIterator;
  }

  @Override
  public void close() throws IOException {
    consumer.close();
  }
}