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

import org.apache.commons.lang.StringUtils;
import org.apache.crunch.Pair;
import org.apache.crunch.io.FormatBundle;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaInputFormat extends InputFormat<BytesWritable, BytesWritable> implements Configurable {

  private static final String KAFKA_INPUT_OFFSETS_BASE = "org.apache.crunch.kafka.offsets.topic";
  private static final String PARTITIONS = "partitions";
  private static final String START = "start";
  private static final String END = "end";

  private static final String TOPIC_KEY_REGEX = KAFKA_INPUT_OFFSETS_BASE + "\\..*\\." + PARTITIONS + "$";

  private Map<TopicPartition, Pair<Long, Long>> offsets;
  private Configuration configuration;

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    offsets = getOffsets(getConf());
    List<InputSplit> splits = new LinkedList<>();
    for (Map.Entry<TopicPartition, Pair<Long, Long>> entry : offsets.entrySet()) {
      TopicPartition topicPartition = entry.getKey();
      splits.add(new KafkaInputSplit(topicPartition.topic(), topicPartition.partition(), entry.getValue().first(),
          entry.getValue().second()));
    }

    return splits;
  }

  @Override
  public RecordReader<BytesWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new KafkaRecordReader<>();
  }

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }


  /**
   * Writes the start and end offsets for the provided topic partitions to the {@code bundle}.
   * @param offsets The starting and ending offsets for the topics and partitions.
   * @param bundle the bundle into which the information should be persisted.
   */
  public static void writeOffsetsToBundle(Map<TopicPartition, Pair<Long, Long>> offsets, FormatBundle bundle) {
    for (Map.Entry<String, String> entry : generateValues(offsets).entrySet()) {
      bundle.set(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Writes the start and end offsets for the provided topic partitions to the {@code config}.
   * @param offsets The starting and ending offsets for the topics and partitions.
   * @param config the config into which the information should be persisted.
   */
  public static void writeOffsetsToConfiguration(Map<TopicPartition, Pair<Long, Long>> offsets, Configuration config) {
    for (Map.Entry<String, String> entry : generateValues(offsets).entrySet()) {
      config.set(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Reads the {@code configuration} to determine which topics, partitions, and offsets should be used for reading data.
   *
   * @param configuration the configuration to derive the data to read.
   * @return a map of {@link TopicPartition} to a pair of start and end offsets.
   */
  public static Map<TopicPartition, Pair<Long, Long>> getOffsets(Configuration configuration) {
    Map<TopicPartition, Pair<Long, Long>> offsets = new HashMap<>();
    //find configuration for all of the topics with defined partitions
    Map<String, String> topicPartitionKeys = configuration.getValByRegex(TOPIC_KEY_REGEX);

    //for each topic start to process it's partitions
    for (String key : topicPartitionKeys.keySet()) {
      String topic = getTopicFromKey(key);
      int[] partitions = configuration.getInts(key);
      //for each partition find and add the start/end offset
      for (int partitionId : partitions) {
        TopicPartition topicPartition = new TopicPartition(topic, partitionId);
        //TODO right now adding defaults but could error out because this should be set.
        long start = configuration.getLong(generatePartitionStartKey(topic, partitionId),
            kafka.api.OffsetRequest.EarliestTime());
        long end = configuration.getLong(generatePartitionEndKey(topic, partitionId),
            kafka.api.OffsetRequest.LatestTime());
        offsets.put(topicPartition, Pair.of(start, end));
      }
    }

    return offsets;
  }

  private static Map<String, String> generateValues(Map<TopicPartition, Pair<Long, Long>> offsets) {
    Map<String, String> values = new HashMap<>();
    Map<String, Set<Integer>> topicsPartitions = new HashMap<>();

    for (Map.Entry<TopicPartition, Pair<Long, Long>> entry : offsets.entrySet()) {
      TopicPartition topicPartition = entry.getKey();
      String topic = topicPartition.topic();
      String startKey = generatePartitionStartKey(topic, topicPartition.partition());
      String endKey = generatePartitionEndKey(topic, topicPartition.partition());
      //Add the start and end offsets for a specific partition
      values.put(startKey, Long.toString(entry.getValue().first()));
      values.put(endKey, Long.toString(entry.getValue().second()));

      Set<Integer> partitions = topicsPartitions.get(topic);
      if (partitions == null) {
        partitions = new HashSet<>();
        topicsPartitions.put(topic, partitions);
      }
      partitions.add(topicPartition.partition());
    }

    //generate the partitions values for each topic
    for (Map.Entry<String, Set<Integer>> entry : topicsPartitions.entrySet()) {
      String key = KAFKA_INPUT_OFFSETS_BASE + "." + entry.getKey() + "." + PARTITIONS;
      Set<Integer> partitions = entry.getValue();
      String partitionsString = StringUtils.join(partitions.toArray(new Integer[partitions.size()]), ",");
      values.put(key, partitionsString);
    }

    return values;
  }

  static String generatePartitionStartKey(String topic, int partition) {
    return KAFKA_INPUT_OFFSETS_BASE + "." + topic + "." + PARTITIONS + "." + partition + "." + START;
  }

  static String generatePartitionEndKey(String topic, int partition) {
    return KAFKA_INPUT_OFFSETS_BASE + "." + topic + "." + PARTITIONS + "." + partition + "." + END;
  }

  static String generateTopicPartitionsKey(String topic) {
    return KAFKA_INPUT_OFFSETS_BASE + "." + topic + "." + PARTITIONS;
  }

  static String getTopicFromKey(String key) {
    //strip off the base key + a trailing "."
    String value = key.substring(KAFKA_INPUT_OFFSETS_BASE.length() + 1);
    //strip off the end part + a preceding "."
    value = value.substring(0, (value.length() - (PARTITIONS.length() + 1)));

    return value;
  }
}