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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.kafka.common.TopicPartition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KafkaInputSplit extends InputSplit implements Writable {

  private String topic;
  private int partition;
  private long startingOffset;
  private long endingOffset;
  private transient TopicPartition topicPartition;

  public KafkaInputSplit() {

  }

  public KafkaInputSplit(String topic, int partition, long startingOffset, long endingOffset) {
    this.topic = topic;
    this.partition = partition;
    this.startingOffset = startingOffset;
    this.endingOffset = endingOffset;
    topicPartition = new TopicPartition(topic, partition);
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return startingOffset > 0 ? endingOffset - startingOffset : endingOffset;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    //Leave empty since data locality not really an issues.
    return new String[0];
  }

  public TopicPartition getTopicPartition() {
    if (topicPartition == null) {
      topicPartition = new TopicPartition(topic, partition);
    }
    return topicPartition;
  }

  public long getStartingOffset() {
    return startingOffset;
  }

  public long getEndingOffset() {
    return endingOffset;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(topic);
    dataOutput.writeInt(partition);
    dataOutput.writeLong(startingOffset);
    dataOutput.writeLong(endingOffset);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    topic = dataInput.readUTF();
    partition = dataInput.readInt();
    startingOffset = dataInput.readLong();
    endingOffset = dataInput.readLong();

    topicPartition = new TopicPartition(topic, partition);
  }

  @Override
  public String toString() {
    return getTopicPartition() + " Start: " + startingOffset + " End: " + endingOffset;
  }
}
