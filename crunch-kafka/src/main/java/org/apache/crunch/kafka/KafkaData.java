package org.apache.crunch.kafka;

import org.apache.crunch.Pair;
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.kafka.inputformat.KafkaInputFormat;
import org.apache.crunch.kafka.inputformat.KafkaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class KafkaData<K, V> implements ReadableData<Pair<K,V>> {

    private final Map<TopicPartition, Pair<Long, Long>> offsets;
    private final Properties props;

    public KafkaData(Properties connectionProperties,
                     Map<TopicPartition, Pair<Long, Long>> offsets){
        this.props = connectionProperties;
        this.offsets = offsets;
    }


    @Override
    public Set<SourceTarget<?>> getSourceTargets() {
        return null;
    }

    @Override
    public void configure(Configuration conf) {
        //no-op
    }

    @Override
    public Iterable<Pair<K, V>> read(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException {
        //TODO figure out how to close consumer
        Consumer<K, V> consumer = new KafkaConsumer<K, V>(props);
        return new KafkaRecordsIterable<>(consumer, offsets, props);
    }
}
