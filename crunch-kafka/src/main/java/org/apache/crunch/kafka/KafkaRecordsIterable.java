package org.apache.crunch.kafka;

import org.apache.crunch.Pair;
import org.apache.crunch.kafka.inputformat.KafkaRecordReader;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;


//TODO could close consumer when all done with data but then this becomes single use.
class KafkaRecordsIterable<K, V> implements Iterable<Pair<K, V>> {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordsIterable.class);

    /**
     * The Kafka consumer responsible for retrieving messages.
     */
    private final Consumer<K, V> consumer;

    /**
     * The starting positions of the iterable for the topic.
     */
    private final Map<TopicPartition, Pair<Long, Long>> offsets;

    /**
     * Tracks if the iterable is empty.
     */
    private final boolean isEmpty;

    /**
     * The poll time between each request to Kafka
     */
    private final long scanPollTime;

    /**
     * Creates the iterable that will pull values for a given {@code topic} using the provided {@code consumer} between
     * the {@code startOffsets} and {@code stopOffsets}.
     * @param consumer The consumer for pulling the data from Kafka.  Callers are responsible for managing the consumer
     *                 instance.
     * @param offsets offsets for pulling data
     * @param properties properties for tweaking the behavior of the iterable.
     * @throws IllegalArgumentException if any of the arguments are {@code null} or empty.
     */
    public KafkaRecordsIterable(Consumer<K, V> consumer, Map<TopicPartition, Pair<Long, Long>> offsets,
                                Properties properties ){
        if(consumer == null){
            throw new IllegalArgumentException("The 'consumer' cannot be 'null'.");
        }
        this.consumer = consumer;

        if(offsets == null){
            throw new IllegalArgumentException("The 'offsets' cannot 'null' or empty.");
        }
        this.offsets = offsets;

        if(properties == null){
            throw new IllegalArgumentException("The 'properties' cannot be 'null'.");
        }

        //check to make sure that based on the offsets there is data to retrieve, otherwise false.
        //there will be data if the start offsets are less than stop offsets-1
        if(offsets.isEmpty()){
            isEmpty = true;
            LOG.warn("Iterable for Kafka for is empty because offsets are empty.");
        }else {
            boolean tempIsEmpty = true;
            for(Map.Entry<TopicPartition, Pair<Long, Long>> entry : offsets.entrySet()){
                Pair<Long, Long> value = entry.getValue();
                //if start is less than one less than stop then there is data to be had
                tempIsEmpty &= value.first() < (value.second() -1);
            }
            isEmpty = tempIsEmpty;
            if(isEmpty){
                LOG.warn("Scan for Kafka is empty because start offsets are equal to or later than stop offsets");
            }
        }

        scanPollTime = Long.parseLong(properties.getProperty(KafkaRecordReader.CONSUMER_POLL_TIMEOUT_KEY,
                Long.toString(KafkaRecordReader.CONSUMER_POLL_TIMEOUT_DEFAULT)));
    }

    @Override
    public Iterator<Pair<K, V>> iterator() {
        if(isEmpty) {
            LOG.debug("Returning empty iterator since offsets align.");
            return Collections.emptyIterator();
        }
        //Assign consumer to all of the partitions
        LOG.debug("Assigning topics and partitions and seeking to start offsets.");

        consumer.assign(new LinkedList<>(offsets.keySet()));
        //hack so maybe look at removing this
        consumer.poll(0);
        for(Map.Entry<TopicPartition, Pair<Long, Long>> entry: offsets.entrySet()){
            consumer.seek(entry.getKey(), entry.getValue().first());
        }

        return new RecordsIterator(consumer, offsets, scanPollTime);
    }

    private static class RecordsIterator<K, V> implements Iterator<Pair<K, V>> {

        private final Consumer<K, V> consumer;
        private final Map<TopicPartition, Pair<Long, Long>> offsets;
        private final long pollTime;
        private ConsumerRecords<K, V> records;
        private Iterator<ConsumerRecord<K, V>> currentIterator;
        private final Set<TopicPartition> remainingPartitions;

        private Pair<K, V> next;

        public RecordsIterator(Consumer<K, V> consumer,
                               Map<TopicPartition, Pair<Long, Long>> offsets, long pollTime){
            this.consumer = consumer;
            remainingPartitions = new HashSet<>(offsets.keySet());
            this.offsets = offsets;
            this.pollTime = pollTime;
        }

        @Override
        public boolean hasNext() {
            if (next != null)
                return true;

            //if partitions to consume then pull next value
            if(remainingPartitions.size() > 0) {
                next = getNext();
            }

            return next != null;
        }

        @Override
        public Pair<K, V> next() {
            if(next == null){
                next = getNext();
            }

            if(next != null){
                Pair<K, V> returnedNext = next;
                //prime for next call
                next = getNext();
                //return the current next
                return returnedNext;
            }else{
                throw new NoSuchElementException("No more elements.");
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not supported.");
        }

        /**
         * Gets the current iterator.
         *
         * @return the current iterator or {@code null} if there are no more values to consume.
         */
        private Iterator<ConsumerRecord<K, V>> getIterator(){
            if(!remainingPartitions.isEmpty()) {
                if (currentIterator != null && currentIterator.hasNext()) {
                    return currentIterator;
                }
                LOG.debug("Retrieving next set of records.");
                records = consumer.poll(pollTime);
                if (records == null || records.isEmpty()) {
                    LOG.debug("Retrieved empty records.");
                    currentIterator = null;
                    return null;
                }
                currentIterator = records.iterator();
                return currentIterator;
            }

            LOG.debug("No more partitions to consume therefore not retrieving any more records.");
            return null;
        }

        /**
         * Internal method for retrieving the next value to retrieve.
         *
         * @return {@code null} if there are no more values to retrieve otherwise the next event.
         */
        private Pair<K, V> getNext(){
            while(!remainingPartitions.isEmpty()) {
                Iterator<ConsumerRecord<K, V>> iterator = getIterator();

                while(iterator != null && iterator.hasNext()){
                    ConsumerRecord<K, V> record = iterator.next();
                    TopicPartition tP = new TopicPartition(record.topic(), record.partition());
                    long offset = record.offset();

                    if(withInRange(tP, offset)) {
                        LOG.debug("Retrieving value for {} with offset {}.", tP, offset);
                        return Pair.of(record.key(), record.value());
                    }
                    LOG.debug("Value for {} with offset {} is outside of range skipping.", tP, offset);
                }
            }
            LOG.debug("Consumed data from all partitions.");
            return null;

        }

        /**
         * Checks whether the value for {@code topicPartition} with an {@code offset} is within scan range.  If
         * the value is not then {@code false} is returned otherwise {@code true}.
         *
         * @param topicPartion The partition for the offset
         * @param offset the offset in the partition
         * @return {@code true} if the value is within the expected consumption range, otherwise {@code false}.
         */
        private boolean withInRange(TopicPartition topicPartion, long offset){
            long endOffset = offsets.get(topicPartion).second();
            //end offsets are one higher than the last written value.
            boolean emit = offset < endOffset;
            if(offset >= endOffset-1){
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Completed consuming partition {} with offset {} and ending offset {}.",
                            new Object[]{topicPartion, offset, endOffset});
                }
                remainingPartitions.remove(topicPartion);
                consumer.pause(topicPartion);
            }
            LOG.debug("Value for partition {} and offset {} is within range.", topicPartion, offset);
            return emit;
        }
    }
}

