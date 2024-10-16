package org.apache.kafka.connect.mirror.integration;

import static org.junit.Assert.assertEquals;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import kafka.api.ConsumerBounceTest;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.mirror.MirrorMaker;
import org.apache.log4j.Logger;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.FutureUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MirrorToUrsaTest {
    private static Logger log = Logger.getLogger(MirrorToUrsaTest.class);
    final AdminClient adminClient;
    final PulsarAdmin pulsarAdmin;
    public MirrorToUrsaTest() throws PulsarClientException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        adminClient = AdminClient.create(props);
        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build();
    }

    static final String schemaRegistryUrl = "http://localhost:8081";

    final String KAFKA_CLUSTER = "localhost:9092";
    final String URSA_CLUSTER = "localhost:9094";

    private static final String USER_SCHEMA = "{\"type\":\"record\",\"name\":\"User\","
        + "\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
    private IndexedRecord createAvroRecord(String value) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name", value);
        return avroRecord;
    }

    @Test
    public void testProduceMessages() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        @Cleanup
        KafkaProducer<String, Object> producer = new KafkaProducer<>(props);
        String topic = "replicateme";
        final int numMessages = 1000;
        for (int i = 0; i < numMessages; i++) {
            final Object value = createAvroRecord("value-" + i);
            RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic, 0, "key-" + i, value)).get();
            log.info("Produced message: key: " + "key-" + i + "; value: " + value + " to topic " + topic + ", offset: " + recordMetadata.offset());
        }
    }

    @Test
    public void testConsumeMessages() throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, URSA_CLUSTER);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        @Cleanup
        KafkaConsumer<String, Object> kafkaConsumer = new KafkaConsumer<>(props);

        kafkaConsumer.subscribe(Collections.singleton("replicateme"));

        while (true) {
            for (ConsumerRecord<String, Object> record : kafkaConsumer.poll(Duration.ofMillis(100))) {
                log.info("Consumed message: key: " + record.key() + "; value: " + record.value() + " from topic " + record.topic() + ", offset: " + record.offset());
            }
        }
    }

    @Test
    public void testMirrorToUrsa() throws Exception {
        String topic = "testMirrorToUrsa-" + UUID.randomUUID();
        createTopic(topic);

        Properties props = new Properties();
        // Source and destination clusters
        props.setProperty("clusters", "source,destination");
        props.setProperty("source.bootstrap.servers", "localhost:9092");
        props.setProperty("source.consumer.group.id", "mirrormaker2-source-group");
        props.setProperty("source.consumer.auto.offset.reset", "earliest");
        props.setProperty("destination.bootstrap.servers", "localhost:9094");

        // Replication policy
        props.setProperty("replication.policy.separator", "_");
        props.setProperty("replication.policy.class", "org.apache.kafka.connect.mirror.DefaultReplicationPolicy");
        props.setProperty("replication.factor", "1");
        props.setProperty("config.storage.replication.factor", "1");
        props.setProperty("status.storage.replication.factor", "1");
        props.setProperty("offset.storage.replication.factor", "1");
        props.setProperty("checkpoints.topic.replication.factor", "1");
        props.setProperty("heartbeats.topic.replication.factor", "1");
        props.setProperty("offset-syncs.topic.replication.factor", "1");

        // MirrorMaker 2 specific settings
        props.setProperty("tasks.max", "1");
        props.setProperty("source->destination.enabled", "true");
        props.setProperty("destination->source.enabled", "false");
        props.setProperty("groups", ".*");
        props.setProperty("source->destination.topics", topic);
        // props.setProperty("topics.exclude", "*.internal,__.__*"); // Uncomment if needed
        props.setProperty("offset.storage.file.filename", "/tmp/mirrormaker2-offsets.txt");
        props.setProperty("emit.heartbeats.enabled", "false");

        final int numMessages = 10;
        final List<String> values =
            IntStream.range(0, numMessages).mapToObj(i -> "value-" + i).collect(Collectors.toList());
        final List<String> keys = IntStream.range(0, numMessages)
            .mapToObj(i -> "key-" + i)
            .collect(Collectors.toList());
        final Set<Integer> flushIndexes = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(3, 7, 8)));

        final ArrayList<CompletableFuture<Long>> offsetFutures = new ArrayList<>();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        @Cleanup
        Producer<String, String> producer = new KafkaProducer<>(producerProps);
        for (int i = 0; i < numMessages; i++) {
            final String key = keys.get(i);
            final String value = values.get(i);

            final CompletableFuture<Long> future = new CompletableFuture<Long>();
            offsetFutures.add(future);

            producer.send(new ProducerRecord<>(topic, 0, key, value),
                ((recordMetadata, e) -> {
                    if (e == null) {
                        future.complete(recordMetadata.offset());
                    } else {
                        future.completeExceptionally(e);
                    }
                }));
            if (flushIndexes.contains(i)) {
                producer.flush();
            }
        }
        FutureUtil.waitForAll(offsetFutures).get(3, TimeUnit.SECONDS);
        for (int i = 0; i < numMessages; i++) {
            Assertions.assertEquals(offsetFutures.get(i).join().intValue(), i);
        }

        Map<String, String> config = Utils.propsToStringMap(props);
        MirrorMaker unilink = new MirrorMaker(config, null);

        Thread unilinkThread = new Thread(() -> {
            try {
                unilink.start();
            } catch (Exception e) {
                log.error("Error in MirrorMaker", e);
            }
        });
        unilinkThread.start();

        // TODO: We should unload topic manually currently.

        Properties consumerPros = new Properties();
        consumerPros.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        consumerPros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerPros.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        consumerPros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerPros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        @Cleanup
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerPros);
        consumer.subscribe(Collections.singleton(topic));
        final List<ConsumerRecord<String, String>> receivedRecords = receiveRecords(consumer, numMessages);

        Assertions.assertEquals(getValuesFromRecords(receivedRecords), values);
        Assertions.assertEquals(getKeysFromRecords(receivedRecords), keys);

        deleteTopic(topic);
        unilinkThread.interrupt();
    }

    List<ConsumerRecord<String, String>> receiveRecords(final KafkaConsumer<String, String> consumer,
                                                                  int numMessages) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        while (numMessages > 0) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(100))) {
                records.add(record);
                numMessages--;
            }
        }
        return records;
    }
    protected static List<String> getValuesFromRecords(final List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::value).collect(Collectors.toList());
    }

    protected static List<String> getKeysFromRecords(final List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::key).collect(Collectors.toList());
    }
    void createTopic(String topicName) throws Exception{
        int partitions = 1;
        short replicationFactor = 1;
        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
        adminClient.createTopics(Collections.singleton(newTopic)).all().get();
    }

    void deleteTopic(String topicName) {
        adminClient.deleteTopics(Collections.singleton(topicName));
    }
}
