/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.format.EncodeRequest;
import io.streamnative.pulsar.handlers.kop.format.EncodeResult;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatterFactory;
import io.streamnative.pulsar.handlers.kop.storage.PartitionLog;
import io.streamnative.pulsar.handlers.kop.utils.KopLogValidator;
import io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils;
import io.streamnative.ursa.metrics.InstrumentProvider;
import io.streamnative.ursa.storage.FileStorage;
import io.streamnative.ursa.storage.IDGenerator;
import io.streamnative.ursa.storage.WalStorage;
import io.streamnative.ursa.storage.impl.PersistStorageApi;
import io.streamnative.ursa.storage.impl.PulsarStorageConfig;
import io.streamnative.ursa.storage.impl.WalStorageFactory;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.impl.oxia.OxiaMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/** Replicates a set of topic-partitions. */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    @Getter
    @Setter
    public static class ManagedLedgerMetadata {
        public long streamId;
        public Map<String, String> properties;
        public OptionalLong terminatedOffse;

        public ManagedLedgerMetadata() {
            if (properties == null) {
                properties = new TreeMap<>();
            }
        }
    }

    private KafkaConsumer<byte[], byte[]> consumer;
    private String sourceClusterAlias;
    private Duration pollTimeout;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;
    private CompletableFuture<MetadataCache<ManagedLedgerMetadata>> metadataCache;
    private CompletableFuture<PersistStorageApi> storageApi;
    private EntryFormatter entryFormatter;

    public MirrorSourceTask() {}

    private static Pair<String, String> validateOxiaUrl(String metadataURL) throws MetadataStoreException {
        if (metadataURL == null || !metadataURL.startsWith("oxia://")) {
            throw new MetadataStoreException("Invalid metadata URL. Must start with 'oxia://'.");
        }
        final String addressWithNamespace = metadataURL.substring("oxia://".length());
        final String[] split = addressWithNamespace.split("/");
        if (split.length != 2 && split.length != 1) {
            throw new MetadataStoreException("Invalid metadata URL. The oxia metadata format should be "
                + "'oxia://host:port/[namespace]'.");
        }

        return Pair.of(split[0], (split.length > 1) ? split[1] : "default");
    }

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.metrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        consumerAccess = new Semaphore(1);  // let one thread at a time access the consumer
        sourceClusterAlias = config.sourceClusterAlias();
        metrics = config.metrics();
        pollTimeout = config.consumerPollTimeout();
        replicationPolicy = config.replicationPolicy();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }
        consumer = MirrorUtils.newConsumer(config.sourceConsumerConfig("replication-consumer"));
        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        initializeConsumer(taskTopicPartitions);

        try {
            String metadataURL = "oxia://localhost:6648/default"; // TODO: Make it configurable
            final Pair<String, String> oxiaUrl = validateOxiaUrl(metadataURL);
            CompletableFuture<AsyncOxiaClient> oxiaClient = OxiaClientBuilder.create(oxiaUrl.getLeft())
                .namespace(oxiaUrl.getRight()).asyncClient();
            metadataCache = oxiaClient.thenApply(oxia -> {
                    MetadataStore store = new OxiaMetadataStore(oxia, "identity");
                    return store.getMetadataCache(ManagedLedgerMetadata.class);
                });
            storageApi = oxiaClient.thenApply(oxia -> {
                try {
                    PulsarStorageConfig pulsarStorageConfig = new PulsarStorageConfig();
                    // TODO: Make them configurable
                    pulsarStorageConfig.setS3Region("us-east-1");
                    pulsarStorageConfig.setS3Bucket("pulsar-storage");
                    pulsarStorageConfig.setS3Endpoint("http://localhost:4566");
                    pulsarStorageConfig.setS3AccessKeyId("test");
                    pulsarStorageConfig.setS3secretAccessKey("test");
                    FileStorage fileStorage = FileStorage.create(pulsarStorageConfig, InstrumentProvider.NOOP);

                    IDGenerator idGenerator = IDGenerator.create(pulsarStorageConfig.getIdGeneratorType(), "wal", oxia);
                    WalStorage innerStorage =
                        WalStorageFactory.create(pulsarStorageConfig, PulsarByteBufAllocator.DEFAULT, fileStorage,
                            idGenerator, InstrumentProvider.NOOP);
                    return new PersistStorageApi(pulsarStorageConfig, oxia, innerStorage,
                        InstrumentProvider.NOOP);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to create storage API", e);
                }

            });
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to validate Oixa URL", e);
        }
        KafkaServiceConfiguration kafkaConfig = new KafkaServiceConfiguration();
        kafkaConfig.setKafkaEnableUrsaStorage(true);
        entryFormatter = EntryFormatterFactory.create(kafkaConfig, null, "kafka"); // TODO: Make it configurable
        log.info("{} replicating {} topic-partitions {}->{}: {}.", Thread.currentThread().getName(),
            taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
    }

    @Override
    public void commit() {
        // Handle delayed and pending offset syncs only when offsetSyncWriter is available
        if (offsetSyncWriter != null) {
            // Offset syncs which were not emitted immediately due to their offset spacing should be sent periodically
            // This ensures that low-volume topics aren't left with persistent lag at the end of the topic
            offsetSyncWriter.promoteDelayedOffsetSyncs();
            // Publish any offset syncs that we've queued up, but have not yet been able to publish
            // (likely because we previously reached our limit for number of outstanding syncs)
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for access to consumer. Will try closing anyway."); 
        }
        Utils.closeQuietly(consumer, "source consumer");
        Utils.closeQuietly(offsetSyncWriter, "offset sync writer");
        Utils.closeQuietly(metrics, "metrics");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }
   
    @Override
    public String version() {
        return new MirrorSourceConnector().version();
    }

    @Override
    public List<SourceRecord> poll() {
        if (!consumerAccess.tryAcquire()) {
            return null;
        }
        if (stopping) {
            return null;
        }

        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout); // poll messages from the source cluster
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
                metrics.recordBytes(topicPartition, byteSize(record.value()));
            }
            if (sourceRecords.isEmpty()) {
                // WorkerSourceTasks expects non-zero batch size
                return null;
            } else {
                System.out.println("RBT: Received " + sourceRecords.size() + " records from " + records.partitions());
                log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
                writeToUrsa(sourceRecords).join();
                return null;
            }
        } catch (WakeupException e) {
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e)  {
            log.error("Failure during poll.", e);
            // allow Connect to deal with the exception
            throw e;
        } finally {
            consumerAccess.release();
        }
    }

    static class RecordsContext {
        final long firstOffset;
        final MemoryRecordsBuilder builder;
        int numberOfMessages;
        public RecordsContext(long firstOffset, MemoryRecordsBuilder builder, int numberOfMessages) {
            this.firstOffset = firstOffset;
            this.builder = builder;
            this.numberOfMessages = numberOfMessages;
        }
    }

    private static final KopLogValidator.CompressionCodec DEFAULT_COMPRESSION =
        new KopLogValidator.CompressionCodec(CompressionType.NONE.name, CompressionType.NONE.id);

    static Map<TopicPartition, RecordsContext> convertToRecordsContext(List<SourceRecord> sourceRecords) {
        if (sourceRecords.isEmpty()) {
            throw new IllegalArgumentException("SourceRecords list is empty");
        }

        Map<TopicPartition, RecordsContext> recordsContextMap = new HashMap<>();

        for (SourceRecord sourceRecord : sourceRecords) {
            String topic = sourceRecord.topic();
            int partition = sourceRecord.kafkaPartition();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            Object offsetObject = sourceRecord.sourceOffset().get("offset");
            final long offset;

            if (offsetObject instanceof Number) {
                offset = ((Number) offsetObject).longValue();
            } else {
                throw new IllegalArgumentException("Offset is not a number: " + offsetObject);
            }

            recordsContextMap.putIfAbsent(topicPartition, new RecordsContext(offset, MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                offset
            ), 0));

            RecordsContext recordsContext = recordsContextMap.get(topicPartition);
            recordsContext.builder.append(sourceRecord.timestamp(), (byte[]) sourceRecord.key(), (byte[]) sourceRecord.value());
            recordsContext.numberOfMessages++;
        }

        return recordsContextMap;
    }

    static String getPath(final TopicPartition topicPartition) throws InvalidTopicException {
        String pulsarTopic = TopicNameUtils.kafkaToPulsar(topicPartition.toString(),
            "public/default/");
        String mlName = TopicName.get(pulsarTopic).getPersistenceNamingEncoding();
        return "/managed-ledgers/" + mlName;
    }

    CompletableFuture<Void> writeToUrsa(List<SourceRecord> sourceRecords) {
        Map<TopicPartition, RecordsContext> records = convertToRecordsContext(sourceRecords);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Map.Entry<TopicPartition, RecordsContext> entry : records.entrySet()) {
            RecordsContext recordsContext = entry.getValue();
            MemoryRecords memoryRecords = recordsContext.builder.build();
            AtomicInteger shallowMessageCount = new AtomicInteger();
            AtomicInteger validBytesCount = new AtomicInteger();
            AtomicInteger numberOfMessages = new AtomicInteger();
            memoryRecords.batches().forEach(batch -> {
                shallowMessageCount.addAndGet(1);
                validBytesCount.addAndGet(batch.sizeInBytes());
                batch.forEach(record -> {
                    numberOfMessages.addAndGet(1);
                });
            });

            EncodeRequest request = EncodeRequest.get(memoryRecords, new PartitionLog.LogAppendInfo(
                Optional.of(entry.getValue().firstOffset),
                Optional.empty(),
                "",
                RecordBatch.NO_PRODUCER_EPOCH,
                numberOfMessages.get(),
                shallowMessageCount.get(),
                false,
                false,
                validBytesCount.get(),
                (int)entry.getValue().firstOffset,
                (int)entry.getValue().firstOffset + entry.getValue().numberOfMessages,
                DEFAULT_COMPRESSION,
                DEFAULT_COMPRESSION,
                false
            ));
            EncodeResult result = entryFormatter.encode(request);
            CompletableFuture<Void> future = metadataCache.thenCompose(
                cache -> cache.get(getPath(entry.getKey())).thenCompose(metadata -> {
                    if (!metadata.isPresent()) {
                        throw new IllegalStateException("ManagedLedger metadata not found for " + entry.getKey());
                    }
                    return storageApi.thenCompose(storage ->
                        storage.append(metadata.get().streamId, recordsContext.numberOfMessages, result.getEncodedByteBuf()));
                })
            ).thenCompose(entryHeader -> {
                if (entryHeader == null) {
                    throw new IllegalStateException("Failed to append to storage");
                }
                return CompletableFuture.completedFuture(null); // TODO: Invoke commitRecord
            });
            futures.add(future);
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
 
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping) {
            return;
        }
        if (metadata == null) {
            log.debug("No RecordMetadata (source record was probably filtered out during transformation) -- can't sync offsets for {}.", record.topic());
            return;
        }
        if (!metadata.hasOffset()) {
            log.error("RecordMetadata has no offset -- can't sync offsets for {}.", record.topic());
            return;
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        long latency = System.currentTimeMillis() - record.timestamp();
        metrics.countRecord(topicPartition);
        metrics.replicationLatency(topicPartition, latency);
        // Queue offset syncs only when offsetWriter is available
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
            // We may be able to immediately publish an offset sync that we've queued up here
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }
 
    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    // visible for testing
    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        log.info("Starting with {} previously uncommitted partitions.", topicPartitionOffsets.values().stream()
                .filter(this::isUncommitted).count());

        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            // Do not call seek on partitions that don't have an existing offset committed.
            if (isUncommitted(offset)) {
                log.trace("Skipping seeking offset for topicPartition: {}", topicPartition);
                return;
            }
            long nextOffsetToCommittedOffset = offset + 1L;
            log.trace("Seeking to offset {} for topicPartition: {}", nextOffsetToCommittedOffset, topicPartition);
            consumer.seek(topicPartition, nextOffsetToCommittedOffset);
        });
    }

    // visible for testing 
    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
                MirrorUtils.wrapOffset(record.offset()),
                targetTopic, record.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.BYTES_SCHEMA, record.value(),
                record.timestamp(), headers);
    }

    private Headers convertHeaders(ConsumerRecord<byte[], byte[]> record) {
        ConnectHeaders headers = new ConnectHeaders();
        for (Header header : record.headers()) {
            headers.addBytes(header.key(), header.value());
        }
        return headers;
    }

    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }

    private static int byteSize(byte[] bytes) {
        if (bytes == null) {
            return 0;
        } else {
            return bytes.length;
        }
    }

    private boolean isUncommitted(Long offset) {
        return offset == null || offset < 0;
    }
}
