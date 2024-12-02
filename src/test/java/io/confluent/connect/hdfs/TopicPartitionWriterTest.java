/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.MockTime;
import io.confluent.connect.hdfs.avro.AvroDataFileReader;
import io.confluent.connect.hdfs.filter.CommittedFileFilter;
import io.confluent.connect.hdfs.partitioner.DefaultPartitioner;
import io.confluent.connect.hdfs.partitioner.FieldPartitioner;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.partitioner.TimeBasedPartitioner;
import io.confluent.connect.hdfs.partitioner.TimeUtils;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.hdfs.wal.FSWAL;
import io.confluent.connect.hdfs.wal.WALFile.Writer;
import io.confluent.connect.hdfs.wal.WALFileTest;
import io.confluent.connect.hdfs.wal.WALFileTest.CorruptWriter;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.wal.WAL;

import static io.confluent.connect.storage.StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG;
import static io.confluent.connect.storage.StorageSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG;
import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TopicPartitionWriterTest extends TestWithMiniDFSCluster {
  private RecordWriterProvider writerProvider = null;
  private io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig>
      newWriterProvider;
  private HdfsStorage storage;
  private Map<String, String> localProps = new HashMap<>();
  private MockTime time;

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    time = new MockTime();

    @SuppressWarnings("unchecked")
    Class<? extends HdfsStorage> storageClass = (Class<? extends HdfsStorage>)
        connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
    storage = StorageFactory.createStorage(
        storageClass,
        HdfsSinkConnectorConfig.class,
        connectorConfig,
        url
    );
    @SuppressWarnings("unchecked")
    Class<io.confluent.connect.storage.format.Format> formatClass
        = (Class<io.confluent.connect.storage.format.Format>) connectorConfig.getClass(
        HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG
    );
    @SuppressWarnings("unchecked")
    io.confluent.connect.storage.format.Format<HdfsSinkConnectorConfig, Path> format
        = formatClass.getConstructor(HdfsStorage.class).newInstance(storage);
    writerProvider = null;
    newWriterProvider = format.getRecordWriterProvider();
    dataFileReader = new AvroDataFileReader();
    extension = newWriterProvider.getExtension();
    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    createTopicDir(url, topicsDir, TOPIC);
    createLogsDir(url, logsDir);
  }

  @Test
  public void testVariablyIncreasingOffsets() throws Exception {
    setUp();
    Partitioner partitioner = new FieldPartitioner();
    partitioner.configure(parsedConfig);

    @SuppressWarnings("unchecked")
    List<String> partitionFields = (List<String>) parsedConfig.get(
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG
    );
    String partitionField = partitionFields.get(0);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    String key = "key";
    List<SinkRecord> sinkRecords = new ArrayList<>();

    Schema schema = createSchema();
    List<Struct> records = new ArrayList<>();
    int offset = 0;
    for (int i = 0; i < 3; ++i) {
      for (int j = 0; j < 3; ++j) {
        Struct record = createRecord(schema, j, 12.2f);
        records.add(record);
        sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
        offset += 10;
      }
    }
    // Add a single records at the end of the batches sequence
    Struct struct = createRecord(schema);
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, struct, offset));
    records.add(struct);
    assertEquals(10, records.size());

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    assertEquals(-1, topicPartitionWriter.offset());
    topicPartitionWriter.recover();
    assertEquals(-1, topicPartitionWriter.offset());

    topicPartitionWriter.write();
    // Flush size is 3, so records with offset 0-80 inclusive are written, and 81 is the next one
    // after the last committed
    assertEquals(81, topicPartitionWriter.offset());
    topicPartitionWriter.close();
    assertEquals(81, topicPartitionWriter.offset());

    Set<Path> expectedFiles = new HashSet<>();
    for (int i = 0; i < records.size() - 1; i++) {
      String directory = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + records.get(i).get("int"));
      expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir.get(TOPIC), directory, TOPIC_PARTITION, i * 10, i * 10, extension, zeroPadFormat)));
    }

    records.sort(Comparator.comparingInt(s -> (int) s.get("int")));
    int expectedBatchSize = 1;
    verify(expectedFiles, expectedBatchSize, records, schema);

    // Try recovering at this point, and check that we've not lost our committed offsets
    topicPartitionWriter.recover();
    assertEquals(81, topicPartitionWriter.offset());
  }

  @Test
  public void testWriteRecordDefaultWithPadding() throws Exception {
    localProps.put(HdfsSinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
    setUp();
    Partitioner partitioner = new DefaultPartitioner();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);
    // Add a single records at the end of the batches sequence. Total records: 10
    records.add(createRecord(schema));
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    Set<Path> expectedFiles = new HashSet<>();
    String topicsDir = this.topicsDir.get(TOPIC);
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
                               "/" + TOPIC + "+" + PARTITION + "+00+02" + extension));
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
                               "/" + TOPIC + "+" + PARTITION + "+03+05" + extension));
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
                               "/" + TOPIC + "+" + PARTITION + "+06+08" + extension));
    int expectedBatchSize = 3;
    verify(expectedFiles, expectedBatchSize, records, schema);
  }

  @Test
  public void testWriteRecordDefaultWithPaddingCorruptRecovery() throws Exception {
    localProps.put(HdfsSinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
    setUp();

    Partitioner partitioner = new DefaultPartitioner();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    //create a corrupt WAL
    WAL wal = new FSWAL(logsDir, TOPIC_PARTITION, storage) {
      public void acquireLease() throws ConnectException {
        super.acquireLease();
        // initialize a new writer if the writer is not a CorruptWriter
        if (writer.getClass() != WALFileTest.CorruptWriter.class) {
          try {
            writer = new CorruptWriter(storage.conf(), Writer.file(new Path(this.getLogFile())),
                Writer.appendIfExists(true));
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    };
    wal.append(WAL.beginMarker, "");

    // Write enough bytes to trigger a sync
    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    for (int i = 0; i < 20; ++i) {
      long startOffset = i * 10;
      long endOffset = (i + 1) * 10 - 1;
      String tempfile = FileUtils.tempFileName(url, topicsDir, partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION), extension);
      fs.createNewFile(new Path(tempfile));
      String committedFile = FileUtils.committedFileName(url, topicsDir, partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION), TOPIC_PARTITION, startOffset,
          endOffset, extension, zeroPadFormat);
      wal.append(tempfile, committedFile);
    }
    wal.append(WAL.endMarker, "");
    wal.close();

    topicPartitionWriter.recover();

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);
    // Add a single records at the end of the batches sequence. Total records: 10
    records.add(createRecord(schema));
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.write();
    topicPartitionWriter.close();

    Set<Path> expectedFiles = new HashSet<>();
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
        "/" + TOPIC + "+" + PARTITION + "+00+02" + extension));
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
        "/" + TOPIC + "+" + PARTITION + "+03+05" + extension));
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
        "/" + TOPIC + "+" + PARTITION + "+06+08" + extension));
    int expectedBatchSize = 3;
    verify(expectedFiles, expectedBatchSize, records, schema);
  }

  @Test
  public void testCloseMultipleTempFiles() throws Exception {
    setUp();
    Partitioner partitioner = new FieldPartitioner();
    partitioner.configure(parsedConfig);

    properties.put(FLUSH_SIZE_CONFIG, "10");
    connectorConfig = new HdfsSinkConnectorConfig(properties);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    Schema schema = createSchema();
    List<Struct> records = new ArrayList<>();
    for (int i = 16; i < 19; ++i) {
      for (int j = 0; j < 2; ++j) {
        records.add(createRecord(schema, i, 12.2f));
      }
    }

    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    assertEquals(-1, topicPartitionWriter.offset());
    topicPartitionWriter.write();
    // Flush size is 10, so records not committed yet
    assertEquals(0, topicPartitionWriter.offset());

    // should not throw
    topicPartitionWriter.close();
  }

  @Test
  public void testWriteRecordFieldPartitioner() throws Exception {
    setUp();
    Partitioner partitioner = new FieldPartitioner();
    partitioner.configure(parsedConfig);

    @SuppressWarnings("unchecked")
    List<String> partitionFields = (List<String>) parsedConfig.get(
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG
    );
    String partitionField = partitionFields.get(0);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    Schema schema = createSchema();
    List<Struct> records = new ArrayList<>();
    for (int i = 16; i < 19; ++i) {
      for (int j = 0; j < 3; ++j) {
        records.add(createRecord(schema, i, 12.2f));

      }
    }
    // Add a single records at the end of the batches sequence
    records.add(createRecord(schema));
    assertEquals(10, records.size());
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    assertEquals(-1, topicPartitionWriter.offset());

    topicPartitionWriter.recover();
    assertEquals(-1, topicPartitionWriter.offset());
    topicPartitionWriter.write();
    // Flush size is 3, so records with offset 0-8 inclusive are written, and 9 is the next one
    // after the last committed
    assertEquals(9, topicPartitionWriter.offset());
    topicPartitionWriter.close();
    assertEquals(9, topicPartitionWriter.offset());

    String directory1 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(16));
    String directory2 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(17));
    String directory3 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(18));

    Set<Path> expectedFiles = new HashSet<>();
    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory1, TOPIC_PARTITION, 0, 2, extension, zeroPadFormat)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory2, TOPIC_PARTITION, 3, 5, extension, zeroPadFormat)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory3, TOPIC_PARTITION, 6, 8, extension, zeroPadFormat)));

    int expectedBatchSize = 3;
    verify(expectedFiles, expectedBatchSize, records, schema);

    // Try recovering at this point, and check that we've not lost our committed offsets
    topicPartitionWriter.recover();
    assertEquals(9, topicPartitionWriter.offset());
  }

  @Test
  public void testWriteRecordTimeBasedPartition() throws Exception {
    setUp();
    Partitioner partitioner = new TimeBasedPartitioner();
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);
    // Add a single records at the end of the batches sequence. Total records: 10
    records.add(createRecord(schema));
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    long partitionDurationMs = (Long) parsedConfig.get(
        PartitionerConfig.PARTITION_DURATION_MS_CONFIG
    );
    String pathFormat = (String) parsedConfig.get(PartitionerConfig.PATH_FORMAT_CONFIG);
    String timeZoneString = (String) parsedConfig.get(PartitionerConfig.TIMEZONE_CONFIG);
    long timestamp = System.currentTimeMillis();

    String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZoneString, timestamp);

    String directory = partitioner.generatePartitionedPath(TOPIC, encodedPartition);

    Set<Path> expectedFiles = new HashSet<>();
    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, 0, 2, extension, zeroPadFormat)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, 3, 5, extension, zeroPadFormat)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, 6, 8, extension, zeroPadFormat)));

    int expectedBatchSize = 3;
    verify(expectedFiles, expectedBatchSize, records, schema);
  }

  @Test
  public void testWriteRecordTimeBasedPartitionFieldTimestampHours() throws Exception {
    // Do not roll on size, only based on time.
    localProps.put(FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(1))
    );
    setUp();

    // Define the partitioner
    partitioner = new DataWriter.PartitionerWrapper(new HourlyPartitioner<FieldSchema>());
    parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "RecordField");
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    Schema schema = createSchemaWithTimestampField();

    DateTime first = new DateTime(2017, 3, 2, 10, 0, DateTimeZone.forID("America/Los_Angeles"));
    // One record every 20 sec, puts 3 records every minute/rotate interval
    long advanceMs = 20000;
    long timestampFirst = first.getMillis();
    int size = 18;

    ArrayList<Struct> records = new ArrayList<>(size);
    for (int i = 0; i < size/2; ++i) {
      records.add(createRecordWithTimestampField(schema, timestampFirst));
      timestampFirst += advanceMs;
    }
    Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 9), schema);

    long timestampLater = first.plusHours(2).getMillis();
    for (int i = size/2; i < size; ++i) {
      records.add(createRecordWithTimestampField(schema, timestampLater));
      timestampLater += advanceMs;
    }
    sinkRecords.addAll(createSinkRecords(
        records.subList(9, 18),
        schema,
        9,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION))
    ));

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    assertEquals(0, topicPartitionWriter.offset());
    time.sleep(TimeUnit.MINUTES.toMillis(2));
    boolean shouldRotateAndMaybeUpdateTimers = topicPartitionWriter.shouldRotateAndMaybeUpdateTimers();
    assertEquals(true, shouldRotateAndMaybeUpdateTimers);
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
    String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    Set<Path> expectedFiles = new HashSet<>();
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, dirPrefixFirst, TOPIC_PARTITION, 0, 8, extension, zeroPadFormat)));
    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, dirPrefixLater, TOPIC_PARTITION, 9, 17, extension, zeroPadFormat)));
    verify(expectedFiles, 9, records, schema);
  }

  @Test
  public void testWriteRecordTimeBasedPartitionRecordTimestampHours() throws Exception {
    // Do not roll on size, only based on time.
    localProps.put(FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(1))
    );
    setUp();

    // Define the partitioner
    partitioner = new DataWriter.PartitionerWrapper(new HourlyPartitioner<FieldSchema>());
    parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    DateTime first = new DateTime(2017, 3, 2, 10, 0, DateTimeZone.forID("America/Los_Angeles"));
    // One record every 20 sec, puts 3 records every minute/rotate interval
    long advanceMs = 20000;
    long timestampFirst = first.getMillis();
    Collection<SinkRecord> sinkRecords = createSinkRecordsWithTimestamp(records.subList(0, 9), schema, 0, timestampFirst, advanceMs);
    long timestampLater = first.plusHours(2).getMillis();
    sinkRecords.addAll(createSinkRecordsWithTimestamp(records.subList(9, 18), schema, 9, timestampLater, advanceMs));

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    time.sleep(TimeUnit.MINUTES.toMillis(2));
    boolean shouldRotateAndMaybeUpdateTimers = topicPartitionWriter.shouldRotateAndMaybeUpdateTimers();
    assertEquals(true, shouldRotateAndMaybeUpdateTimers);
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
    String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    Set<Path> expectedFiles = new HashSet<>();
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, dirPrefixFirst, TOPIC_PARTITION, 0, 8, extension, zeroPadFormat)));

    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, dirPrefixLater, TOPIC_PARTITION, 9, 17, extension, zeroPadFormat)));
    verify(expectedFiles, 9, records, schema);
  }

  @Test
  public void testWriteRecordTimeBasedPartitionRecordTimestampDays() throws Exception {
    // Do not roll on size, only based on time.
    localProps.put(FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(1))
    );
    setUp();

    // Define the partitioner
    partitioner = new DataWriter.PartitionerWrapper(new DailyPartitioner<FieldSchema>());
    parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
    parsedConfig.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY/'month'=MM/'day'=dd");
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    DateTime first = new DateTime(2017, 3, 2, 10, 0, DateTimeZone.forID("America/Los_Angeles"));
    // One record every 20 sec, puts 3 records every minute/rotate interval
    long advanceMs = 20000;
    long timestampFirst = first.getMillis();
    Collection<SinkRecord> sinkRecords = createSinkRecordsWithTimestamp(records.subList(0, 9), schema, 0, timestampFirst, advanceMs);
    long timestampLater = first.plusDays(1).getMillis();
    sinkRecords.addAll(createSinkRecordsWithTimestamp(records.subList(9, 18), schema, 9, timestampLater, advanceMs));

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    time.sleep(TimeUnit.MINUTES.toMillis(2));
    boolean shouldRotateAndMaybeUpdateTimers = topicPartitionWriter.shouldRotateAndMaybeUpdateTimers();
    assertEquals(true, shouldRotateAndMaybeUpdateTimers);
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
    String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);

    Set<Path> expectedFiles = new HashSet<>();
    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, dirPrefixFirst, TOPIC_PARTITION, 0, 8, extension, zeroPadFormat)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, dirPrefixLater, TOPIC_PARTITION, 9, 17, extension, zeroPadFormat)));

    int expectedBatchSize = 9;
    verify(expectedFiles, expectedBatchSize, records, schema);
  }

  @Test
  public void testCloseTempFileByRotateIntervalWithoutReadingNewRecords() throws Exception {
    setUp();
    // Define the partitioner
    partitioner = new DataWriter.PartitionerWrapper(
            new io.confluent.connect.storage.partitioner.TimeBasedPartitioner<>()
    );
    parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
    parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
    parsedConfig.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'ds'=YYYYMMdd");
    partitioner.configure(parsedConfig);

    properties.put(FLUSH_SIZE_CONFIG, "1000");
    properties.put(
            ROTATE_INTERVAL_MS_CONFIG,
            String.valueOf(TimeUnit.MINUTES.toMillis(1))
    );
    connectorConfig = new HdfsSinkConnectorConfig(properties);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
            TOPIC_PARTITION,
            storage,
            writerProvider,
            newWriterProvider,
            partitioner,
            connectorConfig,
            context,
            avroData,
            time
    );

    Schema schema = createSchema();
    List<Struct> records = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      records.add(createRecord(schema, i, 12.2f));
    }

    long advanceMs = 20000;
    DateTime first = new DateTime(2023, 5, 7, 9, 0, DateTimeZone.UTC);
    long timestamp = first.getMillis();
    Collection<SinkRecord> sinkRecords = createSinkRecordsWithTimestamp(
            records, schema, 0, timestamp, advanceMs
    );
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    assertEquals(-1, topicPartitionWriter.offset());
    topicPartitionWriter.write();
    assertEquals(0, topicPartitionWriter.offset());
    time.sleep(TimeUnit.MINUTES.toMillis(2));
    boolean shouldRotateAndMaybeUpdateTimers = topicPartitionWriter.shouldRotateAndMaybeUpdateTimers();
    assertEquals(true, shouldRotateAndMaybeUpdateTimers);
    topicPartitionWriter.write();
    assertEquals(10, topicPartitionWriter.offset());
    topicPartitionWriter.close();

    Set<Path> expectedFiles = new HashSet<>();
    String topicsDir = this.topicsDir.get(TOPIC);
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/ds=20230507" +
            "/" + TOPIC + "+" + PARTITION + "+0000000000+0000000009" + extension));
    int expectedBatchSize = 10;
    verify(expectedFiles, expectedBatchSize, records, schema);
  }

  @Test
  public void testWriteRecordTimeBasedPartitionWallclockMockedWithScheduleRotation()
      throws Exception {
    // Do not roll on size, only based on time.
    localProps.put(FLUSH_SIZE_CONFIG, "1000");
    localProps.put(
        ROTATE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.HOURS.toMillis(1))
    );
    localProps.put(
        HdfsSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
        String.valueOf(TimeUnit.MINUTES.toMillis(10))
    );
    setUp();

    // Define the partitioner
    partitioner = new DataWriter.PartitionerWrapper(
        new io.confluent.connect.storage.partitioner.TimeBasedPartitioner<FieldSchema>()
    );
    parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
    parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockedWallclockTimestampExtractor.class.getName());
    partitioner.configure(parsedConfig);

    MockedWallclockTimestampExtractor.TIME.sleep(SYSTEM.milliseconds());

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        MockedWallclockTimestampExtractor.TIME
    );

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 6);
    Collection<SinkRecord> sinkRecords = createSinkRecords(
        records.subList(0, 3),
        schema,
        0,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION))
    );
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    long timestampFirst = MockedWallclockTimestampExtractor.TIME.milliseconds();

    // 11 minutes
    MockedWallclockTimestampExtractor.TIME.sleep(TimeUnit.MINUTES.toMillis(11));
    // Records are written due to scheduled rotation
    topicPartitionWriter.write();

    sinkRecords = createSinkRecords(
        records.subList(3, 6),
        schema,
        3,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION))
    );
    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    // More records later
    topicPartitionWriter.write();
    long timestampLater = MockedWallclockTimestampExtractor.TIME.milliseconds();

    // 11 minutes later, another scheduled rotation
    MockedWallclockTimestampExtractor.TIME.sleep(TimeUnit.MINUTES.toMillis(11));

    // Again the records are written due to scheduled rotation
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
    String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

    String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    Set<Path> expectedFiles = new HashSet<>();
    for (int i : new int[]{0}) {
      expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, i + 2, extension, zeroPadFormat)));
    }

    String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
    for (int i : new int[]{3}) {
      expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, dirPrefixLater, TOPIC_PARTITION, i, i + 2, extension, zeroPadFormat)));
    }
    verify(expectedFiles, 3, records, schema);
  }

  private String getTimebasedEncodedPartition(long timestamp) {
    long partitionDurationMs = (Long) parsedConfig.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
    String pathFormat = (String) parsedConfig.get(PartitionerConfig.PATH_FORMAT_CONFIG);
    String timeZone = (String) parsedConfig.get(PartitionerConfig.TIMEZONE_CONFIG);
    return TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZone, timestamp);
  }

  private void createTopicDir(String url, String topicsDir, String topic) throws IOException {
    Path path = new Path(FileUtils.topicDirectory(url, topicsDir, topic));
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  private void createLogsDir(String url, String logsDir) throws IOException {
    Path path = new Path(url + "/" + logsDir);
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  private void verify(Set<Path> expectedFiles, int expectedSize, List<Struct> records, Schema schema) throws IOException {
    String topicsDir = this.topicsDir.get(TOPIC);
    Path path = new Path(FileUtils.topicDirectory(url, topicsDir, TOPIC));
    FileStatus[] statuses = FileUtils.traverse(storage, path, new CommittedFileFilter());
    assertEquals(expectedFiles.size(), statuses.length);
    int index = 0;
    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      assertTrue(expectedFiles.contains(status.getPath()));
      Collection<Object> avroRecords = dataFileReader.readData(connectorConfig.getHadoopConfiguration(), filePath);
      assertEquals(expectedSize, avroRecords.size());
      for (Object avroRecord : avroRecords) {
        assertEquals(avroData.fromConnectData(schema, records.get(index++)), avroRecord);
      }
    }
  }

  public static class MockedWallclockTimestampExtractor
      extends
      io.confluent.connect.storage.partitioner.TimeBasedPartitioner.WallclockTimestampExtractor{
    public static final MockTime TIME = new MockTime();

    @Override
    public void configure(Map<String, Object> config) {}

    @Override
    public Long extract(ConnectRecord<?> record) {
      return TIME.milliseconds();
    }
  }
}
