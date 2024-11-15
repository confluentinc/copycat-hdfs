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

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.hdfs.filter.TopicPartitionCommittedFileFilter;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestWithMiniDFSCluster extends HdfsSinkConnectorTestBase {

  protected static FileSystem fs;
  protected static MiniDFSCluster cluster;

  protected DataFileReader dataFileReader;
  protected Partitioner partitioner;
  protected String extension;
  // The default based on default configuration of 10
  protected String zeroPadFormat = "%010d";
  private Map<String, String> localProps = new HashMap<>();

  @Before
  public void setup() throws IOException {
    cluster = createDFSCluster();
    fs = cluster.getFileSystem();
  }

  @After
  public void cleanup() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown(true);
    }
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    url = "hdfs://" + cluster.getNameNode().getClientNamenodeAddress();
    // Override configs using url here
    localProps.put(HdfsSinkConnectorConfig.HDFS_URL_CONFIG, url);
    localProps.put(StorageCommonConfig.STORE_URL_CONFIG, url);
    props.putAll(localProps);
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster.isDataNodeUp() && fs.exists(new Path("/")) && fs.isDirectory(new Path("/"))) {
      for (FileStatus file : fs.listStatus(new Path("/"))) {
        if (file.isDirectory()) {
          fs.delete(file.getPath(), true);
        } else {
          fs.delete(file.getPath(), false);
        }
      }
    }
  }

  /**
   * Return a list of new records starting at zero offset.
   *
   * @param size the number of records to return.
   * @return the list of records.
   */
  protected List<SinkRecord> createSinkRecords(int size) {
    return createSinkRecords(size, 0);
  }

  /**
   * Return a list of new records starting at the given offset.
   *
   * @param size the number of records to return.
   * @param startOffset the starting offset.
   * @return the list of records.
   */
  protected List<SinkRecord> createSinkRecords(int size, long startOffset) {
    return createSinkRecords(size, startOffset, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  /**
   * Return a list of new records for a set of partitions, starting at the given offset in each partition.
   *
   * @param size the number of records to return.
   * @param startOffset the starting offset.
   * @param partitions the set of partitions to create records for.
   * @return the list of records.
   */
  protected List<SinkRecord> createSinkRecords(int size, long startOffset, Set<TopicPartition> partitions) {
    Schema schema = createSchema();
    Struct record = createRecord(schema);
    List<Struct> same = new ArrayList<>();
    for (int i = 0; i < size; ++i) {
      same.add(record);
    }
    return createSinkRecords(same, schema, startOffset, partitions);
  }

  protected List<SinkRecord> createSinkRecords(List<Struct> records, Schema schema) {
    return createSinkRecords(records, schema, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  protected List<SinkRecord> createSinkRecords(List<Struct> records, Schema schema, long startOffset,
                                               Set<TopicPartition> partitions) {
    String key = "key";
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      long offset = startOffset;
      for (Struct record : records) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset++));
      }
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createSinkRecordsNoVersion(int size, long startOffset) {
    String key = "key";
    Schema schemaNoVersion = SchemaBuilder.struct().name("record")
                                 .field("boolean", Schema.BOOLEAN_SCHEMA)
                                 .field("int", Schema.INT32_SCHEMA)
                                 .field("long", Schema.INT64_SCHEMA)
                                 .field("float", Schema.FLOAT32_SCHEMA)
                                 .field("double", Schema.FLOAT64_SCHEMA)
                                 .build();

    Struct recordNoVersion = new Struct(schemaNoVersion);
    recordNoVersion.put("boolean", true)
        .put("int", 12)
        .put("long", 12L)
        .put("float", 12.2f)
        .put("double", 12.2);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset; offset < startOffset + size; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schemaNoVersion,
                                     recordNoVersion, offset));
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createSinkRecordsWithAlternatingSchemas(int size, long startOffset) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);
    Schema newSchema = createNewSchema();
    Struct newRecord = createNewRecord(newSchema);

    int limit = (size / 2) * 2;
    boolean remainder = size % 2 > 0;
    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset; offset < startOffset + limit; ++offset) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, ++offset));
    }
    if (remainder) {
      sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record,
                                     startOffset + size - 1));
    }
    return sinkRecords;
  }

  protected List<SinkRecord> createSinkRecordsInterleaved(int size, long startOffset, Set<TopicPartition> partitions) {
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset, total = 0; total < size; ++offset) {
      for (TopicPartition tp : partitions) {
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
        if (++total >= size) {
          break;
        }
      }
    }
    return sinkRecords;
  }

  // Given a list of records, create a list of sink records with contiguous offsets.
  protected List<SinkRecord> createSinkRecordsWithTimestamp(
      List<Struct> records,
      Schema schema,
      int startOffset,
      long startTime,
      long timeStep
  ) {
    String key = "key";
    ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
    for (int i = 0, offset = startOffset; i < records.size(); ++i, ++offset) {
      sinkRecords.add(new SinkRecord(
          TOPIC,
          PARTITION,
          Schema.STRING_SCHEMA,
          key,
          schema,
          records.get(i),
          offset,
          startTime + offset * timeStep,
          TimestampType.CREATE_TIME
      ));
    }
    return sinkRecords;
  }

  private Schema createLogicalSchema() {
    return SchemaBuilder.struct().name("record").version(1)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .field("date", Date.SCHEMA)
        .field("decimal", Decimal.schema(2))
        .build();
  }

  protected Struct createLogicalStruct() {

    Struct struct = new Struct(createLogicalSchema());
    struct.put("time", Time.toLogical(Time.SCHEMA, 167532));
    struct.put("timestamp", Timestamp.toLogical(Timestamp.SCHEMA, 1675323210));
    struct.put("date", Date.toLogical(Date.SCHEMA, 12345));
    struct.put("decimal", BigDecimal.valueOf(5000, 2));
    return struct;
  }

  private Schema createArraySchema() {
    return SchemaBuilder.struct().name("record").version(1)
        .field("struct_array", SchemaBuilder.array(createSchema()).build())
        .field("int_array", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
        .field("logical_array", SchemaBuilder.array(Date.SCHEMA).build())
        .field("array_array", SchemaBuilder.array(SchemaBuilder.array(createLogicalSchema()).build()).build())
        .build();

  }
  protected Struct createArrayStruct() {

    Struct record = createRecord(createSchema());
    Struct logicalStruct = createLogicalStruct();
    java.util.Date today = new java.util.Date(Instant.now().toEpochMilli());
    Struct struct = new Struct(createArraySchema());

    struct.put("struct_array", Arrays.asList(record, record));
    struct.put("int_array", Arrays.asList(Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3)));
    struct.put("logical_array", Arrays.asList(today, today));
    struct.put("array_array", Arrays.asList(Arrays.asList(logicalStruct, logicalStruct), Arrays.asList(logicalStruct, logicalStruct)));
    return struct;
  }

  protected Struct createNestedStruct() {

    Schema nestedSchema = SchemaBuilder.struct().name("record").version(1)
        .field("struct", createSchema())
        .field("int", Schema.INT32_SCHEMA)
        .field("array", SchemaBuilder.array(createSchema()).build())
        .field("map", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.STRING_SCHEMA).build())
        .build();
    Schema schema = SchemaBuilder.struct().name("record").version(1)
        .field("struct", createLogicalSchema())
        .field("nested", nestedSchema)
        .field("string", Schema.STRING_SCHEMA)
        .field("map", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, createLogicalSchema()).build())
        .build();

    Struct struct = new Struct(schema);
    struct.put("struct", createLogicalStruct());
    Struct nested = new Struct(nestedSchema);
    nested.put("struct", createRecord(createSchema()));
    nested.put("int", 10);
    nested.put("array", Arrays.asList(createRecord(createSchema()), createRecord(createSchema())));
    nested.put("map", ImmutableMap.of("a", "b", "c", "d"));
    struct.put("nested", nested);
    struct.put("string", "test");
    struct.put("map", ImmutableMap.of("s1", createLogicalStruct(), "s2", createLogicalStruct()));

    return struct;
  }

  protected String getDirectory() {
    return getDirectory(TOPIC, PARTITION);
  }

  protected String getDirectory(String topic, int partition) {
    String encodedPartition = "partition=" + String.valueOf(partition);
    return partitioner.generatePartitionedPath(topic, encodedPartition);
  }

  /**
   * Verify files and records are uploaded appropriately.
   *
   * @param sinkRecords a flat list of the records that need to appear in potentially several files
   * in HDFS.
   * @param validOffsets an array containing the offsets that map to uploaded files for a
   * topic-partition. Offsets appear in ascending order, the difference between two consecutive
   * offsets equals the expected size of the file, and last offset is exclusive.
   */
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException {
    verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)), false);
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions)
      throws IOException {
    verify(sinkRecords, validOffsets, partitions, false);
  }

  /**
   * Verify files and records are uploaded appropriately.
   *
   * @param sinkRecords a flat list of the records that need to appear in potentially several *
   * files in HDFS.
   * @param validOffsets an array containing the offsets that map to uploaded files for a
   * topic-partition. Offsets appear in ascending order, the difference between two consecutive
   * offsets equals the expected size of the file, and last offset is exclusive.
   * @param partitions the set of partitions to verify records for.
   */
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                        boolean skipFileListing) throws IOException {
    if (!skipFileListing) {
      verifyFileListing(validOffsets, partitions);
    }

    for (TopicPartition tp : partitions) {
      for (int i = 1, j = 0; i < validOffsets.length; ++i) {
        long startOffset = validOffsets[i - 1];
        long endOffset = validOffsets[i] - 1;

        String topicsDir = this.topicsDir.get(tp.topic());
        String filename = FileUtils.committedFileName(url, topicsDir, getDirectory(tp.topic(), tp.partition()), tp,
                                                      startOffset, endOffset, extension, zeroPadFormat);
        Path path = new Path(filename);
        Collection<Object> records = dataFileReader.readData(connectorConfig.getHadoopConfiguration(), path);

        long size = endOffset - startOffset + 1;
        assertEquals(size, records.size());
        verifyContents(sinkRecords, j, records);
        j += size;
      }
    }
  }

  protected List<String> getExpectedFiles(long[] validOffsets, TopicPartition tp) {
    List<String> expectedFiles = new ArrayList<>();
    for (int i = 1; i < validOffsets.length; ++i) {
      long startOffset = validOffsets[i - 1];
      long endOffset = validOffsets[i] - 1;
      String topicsDir = this.topicsDir.get(tp.topic());
      expectedFiles.add(FileUtils.committedFileName(url, topicsDir, getDirectory(tp.topic(), tp.partition()), tp,
                                                    startOffset, endOffset, extension, zeroPadFormat));
    }
    return expectedFiles;
  }

  protected void verifyFileListing(long[] validOffsets, Set<TopicPartition> partitions) throws IOException {
    for (TopicPartition tp : partitions) {
      verifyFileListing(getExpectedFiles(validOffsets, tp), tp);
    }
  }

  protected void verifyFileListing(List<String> expectedFiles, TopicPartition tp) throws IOException {
    FileStatus[] statuses = {};
    try {
      String topicsDir = this.topicsDir.get(tp.topic());
      statuses = fs.listStatus(
          new Path(FileUtils.directoryName(url, topicsDir, getDirectory(tp.topic(), tp.partition()))),
          new TopicPartitionCommittedFileFilter(tp));
    } catch (FileNotFoundException e) {
      // the directory does not exist.
    }

    List<String> actualFiles = new ArrayList<>();
    for (FileStatus status : statuses) {
      actualFiles.add(status.getPath().toString());
    }

    Collections.sort(actualFiles);
    Collections.sort(expectedFiles);
    assertThat(actualFiles, is(expectedFiles));
  }

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
    Schema expectedSchema = null;
    for (Object avroRecord : records) {
      if (expectedSchema == null) {
        expectedSchema = expectedRecords.get(startIndex).valueSchema();
      }
      Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
                                                     expectedRecords.get(startIndex++).value(),
                                                     expectedSchema);
      assertEquals(avroData.fromConnectData(expectedSchema, expectedValue), avroRecord);
    }
  }

  private static MiniDFSCluster createDFSCluster() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration())
        .hosts(new String[]{"localhost", "localhost", "localhost"})
        .nameNodePort(9001)
        .numDataNodes(3)
        .build();
    cluster.waitActive();

    return cluster;
  }
  protected int getFileSystemCacheSize() throws Exception {
    Field cacheField = FileSystem.class.getDeclaredField("CACHE");
    cacheField.setAccessible(true);
    Object cache = cacheField.get(Object.class);
    Field cacheMapField = cache.getClass().getDeclaredField("map");
    cacheMapField.setAccessible(true);
    //suppressing the warning since org.apache.hadoop.fs.FileSystem.Cache.Key has package-level visibility
    @SuppressWarnings("rawtypes") Map cacheMap = (Map) cacheMapField.get(cache);
    cacheField.setAccessible(false);
    cacheMapField.setAccessible(false);
    return cacheMap.size();
  }

  protected void writeAndVerify(List<SinkRecord> sinkRecords) throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);


    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    List<Long> validOffsets = new ArrayList<>();
    int flushSize = connectorConfig.getInt(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG);
    for (long i = 0; i < sinkRecords.size(); i += flushSize) validOffsets.add(i);
    verify(sinkRecords, validOffsets.stream().mapToLong(l -> l).toArray());
  }

}
