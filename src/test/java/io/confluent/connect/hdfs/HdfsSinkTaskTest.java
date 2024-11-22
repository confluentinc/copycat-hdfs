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

import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.avro.AvroDataFileReader;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.wal.WAL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HdfsSinkTaskTest extends TestWithMiniDFSCluster {

  private static final String DIRECTORY1 = TOPIC + "/" + "partition=" + String.valueOf(PARTITION);
  private static final String DIRECTORY2 = TOPIC + "/" + "partition=" + String.valueOf(PARTITION2);
  private static final String extension = ".avro";
  private static final String ZERO_PAD_FMT = "%010d";
  private final DataFileReader schemaFileReader = new AvroDataFileReader();

  @Test
  public void testSinkTaskStart() throws Exception {
    setUp();
    createCommittedFiles();
    HdfsSinkTask task = new HdfsSinkTask();

    task.initialize(context);
    task.start(properties);

    Map<TopicPartition, Long> offsets = context.offsets();
    assertEquals(offsets.size(), 2);
    assertTrue(offsets.containsKey(TOPIC_PARTITION));
    assertEquals(21, (long) offsets.get(TOPIC_PARTITION));
    assertTrue(offsets.containsKey(TOPIC_PARTITION2));
    assertEquals(46, (long) offsets.get(TOPIC_PARTITION2));

    task.stop();
  }

  @Test
  public void testSinkTaskFileSystemIsolation() throws Exception {
    // Shutdown of one task should not affect another task
    setUp();
    createCommittedFiles();

    // Generate two rounds of data two write at separate times
    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);
    Collection<SinkRecord> sinkRecordsA = new ArrayList<>();
    Collection<SinkRecord> sinkRecordsB = new ArrayList<>();
    for (TopicPartition tp : context.assignment()) {
      for (long offset = 0; offset < 7; offset++) {
        SinkRecord sinkRecord =
            new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record,
                offset);
        sinkRecordsA.add(sinkRecord);
      }
      for (long offset = 7; offset < 16; offset++) {
        SinkRecord sinkRecord =
            new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record,
                offset);
        sinkRecordsB.add(sinkRecord);
      }
    }

    HdfsSinkTask task = new HdfsSinkTask();
    task.initialize(context);
    task.start(properties);
    task.put(sinkRecordsA);

    // Get an aliased reference to the filesystem object from the per-worker FileSystem.CACHE
    // Close it to induce exceptions when aliased FileSystem objects are used after closing.
    // Paths within this filesystem (such as the WAL) will also share the same FileSystem object
    // because the cache is keyed on uri.getScheme() and uri.getAuthority().
    FileSystem.get(
        new URI(connectorConfig.getString(HdfsSinkConnectorConfig.HDFS_URL_CONFIG)),
        connectorConfig.getHadoopConfiguration()
    ).close();

    // If any FileSystem-based resources are kept in-use between put calls, they should generate
    // exceptions on a subsequent put. These exceptions must not affect the correctness of the task.
    task.put(sinkRecordsB);
    task.stop();

    // Verify that the data arrived correctly
    AvroData avroData = task.getAvroData();
    // Last file (offset 15) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {-1, 2, 5, 8, 11, 14};

    for (TopicPartition tp : context.assignment()) {
      String directory = tp.topic() + "/" + "partition=" + String.valueOf(tp.partition());
      for (int j = 1; j < validOffsets.length; ++j) {
        long startOffset = validOffsets[j - 1] + 1;
        long endOffset = validOffsets[j];
        String topicsDir = this.topicsDir.get(tp.topic());
        Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory, tp,
            startOffset, endOffset, extension,
            ZERO_PAD_FMT));
        Collection<Object> records = schemaFileReader.readData(connectorConfig.getHadoopConfiguration(), path);
        long size = endOffset - startOffset + 1;
        assertEquals(records.size(), size);
        for (Object avroRecord : records) {
          assertEquals(avroRecord, avroData.fromConnectData(schema, record));
        }
      }
    }
  }

  @Test
  public void testSinkTaskStartNoCommittedFiles() throws Exception {
    setUp();
    HdfsSinkTask task = new HdfsSinkTask();

    task.initialize(context);
    task.start(properties);

    // Without any files in HDFS, we expect no offsets to be set by the connector.
    // Thus, the consumer will start where it last left off or based upon the
    // 'auto.offset.reset' setting, which Connect defaults to 'earliest'.
    Map<TopicPartition, Long> offsets = context.offsets();
    assertEquals(0, offsets.size());

    task.stop();
  }

  @Test
  public void testSinkTaskStartSomeCommittedFiles() throws Exception {
    setUp();

    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    Map<TopicPartition, List<String>> tempfiles = new HashMap<>();
    List<String> list1 = new ArrayList<>();
    list1.add(FileUtils.tempFileName(url, topicsDir, DIRECTORY1, extension));
    list1.add(FileUtils.tempFileName(url, topicsDir, DIRECTORY1, extension));
    tempfiles.put(TOPIC_PARTITION, list1);

    Map<TopicPartition, List<String>> committedFiles = new HashMap<>();
    List<String> list3 = new ArrayList<>();
    list3.add(FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 100, 200,
        extension, ZERO_PAD_FMT));
    list3.add(FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 201, 300,
        extension, ZERO_PAD_FMT));
    committedFiles.put(TOPIC_PARTITION, list3);

    for (TopicPartition tp : tempfiles.keySet()) {
      for (String file : tempfiles.get(tp)) {
        fs.createNewFile(new Path(file));
      }
    }

    createWALs(tempfiles, committedFiles);
    HdfsSinkTask task = new HdfsSinkTask();

    task.initialize(context);
    task.start(properties);

    // Without any files in HDFS, we expect no offsets to be set by the connector.
    // Thus, the consumer will start where it last left off or based upon the
    // 'auto.offset.reset' setting, which Connect defaults to 'earliest'.
    Map<TopicPartition, Long> offsets = context.offsets();
    assertEquals(1, offsets.size());
    assertTrue(offsets.containsKey(TOPIC_PARTITION));
    assertEquals(301, (long) offsets.get(TOPIC_PARTITION));

    task.stop();
  }

  @Test
  public void testSinkTaskStartWithRecovery() throws Exception {
    setUp();

    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    Map<TopicPartition, List<String>> tempfiles = new HashMap<>();
    List<String> list1 = new ArrayList<>();
    list1.add(FileUtils.tempFileName(url, topicsDir, DIRECTORY1, extension));
    list1.add(FileUtils.tempFileName(url, topicsDir, DIRECTORY1, extension));
    tempfiles.put(TOPIC_PARTITION, list1);

    topicsDir = this.topicsDir.get(TOPIC_PARTITION2.topic());
    List<String> list2 = new ArrayList<>();
    list2.add(FileUtils.tempFileName(url, topicsDir, DIRECTORY2, extension));
    list2.add(FileUtils.tempFileName(url, topicsDir, DIRECTORY2, extension));
    tempfiles.put(TOPIC_PARTITION2, list2);

    topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    Map<TopicPartition, List<String>> committedFiles = new HashMap<>();
    List<String> list3 = new ArrayList<>();
    list3.add(FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 100, 200,
                                          extension, ZERO_PAD_FMT));
    list3.add(FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 201, 300,
                                          extension, ZERO_PAD_FMT));
    committedFiles.put(TOPIC_PARTITION, list3);

    topicsDir = this.topicsDir.get(TOPIC_PARTITION2.topic());
    List<String> list4 = new ArrayList<>();
    list4.add(FileUtils.committedFileName(url, topicsDir, DIRECTORY2, TOPIC_PARTITION2, 400, 500,
                                          extension, ZERO_PAD_FMT));
    list4.add(FileUtils.committedFileName(url, topicsDir, DIRECTORY2, TOPIC_PARTITION2, 501, 800,
                                          extension, ZERO_PAD_FMT));
    committedFiles.put(TOPIC_PARTITION2, list4);

    for (TopicPartition tp : tempfiles.keySet()) {
      for (String file : tempfiles.get(tp)) {
        fs.createNewFile(new Path(file));
      }
    }

    createWALs(tempfiles, committedFiles);
    HdfsSinkTask task = new HdfsSinkTask();

    task.initialize(context);
    task.start(properties);

    Map<TopicPartition, Long> offsets = context.offsets();
    assertEquals(2, offsets.size());
    assertTrue(offsets.containsKey(TOPIC_PARTITION));
    assertEquals(301, (long) offsets.get(TOPIC_PARTITION));
    assertTrue(offsets.containsKey(TOPIC_PARTITION2));
    assertEquals(801, (long) offsets.get(TOPIC_PARTITION2));

    task.stop();
  }

  @Test
  public void testSinkTaskPut() throws Exception {
    setUp();
    HdfsSinkTask task = new HdfsSinkTask();

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);
    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : context.assignment()) {
      for (long offset = 0; offset < 7; offset++) {
        SinkRecord sinkRecord =
            new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
        sinkRecords.add(sinkRecord);
      }
    }
    task.initialize(context);
    task.start(properties);
    task.put(sinkRecords);
    task.stop();

    AvroData avroData = task.getAvroData();
    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {-1, 2, 5};

    for (TopicPartition tp : context.assignment()) {
      String directory = tp.topic() + "/" + "partition=" + String.valueOf(tp.partition());
      for (int j = 1; j < validOffsets.length; ++j) {
        long startOffset = validOffsets[j - 1] + 1;
        long endOffset = validOffsets[j];
        String topicsDir = this.topicsDir.get(tp.topic());
        Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory, tp,
                                                         startOffset, endOffset, extension,
                                                         ZERO_PAD_FMT));
        Collection<Object> records = schemaFileReader.readData(connectorConfig.getHadoopConfiguration(), path);
        long size = endOffset - startOffset + 1;
        assertEquals(records.size(), size);
        for (Object avroRecord : records) {
          assertEquals(avroRecord, avroData.fromConnectData(schema, record));
        }
      }
    }
  }

  @Test
  public void testSinkTaskPutPrimitive() throws Exception {
    setUp();
    HdfsSinkTask task = new HdfsSinkTask();

    final String key = "key";
    final Schema schema = Schema.INT32_SCHEMA;
    final int record = 12;
    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : context.assignment()) {
      for (long offset = 0; offset < 7; offset++) {
        SinkRecord sinkRecord =
                new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset);
        sinkRecords.add(sinkRecord);
      }
    }
    task.initialize(context);
    task.start(properties);
    task.put(sinkRecords);
    task.stop();

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {-1, 2, 5};

    for (TopicPartition tp : context.assignment()) {
      String directory = tp.topic() + "/" + "partition=" + String.valueOf(tp.partition());
      for (int j = 1; j < validOffsets.length; ++j) {
        long startOffset = validOffsets[j - 1] + 1;
        long endOffset = validOffsets[j];
        String topicsDir = this.topicsDir.get(tp.topic());
        Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory, tp,
                startOffset, endOffset, extension,
                ZERO_PAD_FMT));
        Collection<Object> records = schemaFileReader.readData(connectorConfig.getHadoopConfiguration(), path);
        long size = endOffset - startOffset + 1;
        assertEquals(records.size(), size);
        for (Object avroRecord : records) {
          assertEquals(avroRecord, record);
        }
      }
    }
  }

  @Test
  public void testPartialRevocation() throws Exception {
    setUp();

    Collection<TopicPartition> initialAssignment = new ArrayList<>();
    initialAssignment.add(TOPIC_PARTITION);
    initialAssignment.add(TOPIC_PARTITION2);
    initialAssignment.add(TOPIC_PARTITION3);

    Collection<TopicPartition> revokedPartitions = new ArrayList<>();
    revokedPartitions.add(TOPIC_PARTITION3);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);
    Collection<SinkRecord> sinkRecords = Collections.singleton(
            new SinkRecord(TOPIC_PARTITION.topic(), TOPIC_PARTITION.partition(),
                    Schema.STRING_SCHEMA, key, schema, record, 0));

    HdfsSinkTask task = new HdfsSinkTask();
    task.initialize(context);
    task.start(properties);

    // Given 3 owned partitions
    task.open(initialAssignment);

    // When 1 partition revoked (partial revocation)
    task.close(revokedPartitions);

    try {
      // Should continue processing messages from the 2 left partitions (should succeed)
      task.put(sinkRecords);
    } finally {
      task.stop();
    }
  }

  private void createCommittedFiles() throws IOException {
    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    String file1 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 0,
                                               10, extension, ZERO_PAD_FMT);
    String file2 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION, 11,
                                               20, extension, ZERO_PAD_FMT);
    String file3 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION2, 21,
                                               40, extension, ZERO_PAD_FMT);
    String file4 = FileUtils.committedFileName(url, topicsDir, DIRECTORY1, TOPIC_PARTITION2, 41,
                                               45, extension, ZERO_PAD_FMT);
    fs.createNewFile(new Path(file1));
    fs.createNewFile(new Path(file2));
    fs.createNewFile(new Path(file3));
    fs.createNewFile(new Path(file4));
  }

  private void createWALs(Map<TopicPartition, List<String>> tempfiles,
                          Map<TopicPartition, List<String>> committedFiles) throws Exception {
    @SuppressWarnings("unchecked")
    Class<? extends HdfsStorage> storageClass = (Class<? extends HdfsStorage>)
        connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
    HdfsStorage storage = StorageFactory.createStorage(
        storageClass,
        HdfsSinkConnectorConfig.class,
        connectorConfig,
        url
    );

    for (TopicPartition tp: tempfiles.keySet()) {
      WAL wal = storage.wal(logsDir, tp);
      List<String> tempList = tempfiles.get(tp);
      List<String> committedList = committedFiles.get(tp);
      wal.append(WAL.beginMarker, "");
      for (int i = 0; i < tempList.size(); ++i) {
        wal.append(tempList.get(i), committedList.get(i));
      }
      wal.append(WAL.endMarker, "");
      wal.close();
    }
  }
}

