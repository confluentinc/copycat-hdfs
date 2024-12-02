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

import io.confluent.connect.hdfs.orc.OrcFormat;
import io.confluent.connect.hdfs.parquet.ParquetFormat;
import io.confluent.connect.hdfs.string.StringFormat;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.avro.AvroFormat;
import io.confluent.connect.hdfs.json.JsonFormat;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class HdfsSinkConnectorConfigTest extends TestWithMiniDFSCluster {
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testHiveTableName() {
    properties.put(HdfsSinkConnectorConfig.HIVE_TABLE_NAME_CONFIG, "a-${topic}-test");
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    assertEquals("a-test-topic-test",
            connectorConfig.getHiveTableName("test-topic"));
  }

  @Test
  public void testHiveTableNameValidation() {
    properties.put(HdfsSinkConnectorConfig.HIVE_TABLE_NAME_CONFIG, "static-table");
    ConfigException configException = assertThrows(ConfigException.class,
            () -> new HdfsSinkConnectorConfig(properties));
    assertEquals(
            "hive.table.name: 'static-table' has to contain topic substitution '${topic}'.",
            configException.getMessage());

    properties.put(HdfsSinkConnectorConfig.HIVE_TABLE_NAME_CONFIG, "${topic}-${extra}");
    configException = assertThrows(ConfigException.class,
            () -> new HdfsSinkConnectorConfig(properties));
    assertEquals(
            "hive.table.name: '${topic}-${extra}' contains an invalid ${} substitution " +
                    "'${extra}'. Valid substitution is '${topic}'",
            configException.getMessage());
  }

  @Test
  public void testValidRegexCaptureGroup() {
    String topic = "topica";
    String topicDir = "topic.another.${topic}.again";

    properties.put(HdfsSinkConnectorConfig.TOPIC_CAPTURE_GROUPS_REGEX_CONFIG, ".*");
    properties.put(StorageCommonConfig.TOPICS_DIR_CONFIG, topicDir);
    connectorConfig = new HdfsSinkConnectorConfig(properties);

    assertEquals(
        topicDir.replace("${topic}", topic),
        connectorConfig.getTopicsDirFromTopic(topic)
    );
  }

  @Test
  public void testTopicDirFromTopicParts() {
    String topic = "a.b.c.d";
    String topicDir = "${1}-${2}-${3}-${4}";

    properties.put(
        HdfsSinkConnectorConfig.TOPIC_CAPTURE_GROUPS_REGEX_CONFIG,
        "([a-z])\\.([a-z])\\.([a-z])\\.([a-z])"
    );
    properties.put(StorageCommonConfig.TOPICS_DIR_CONFIG, topicDir);
    connectorConfig = new HdfsSinkConnectorConfig(properties);

    assertEquals(
        topic.replace(".", "-"),
        connectorConfig.getTopicsDirFromTopic(topic)
    );
  }

  @Test
  public void testTopicDirCanContainNumber() {
    String topic = "a.b.c.d";
    String topicDir = "${1}-${2}-${3}-${4}-1000";

    properties.put(
        HdfsSinkConnectorConfig.TOPIC_CAPTURE_GROUPS_REGEX_CONFIG,
        "([a-z])[\\.\\-_]([a-z])[\\.\\-_]([a-z])[\\.\\-_]([a-z])"
    );
    properties.put(StorageCommonConfig.TOPICS_DIR_CONFIG, topicDir);
    connectorConfig = new HdfsSinkConnectorConfig(properties);

    assertEquals(
        topic.replace(".", "-") + "-1000",
        connectorConfig.getTopicsDirFromTopic(topic)
    );
  }

  @Test(expected = ConfigException.class)
  public void testInvalidTopicDir() {
    String topic = "a.b.c.d";
    String topicDir = "${100}-${2}-${3}-${4}";

    properties.put(
        HdfsSinkConnectorConfig.TOPIC_CAPTURE_GROUPS_REGEX_CONFIG,
        "([a-z])\\.([a-z])\\.([a-z])\\.([a-z])"
    );
    properties.put(StorageCommonConfig.TOPICS_DIR_CONFIG, topicDir);
    connectorConfig = new HdfsSinkConnectorConfig(properties);

    connectorConfig.getTopicsDirFromTopic(topic);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidTopicDirNegative() {
    String topic = "a.b.c.d";
    String topicDir = "${-1}-${2}-${3}-${4}";

    properties.put(
        HdfsSinkConnectorConfig.TOPIC_CAPTURE_GROUPS_REGEX_CONFIG,
        "([a-z])\\.([a-z])\\.([a-z])\\.([a-z])"
    );
    properties.put(StorageCommonConfig.TOPICS_DIR_CONFIG, topicDir);
    connectorConfig = new HdfsSinkConnectorConfig(properties);

    connectorConfig.getTopicsDirFromTopic(topic);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidRegexCaptureGroup() {
    String topicDir = "topic.another.${topic}.again";

    properties.put(HdfsSinkConnectorConfig.TOPIC_CAPTURE_GROUPS_REGEX_CONFIG, "[a");
    properties.put(StorageCommonConfig.TOPICS_DIR_CONFIG, topicDir);
    connectorConfig = new HdfsSinkConnectorConfig(properties);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidRegexCaptureGroupDoesntMatchTopic() {
    String topic = "topica";
    String topicDir = "topic.another.${topic}.again";

    properties.put(HdfsSinkConnectorConfig.TOPIC_CAPTURE_GROUPS_REGEX_CONFIG, "[a-z]");
    properties.put(StorageCommonConfig.TOPICS_DIR_CONFIG, topicDir);
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    connectorConfig.getTopicsDirFromTopic(topic);
  }

  @Test(expected = ConfigException.class)
  public void testUrlConfigMustBeNonEmpty() {
    properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
    properties.remove(HdfsSinkConnectorConfig.HDFS_URL_CONFIG);
    connectorConfig = new HdfsSinkConnectorConfig(properties);
  }

  @Test
  public void testStorageCommonUrlPreferred() {
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    assertEquals(url, connectorConfig.url());
  }

  @Test
  public void testHdfsUrlIsValid() {
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
    assertEquals(url, connectorConfig.url());
  }

  @Test
  public void testStorageClass() throws Exception {
    // No real test case yet
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    assertEquals(
        HdfsStorage.class,
        connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG)
    );
  }

  @Test
  public void testUndefinedURL() throws Exception {
    properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    assertNull(connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG));
  }

  @Test
  public void testAvroCompressionSettings() {
    for (String codec : HdfsSinkConnectorConfig.AVRO_SUPPORTED_CODECS) {
      Map<String, String> props = new HashMap<>(this.properties);
      props.put(HdfsSinkConnectorConfig.AVRO_CODEC_CONFIG, codec);
      HdfsSinkConnectorConfig config = new HdfsSinkConnectorConfig(props);
      Assert.assertNotNull(config.getAvroCodec());
    }
  }

  @Test(expected = ConfigException.class)
  public void testUnsupportedAvroCompressionSettings() {
    // test for an unsupported codec.
    this.properties.put(HdfsSinkConnectorConfig.AVRO_CODEC_CONFIG, "abc");

    new HdfsSinkConnectorConfig(properties);
    Assert.assertTrue("Expected the constructor to throw an exception", false);
  }

  @Test
  public void testValidTimezoneWithScheduleIntervalAccepted (){
    properties.put(PartitionerConfig.TIMEZONE_CONFIG, "CET");
    properties.put(HdfsSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "30");
    new HdfsSinkConnectorConfig(properties);
  }

  @Test(expected = ConfigException.class)
  public void testEmptyTimezoneThrowsExceptionOnScheduleInterval() {
    properties.put(PartitionerConfig.TIMEZONE_CONFIG, PartitionerConfig.TIMEZONE_DEFAULT);
    properties.put(HdfsSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "30");
    new HdfsSinkConnectorConfig(properties);
  }

  @Test
  public void testEmptyTimezoneExceptionMessage() {
    properties.put(PartitionerConfig.TIMEZONE_CONFIG, PartitionerConfig.TIMEZONE_DEFAULT);
    properties.put(HdfsSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG, "30");
    String expectedError =  String.format(
        "%s configuration must be set when using %s",
        PartitionerConfig.TIMEZONE_CONFIG,
        HdfsSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG
    );
    try {
      new HdfsSinkConnectorConfig(properties);
    } catch (ConfigException e) {
      assertEquals(expectedError, e.getMessage());
    }
  }

  @Test
  public void testKerberosRefreshTicketDefault() {
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    assertEquals(Boolean.TRUE,
            connectorConfig.kerberosRefreshTicket());
  }
  @Test
  public void testKerberosRefreshTicket() {
    properties.put(HdfsSinkConnectorConfig.KERBEROS_REFRESH_TICKET_CONFIG, "false");
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    assertEquals(Boolean.FALSE,
            connectorConfig.kerberosRefreshTicket());
  }
  @Test
  public void testRecommendedValues() throws Exception {
    List<Object> expectedStorageClasses = Arrays.<Object>asList(HdfsStorage.class);

    List<Object> expectedFormatClasses = Arrays.<Object>asList(
        AvroFormat.class,
        JsonFormat.class,
        OrcFormat.class,
        ParquetFormat.class,
        StringFormat.class
    );

    List<Object> expectedPartitionerClasses = Arrays.<Object>asList(
        DefaultPartitioner.class,
        HourlyPartitioner.class,
        DailyPartitioner.class,
        TimeBasedPartitioner.class,
        FieldPartitioner.class
    );

    List<ConfigValue> values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      if (val.value() instanceof Class) {
        switch (val.name()) {
          case StorageCommonConfig.STORAGE_CLASS_CONFIG:
            assertEquals(expectedStorageClasses, val.recommendedValues());
            break;
          case HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG:
            assertEquals(expectedFormatClasses, val.recommendedValues());
            break;
          case PartitionerConfig.PARTITIONER_CLASS_CONFIG:
            assertEquals(expectedPartitionerClasses, val.recommendedValues());
            break;
        }
      }
    }
  }

  @Test
  public void testVisibilityForPartitionerClassDependentConfigs() throws Exception {
    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
    List<ConfigValue> values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getName());
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
          assertTrue(val.visible());
          break;
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DailyPartitioner.class.getName());
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, HourlyPartitioner.class.getName());
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        TimeBasedPartitioner.class.getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    Partitioner<?> klass = new Partitioner<FieldSchema>() {
      @Override
      public void configure(Map<String, Object> config) {}

      @Override
      public String encodePartition(SinkRecord sinkRecord) {
        return null;
      }

      @Override
      public String generatePartitionedPath(String topic, String encodedPartition) {
        return null;
      }

      @Override
      public List<FieldSchema> partitionFields() {
        return null;
      }
    };

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        klass.getClass().getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }
  }

  @Test
  public void testVisibilityForDeprecatedPartitionerClassDependentConfigs() throws Exception {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        io.confluent.connect.hdfs.partitioner.DefaultPartitioner.class.getName()
    );
    List<ConfigValue> values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        io.confluent.connect.hdfs.partitioner.FieldPartitioner.class.getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
          assertTrue(val.visible());
          break;
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        io.confluent.connect.hdfs.partitioner.DailyPartitioner.class.getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        io.confluent.connect.hdfs.partitioner.HourlyPartitioner.class.getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        io.confluent.connect.hdfs.partitioner.TimeBasedPartitioner.class.getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    io.confluent.connect.hdfs.partitioner.Partitioner klass =
        new io.confluent.connect.hdfs.partitioner.Partitioner() {
      @Override
      public void configure(Map<String, Object> config) {}

      @Override
      public String encodePartition(SinkRecord sinkRecord) {
        return null;
      }

      @Override
      public String generatePartitionedPath(String topic, String encodedPartition) {
        return null;
      }

      @Override
      public List<FieldSchema> partitionFields() {
        return null;
      }
    };

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        klass.getClass().getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }
  }
}

