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
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import io.confluent.connect.hdfs.parquet.ParquetFormat;
import io.confluent.connect.hdfs.string.StringFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.confluent.connect.hdfs.avro.AvroFormat;
import io.confluent.connect.hdfs.json.JsonFormat;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.ParentValueRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

import static io.confluent.connect.hdfs.HdfsSinkConnector.TASK_ID_CONFIG_NAME;
import static io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_CONFIG;
import static io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_DISPLAY;
import static io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_DOC;
import static io.confluent.connect.storage.common.StorageCommonConfig.TOPICS_DIR_CONFIG;
import static io.confluent.connect.storage.common.StorageCommonConfig.TOPICS_DIR_DEFAULT;
import static io.confluent.connect.storage.common.StorageCommonConfig.TOPICS_DIR_DISPLAY;
import static io.confluent.connect.storage.common.StorageCommonConfig.TOPICS_DIR_DOC;
import static io.confluent.connect.storage.hive.HiveConfig.HIVE_DATABASE_CONFIG;
import static io.confluent.connect.storage.hive.HiveConfig.HIVE_INTEGRATION_CONFIG;

public class HdfsSinkConnectorConfig extends StorageSinkConnectorConfig {

  private static final String TOPIC_SUBSTITUTION = "${topic}";

  // HDFS Group
  // This config is deprecated and will be removed in future releases. Use store.url instead.
  public static final String HDFS_URL_CONFIG = "hdfs.url";
  public static final String HDFS_URL_DOC =
      "The HDFS connection URL. This configuration has the format of hdfs://hostname:port and "
          + "specifies the HDFS to export data to. This property is deprecated and will be "
          + "removed in future releases. Use ``store.url`` instead.";
  public static final String HDFS_URL_DEFAULT = null;
  public static final String HDFS_URL_DISPLAY = "HDFS URL";

  public static final String HADOOP_CONF_DIR_CONFIG = "hadoop.conf.dir";
  public static final String HADOOP_CONF_DIR_DEFAULT = "";
  private static final String HADOOP_CONF_DIR_DOC = "The Hadoop configuration directory.";
  private static final String HADOOP_CONF_DIR_DISPLAY = "Hadoop Configuration Directory";

  public static final String HADOOP_HOME_CONFIG = "hadoop.home";
  public static final String HADOOP_HOME_DEFAULT = "";
  private static final String HADOOP_HOME_DOC = "The Hadoop home directory.";
  private static final String HADOOP_HOME_DISPLAY = "Hadoop home directory";

  public static final String HIVE_TABLE_NAME_CONFIG = "hive.table.name";
  public static final String HIVE_TABLE_NAME_DEFAULT = TOPIC_SUBSTITUTION;
  private static final String HIVE_TABLE_NAME_DOC = "The hive table name to use. "
          + "It must contain '${topic}' to inject the corresponding topic name.";
  private static final String HIVE_TABLE_NAME_DISPLAY = "Hive table name";

  // Storage group
  public static final String TOPIC_CAPTURE_GROUPS_REGEX_CONFIG = "topic.capture.groups.regex";
  public static final String TOPIC_CAPTURE_GROUPS_REGEX_DISPLAY = "Topic Capture Groups Regex";
  public static final String TOPIC_CAPTURE_GROUPS_REGEX_DOC = "A Java Pattern regex that matches "
      + "the entire topic and captures values for substituting into ``topics.dir``. Indexed "
      + "capture groups are accessible with ``${n}``, where ``${0}`` refers to the whole match and "
      + "``${1}`` refers to the first capture group. Example config value of "
      + "``([a-zA-Z]*)_([a-zA-Z]*)`` will match topics that are two words delimited by an "
      + "underscore and will capture each word separately. With ``topic.dir = ${1}/${2}``, a "
      + "record from the topic ``example_name`` will be written into a subdirectory of "
      + "``example/name/``. By default, this functionality is not enabled.";
  public static final String TOPIC_CAPTURE_GROUPS_REGEX_DEFAULT = null;

  private static final String DIR_REGEX_DOC = " Supports ``${topic}`` in the value, which will be "
      + "replaced by the actual topic name. Supports ``${0}``, ..., ``${n}`` in "
      + "conjunction with " + TOPIC_CAPTURE_GROUPS_REGEX_CONFIG + ". See "
      + TOPIC_CAPTURE_GROUPS_REGEX_CONFIG + " configuration documentation for details.";

  // HDFS Group
  public static final String LOGS_DIR_CONFIG = "logs.dir";
  public static final String LOGS_DIR_DOC =
      "Top level directory to store the write ahead logs." + DIR_REGEX_DOC;
  public static final String LOGS_DIR_DEFAULT = "logs";
  public static final String LOGS_DIR_DISPLAY = "Logs directory";
  
  // Security group
  public static final String HDFS_AUTHENTICATION_KERBEROS_CONFIG = "hdfs.authentication.kerberos";
  private static final String HDFS_AUTHENTICATION_KERBEROS_DOC =
      "Configuration indicating whether HDFS is using Kerberos for authentication.";
  private static final boolean HDFS_AUTHENTICATION_KERBEROS_DEFAULT = false;
  private static final String HDFS_AUTHENTICATION_KERBEROS_DISPLAY = "HDFS Authentication Kerberos";

  public static final String CONNECT_HDFS_PRINCIPAL_CONFIG = "connect.hdfs.principal";
  public static final String CONNECT_HDFS_PRINCIPAL_DEFAULT = "";
  private static final String CONNECT_HDFS_PRINCIPAL_DOC =
      "The principal to use when HDFS is using Kerberos to for authentication.";
  private static final String CONNECT_HDFS_PRINCIPAL_DISPLAY = "Connect Kerberos Principal";

  public static final String CONNECT_HDFS_KEYTAB_CONFIG = "connect.hdfs.keytab";
  public static final String CONNECT_HDFS_KEYTAB_DEFAULT = "";
  private static final String CONNECT_HDFS_KEYTAB_DOC =
      "The path to the keytab file for the HDFS connector principal. "
          + "This keytab file should only be readable by the connector user.";
  private static final String CONNECT_HDFS_KEYTAB_DISPLAY = "Connect Kerberos Keytab";

  public static final String HDFS_NAMENODE_PRINCIPAL_CONFIG = "hdfs.namenode.principal";
  public static final String HDFS_NAMENODE_PRINCIPAL_DEFAULT = "";
  private static final String HDFS_NAMENODE_PRINCIPAL_DOC = "The principal for HDFS Namenode.";
  private static final String HDFS_NAMENODE_PRINCIPAL_DISPLAY = "HDFS NameNode Kerberos Principal";

  public static final String KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG =
      "kerberos.ticket.renew.period.ms";
  public static final long KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT = 60000 * 60;
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DOC =
      "The period in milliseconds to renew the Kerberos ticket.";
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY = "Kerberos Ticket Renew "
      + "Period (ms)";

  private static final Pattern SUBSTITUTION_PATTERN = Pattern.compile("\\$\\{(\\d+)}");
  private static final Pattern INVALID_SUB_PATTERN = Pattern.compile("\\$\\{.*}");

  private static final ConfigDef.Recommender hdfsAuthenticationKerberosDependentsRecommender =
      new BooleanParentRecommender(
          HDFS_AUTHENTICATION_KERBEROS_CONFIG);

  private static final GenericRecommender STORAGE_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender FORMAT_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER = new GenericRecommender();
  private static final ParentValueRecommender AVRO_COMPRESSION_RECOMMENDER
      = new ParentValueRecommender(FORMAT_CLASS_CONFIG, AvroFormat.class, AVRO_SUPPORTED_CODECS);

  //Kerberos renew ticket
  public static final String KERBEROS_REFRESH_TICKET_CONFIG = "hdfs.kerberos.refresh.ticket";
  private static final String KERBEROS_REFRESH_TICKET_DOC =
          "Configuration indicating whether kerberos should refresh ticket or not.";
  private static final boolean KERBEROS_REFRESH_TICKET_DEFAULT = true;
  private static final String KERBEROS_REFRESH_TICKET_DISPLAY = "Kerberos refresh ticket";
  private static final ConfigDef.Recommender KerberosRenewTicketDependentsRecommender =
          new BooleanParentRecommender(
                  KERBEROS_REFRESH_TICKET_CONFIG);

  static {
    STORAGE_CLASS_RECOMMENDER.addValidValues(
        Arrays.asList(HdfsStorage.class)
    );

    FORMAT_CLASS_RECOMMENDER.addValidValues(
        Arrays.asList(
            AvroFormat.class,
            JsonFormat.class,
            OrcFormat.class,
            ParquetFormat.class,
            StringFormat.class
        )
    );

    PARTITIONER_CLASS_RECOMMENDER.addValidValues(
        Arrays.asList(
            DefaultPartitioner.class,
            HourlyPartitioner.class,
            DailyPartitioner.class,
            TimeBasedPartitioner.class,
            FieldPartitioner.class
        )
    );
  }

  public static ConfigDef newConfigDef() {
    ConfigDef configDef = new ConfigDef();
    // Define HDFS configuration group
    {
      final String group = "HDFS";
      int orderInGroup = 0;

      // HDFS_URL_CONFIG property is retained for backwards compatibility with HDFS connector and
      // will be removed in future versions.
      configDef.define(
          HDFS_URL_CONFIG,
          Type.STRING,
          HDFS_URL_DEFAULT,
          Importance.HIGH,
          HDFS_URL_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          HDFS_URL_DISPLAY
      );

      configDef.define(
          HADOOP_CONF_DIR_CONFIG,
          Type.STRING,
          HADOOP_CONF_DIR_DEFAULT,
          Importance.HIGH,
          HADOOP_CONF_DIR_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          HADOOP_CONF_DIR_DISPLAY
      );

      configDef.define(
          HADOOP_HOME_CONFIG,
          Type.STRING,
          HADOOP_HOME_DEFAULT,
          Importance.HIGH,
          HADOOP_HOME_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          HADOOP_HOME_DISPLAY
      );

      configDef.define(
          LOGS_DIR_CONFIG,
          Type.STRING,
          LOGS_DIR_DEFAULT,
          Importance.HIGH,
          LOGS_DIR_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          LOGS_DIR_DISPLAY
      );

      configDef.define(
              HIVE_TABLE_NAME_CONFIG,
              Type.STRING,
              HIVE_TABLE_NAME_DEFAULT,
              Importance.LOW,
              HIVE_TABLE_NAME_DOC,
              group,
              ++orderInGroup,
              Width.SHORT,
              HIVE_TABLE_NAME_DISPLAY
      );
    }

    {
      final String group = "Security";
      int orderInGroup = 0;
      // Define Security configuration group
      configDef.define(
          HDFS_AUTHENTICATION_KERBEROS_CONFIG,
          Type.BOOLEAN,
          HDFS_AUTHENTICATION_KERBEROS_DEFAULT,
          Importance.HIGH,
          HDFS_AUTHENTICATION_KERBEROS_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          HDFS_AUTHENTICATION_KERBEROS_DISPLAY,
          Arrays.asList(
              CONNECT_HDFS_PRINCIPAL_CONFIG,
              CONNECT_HDFS_KEYTAB_CONFIG,
              HDFS_NAMENODE_PRINCIPAL_CONFIG,
              KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG
          )
      );

      configDef.define(
          CONNECT_HDFS_PRINCIPAL_CONFIG,
          Type.STRING,
          CONNECT_HDFS_PRINCIPAL_DEFAULT,
          Importance.HIGH,
          CONNECT_HDFS_PRINCIPAL_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          CONNECT_HDFS_PRINCIPAL_DISPLAY,
          hdfsAuthenticationKerberosDependentsRecommender
      );

      configDef.define(
          CONNECT_HDFS_KEYTAB_CONFIG,
          Type.STRING,
          CONNECT_HDFS_KEYTAB_DEFAULT,
          Importance.HIGH,
          CONNECT_HDFS_KEYTAB_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          CONNECT_HDFS_KEYTAB_DISPLAY,
          hdfsAuthenticationKerberosDependentsRecommender
      );

      configDef.define(
          HDFS_NAMENODE_PRINCIPAL_CONFIG,
          Type.STRING,
          HDFS_NAMENODE_PRINCIPAL_DEFAULT,
          Importance.HIGH,
          HDFS_NAMENODE_PRINCIPAL_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          HDFS_NAMENODE_PRINCIPAL_DISPLAY,
          hdfsAuthenticationKerberosDependentsRecommender
      );

      configDef.define(
          KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG,
          Type.LONG,
          KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT,
          Importance.LOW,
          KERBEROS_TICKET_RENEW_PERIOD_MS_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY,
          hdfsAuthenticationKerberosDependentsRecommender
      );

      configDef.define(
              KERBEROS_REFRESH_TICKET_CONFIG,
              Type.BOOLEAN,
              KERBEROS_REFRESH_TICKET_DEFAULT,
              Importance.LOW,
              KERBEROS_REFRESH_TICKET_DOC,
              group,
              ++orderInGroup,
              Width.SHORT,
              KERBEROS_REFRESH_TICKET_DISPLAY
      );
    }
    // Put the storage group(s) last ...
    ConfigDef storageConfigDef = StorageSinkConnectorConfig.newConfigDef(
        FORMAT_CLASS_RECOMMENDER,
        AVRO_COMPRESSION_RECOMMENDER);

    int lastOrder = 0;
    String group = "Storage";
    for (ConfigDef.ConfigKey key : storageConfigDef.configKeys().values()) {
      configDef.define(key);
      lastOrder = Math.max(key.orderInGroup, lastOrder);
    }

    // add the topic.capture.groups.regex config to storage
    configDef
        .define(
            TOPIC_CAPTURE_GROUPS_REGEX_CONFIG,
            Type.STRING,
            TOPIC_CAPTURE_GROUPS_REGEX_DEFAULT,
            Importance.LOW,
            TOPIC_CAPTURE_GROUPS_REGEX_DOC,
            group,
            ++lastOrder,
            Width.LONG,
            TOPIC_CAPTURE_GROUPS_REGEX_DISPLAY
    );

    return configDef;
  }

  private final String url;
  private final StorageCommonConfig commonConfig;
  private final HiveConfig hiveConfig;
  private final PartitionerConfig partitionerConfig;
  private final Pattern topicRegexCaptureGroup;
  private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();
  private final Set<AbstractConfig> allConfigs = new HashSet<>();
  private Configuration hadoopConfig;
  private int topicDirGroupsMaxIndex;
  private int logDirGroupsMaxIndex;
  private int taskId;

  public HdfsSinkConnectorConfig(Map<String, String> props) {
    this(newConfigDef() , addDefaults(props));
  }

  protected HdfsSinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
    ConfigDef storageCommonConfigDef = StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER);
    commonConfig = new StorageCommonConfig(storageCommonConfigDef, originalsStrings());
    hiveConfig = new HiveConfig(originalsStrings());
    ConfigDef partitionerConfigDef = PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER);
    partitionerConfig = new PartitionerConfig(partitionerConfigDef, originalsStrings());
    this.hadoopConfig = new Configuration();
    taskId = props.get(TASK_ID_CONFIG_NAME) != null
        ? Integer.parseInt(props.get(TASK_ID_CONFIG_NAME)) : -1;
    addToGlobal(hiveConfig);
    addToGlobal(partitionerConfig);
    addToGlobal(commonConfig);
    addToGlobal(this);
    this.url = extractUrl();
    try {
      String topicRegex = getString(TOPIC_CAPTURE_GROUPS_REGEX_CONFIG);
      this.topicRegexCaptureGroup = topicRegex != null ? Pattern.compile(topicRegex) : null;
    } catch (PatternSyntaxException e) {
      throw new ConfigException(
          TOPIC_CAPTURE_GROUPS_REGEX_CONFIG + " is an invalid regex pattern: ",
          e
      );
    }

    topicDirGroupsMaxIndex = getMaxIndexToReplace(getString(TOPICS_DIR_CONFIG));
    logDirGroupsMaxIndex = getMaxIndexToReplace(getString(LOGS_DIR_CONFIG));

    validateDirsAndRegex();
    validateTimezone();
  }

  /**
   * Validate the timezone with the rotate.schedule.interval.ms config,
   * these need validation before use in the TopicPartitionWriter.
   */
  private void validateTimezone() {
    String timezone = getString(PartitionerConfig.TIMEZONE_CONFIG);
    long rotateScheduleIntervalMs = getLong(ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
    if (rotateScheduleIntervalMs > 0 && timezone.isEmpty()) {
      throw new ConfigException(
          String.format(
              "%s configuration must be set when using %s",
              PartitionerConfig.TIMEZONE_CONFIG,
              ROTATE_SCHEDULE_INTERVAL_MS_CONFIG
          )
      );
    }
  }

  public static Map<String, String> addDefaults(Map<String, String> props) {
    ConcurrentMap<String, String> propsCopy = new ConcurrentHashMap<>(props);
    propsCopy.putIfAbsent(STORAGE_CLASS_CONFIG, HdfsStorage.class.getName());
    propsCopy.putIfAbsent(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    return propsCopy;
  }

  private void addToGlobal(AbstractConfig config) {
    allConfigs.add(config);
    addConfig(config.values(), (ComposableConfig) config);
  }

  private void addConfig(Map<String, ?> parsedProps, ComposableConfig config) {
    for (String key : parsedProps.keySet()) {
      propertyToConfig.put(key, config);
    }
  }

  /**
   * Returns the url property. Preference is given to property <code>store.url</code> over
   * <code>hdfs.url</code> because <code>hdfs.url</code> is deprecated.
   *
   * @return String url for HDFS
   */
  private String extractUrl() {
    String storageUrl = getString(StorageCommonConfig.STORE_URL_CONFIG);
    if (StringUtils.isNotBlank(storageUrl)) {
      return storageUrl;
    }

    String hdfsUrl = getString(HDFS_URL_CONFIG);
    if (StringUtils.isNotBlank(hdfsUrl)) {
      return hdfsUrl;
    }

    throw new ConfigException(
        String.format("Configuration %s cannot be empty.", StorageCommonConfig.STORE_URL_CONFIG)
    );
  }

  public String connectHdfsPrincipal() {
    return getString(CONNECT_HDFS_PRINCIPAL_CONFIG);
  }

  public String connectHdfsKeytab() {
    return getString(CONNECT_HDFS_KEYTAB_CONFIG);
  }

  public String hadoopConfDir() {
    return getString(HADOOP_CONF_DIR_CONFIG);
  }

  public String hadoopHome() {
    return getString(HADOOP_HOME_CONFIG);
  }

  public String hdfsNamenodePrincipal() {
    return getString(HDFS_NAMENODE_PRINCIPAL_CONFIG);
  }

  public boolean kerberosAuthentication() {
    return getBoolean(HDFS_AUTHENTICATION_KERBEROS_CONFIG);
  }

  public long kerberosTicketRenewPeriodMs() {
    return getLong(KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG);
  }

  public boolean kerberosRefreshTicket() {
    return getBoolean(KERBEROS_REFRESH_TICKET_CONFIG);
  }

  public String logsDir() {
    return getString(LOGS_DIR_CONFIG);
  }

  public String name() {
    return originalsStrings().getOrDefault("name", "HDFS-sink");
  }

  public String url() {
    return url;
  }

  @Override
  public Object get(String key) {
    ComposableConfig config = propertyToConfig.get(key);
    if (config == null) {
      throw new ConfigException(String.format("Unknown configuration '%s'", key));
    }
    return config == this ? super.get(key) : config.get(key);
  }

  public Configuration getHadoopConfiguration() {
    return hadoopConfig;
  }

  public Map<String, ?> plainValues() {
    Map<String, Object> map = new HashMap<>();
    for (AbstractConfig config : allConfigs) {
      map.putAll(config.values());
    }
    // Include any additional properties not defined by the ConfigDef
    // that custom partitioners might need
    Map<String, ?> originals = originals();
    for (String originalKey : originals.keySet()) {
      if (!map.containsKey(originalKey)) {
        map.put(originalKey, originals.get(originalKey));
      }
    }

    return map;
  }

  /**
   * Performs all substitutions on `logs.dir` and calculates the final directory for a topic
   *
   * @param topic - String - the topic whose directory to find
   * @return String - the directory name and path
   */
  public String getLogsDirFromTopic(String topic) {
    return getDirFromTopic(getString(LOGS_DIR_CONFIG), topic, logDirGroupsMaxIndex);
  }

  /**
   * Performs all substitutions on `topics.dir` and calculates the final directory for a topic
   *
   * @param topic - String - the topic whose directory to find
   * @return String - the directory name and path
   */
  public String getTopicsDirFromTopic(String topic) {
    return getDirFromTopic(getString(TOPICS_DIR_CONFIG), topic, topicDirGroupsMaxIndex);
  }

  /**
   * Performs all substitutions on {@value HIVE_TABLE_NAME_CONFIG} and calculates the final
   * hive table name for the given topic
   *
   * @param topic String - the topic name
   * @return String the hive table name
   */
  public String getHiveTableName(String topic) {
    return getString(HIVE_TABLE_NAME_CONFIG).replace("${topic}", topic);
  }

  /**
   * Performs all substitutions and calculates the final directory for a topic
   *
   * @param dir - String - the directory to perform substitutions on
   * @param topic - String - the topic whose directory to find
   * @return String - the directory name and path
   */
  private String getDirFromTopic(String dir, String topic, int maxIndexParts) {
    dir = dir.replace("${topic}", topic);

    // only if configured
    if (topicRegexCaptureGroup != null) {

      // find all of the captured groups by the regex
      Matcher matcher = topicRegexCaptureGroup.matcher(topic);
      if (!matcher.matches()) {
        throw new ConfigException(
            TOPIC_CAPTURE_GROUPS_REGEX_CONFIG,
            topicRegexCaptureGroup.pattern(),
            String.format("Topic %s does not fully match the specified regex.", topic)
        );
      }

      // make sure that all references to captured groups actually exist
      if (maxIndexParts > matcher.groupCount()) {
        throw new ConfigException(
            String.format(
                "Topic %s must have at least %d capture groups using regex pattern %s, "
                    + "but actually had %d capture groups.",
                topic,
                maxIndexParts,
                topicRegexCaptureGroup.pattern(),
                matcher.groupCount()
            )
        );
      }

      for (int index = 0; index < matcher.groupCount() + 1; index++) {
        dir = dir.replace("${" + index + "}", matcher.group(index));
      }
    }

    return dir;
  }

  /**
   * Finds all instances of `${[0-9]+}` in a string and returns the maximum integer.
   * @return int - largest number found in the regex pattern - -1 if none found
   */
  private int getMaxIndexToReplace(String string) {

    List<Integer> toReplace = new ArrayList<>();
    Matcher partsMatcher = SUBSTITUTION_PATTERN.matcher(string);

    while (partsMatcher.find()) {
      String part = partsMatcher.group();
      if (!part.isEmpty() && partsMatcher.groupCount() == 1) {
        toReplace.add(Integer.valueOf(partsMatcher.group(1))); // should be the first group
      }
    }

    return toReplace.isEmpty() ? -1 : Collections.max(toReplace);
  }

  /**
   * Validates that the `topics.dir`, `logs.dir`, and `topic.regex.capture.group` configs are all
   * within valid ranges.
   */
  private void validateDirsAndRegex() {
    if (topicDirGroupsMaxIndex >= 0 && topicRegexCaptureGroup == null) {
      throw new ConfigException(
          TOPICS_DIR_CONFIG + " cannot contain ${} without a valid "
              + TOPIC_CAPTURE_GROUPS_REGEX_CONFIG + " being configured."
      );
    }

    if (logDirGroupsMaxIndex >= 0 && topicRegexCaptureGroup == null) {
      throw new ConfigException(
          LOGS_DIR_CONFIG + " cannot contain ${} without a valid "
              + TOPIC_CAPTURE_GROUPS_REGEX_CONFIG + " being configured."
      );
    }

    validateReplacements(TOPICS_DIR_CONFIG);
    validateReplacements(LOGS_DIR_CONFIG);
    validateHiveTableNameReplacements();
  }

  private void validateHiveTableNameReplacements() {
    String config = HIVE_TABLE_NAME_CONFIG;
    String configValue = getString(config);

    if (!configValue.contains(TOPIC_SUBSTITUTION)) {
      throw new ConfigException(
              String.format(
                      "%s: '%s' has to contain topic substitution '%s'.",
                      config,
                      getString(config),
                      TOPIC_SUBSTITUTION
              )
      );
    }

    // remove all valid ${} substitutions
    String tableName = configValue.replace(TOPIC_SUBSTITUTION, "");

    // check for invalid ${} substitutions
    Matcher invalidMatcher = INVALID_SUB_PATTERN.matcher(tableName);
    if (invalidMatcher.find()) {
      throw new ConfigException(
              String.format(
                      "%s: '%s' contains an invalid ${} substitution '%s'. "
                              + "Valid substitution is '%s'",
                      config,
                      getString(config),
                      invalidMatcher.group(),
                      TOPIC_SUBSTITUTION
              )
      );
    }
  }

  /**
   * Validates that the config has no invalid substitutions
   *
   * @param config - String - the config to validate
   */
  private void validateReplacements(String config) {
    // remove all valid ${} substitutions
    Matcher partsMatcher = SUBSTITUTION_PATTERN.matcher(getString(config));
    String dir = partsMatcher.replaceAll("").replace(TOPIC_SUBSTITUTION, "");

    // check for invalid ${} substitutions
    Matcher invalidMatcher = INVALID_SUB_PATTERN.matcher(dir);
    if (invalidMatcher.find()) {
      throw new ConfigException(
          String.format(
              "%s: %s contains an invalid ${} substitution %s. Valid substitutions are %s "
                  + "and ${n} where n >= 0.",
              config,
              getString(config),
              invalidMatcher.group(),
              TOPIC_SUBSTITUTION
          )
      );
    }
  }

  private static class BooleanParentRecommender implements ConfigDef.Recommender {

    protected String parentConfigName;

    public BooleanParentRecommender(String parentConfigName) {
      this.parentConfigName = parentConfigName;
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return new LinkedList<>();
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return (Boolean) connectorConfigs.get(parentConfigName);
    }
  }

  public static ConfigDef getConfig() {
    // Define the names of the configurations we're going to override
    Set<String> skip = new HashSet<>();
    skip.add(STORAGE_CLASS_CONFIG);
    skip.add(FORMAT_CLASS_CONFIG);
    skip.add(TOPICS_DIR_CONFIG);

    // Order added is important, so that group order is maintained
    ConfigDef visible = new ConfigDef();
    addAllConfigKeys(visible, newConfigDef(), skip);
    addAllConfigKeys(visible, StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER), skip);
    addAllConfigKeys(visible, PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER), skip);
    addAllConfigKeys(visible, HiveConfig.getConfig(), skip);

    // Add the overridden configurations
    visible.define(
        STORAGE_CLASS_CONFIG,
        Type.CLASS,
        HdfsStorage.class.getName(),
        Importance.HIGH,
        STORAGE_CLASS_DOC,
        "Storage",
        1,
        Width.NONE,
        STORAGE_CLASS_DISPLAY,
        STORAGE_CLASS_RECOMMENDER
    );

    visible.define(
        FORMAT_CLASS_CONFIG,
        Type.CLASS,
        AvroFormat.class.getName(),
        Importance.HIGH,
        FORMAT_CLASS_DOC,
        "Connector",
        1,
        Width.NONE,
        FORMAT_CLASS_DISPLAY,
        FORMAT_CLASS_RECOMMENDER
    );

    visible.define(
        TOPICS_DIR_CONFIG,
        Type.STRING,
        TOPICS_DIR_DEFAULT,
        Importance.HIGH,
        TOPICS_DIR_DOC + DIR_REGEX_DOC,
        "Storage",
        2,
        Width.NONE,
        TOPICS_DIR_DISPLAY
    );

    return visible;
  }

  private static void addAllConfigKeys(ConfigDef container, ConfigDef other, Set<String> skip) {
    for (ConfigDef.ConfigKey key : other.configKeys().values()) {
      if (skip != null && !skip.contains(key.name)) {
        container.define(key);
      }
    }
  }

  public int getTaskId() {
    return taskId;
  }

  public boolean hiveIntegrationEnabled() {
    return getBoolean(HIVE_INTEGRATION_CONFIG);
  }

  public String hiveDatabase() {
    return getString(HIVE_DATABASE_CONFIG);
  }

  @SuppressWarnings("unchecked")
  public Class<? extends HdfsStorage> storageClass() {
    return (Class<? extends HdfsStorage>) getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
  }

  public static void main(String[] args) {
    System.out.println(getConfig().toEnrichedRst());
  }

}
