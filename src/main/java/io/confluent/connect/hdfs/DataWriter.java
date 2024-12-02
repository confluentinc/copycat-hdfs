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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.filter.CommittedFileFilter;
import io.confluent.connect.hdfs.filter.TopicCommittedFileFilter;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

public class DataWriter {
  private static final Logger log = LoggerFactory.getLogger(DataWriter.class);
  private static final Time SYSTEM_TIME = new SystemTime();
  private final Time time;

  private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private HdfsStorage storage;
  private HashMap<String, String> logDirs;
  private HashMap<String, String> topicDirs;
  private Format format;
  private RecordWriterProvider writerProvider;
  private io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig>
      newWriterProvider;
  private io.confluent.connect.storage.format.SchemaFileReader<HdfsSinkConnectorConfig, Path>
      schemaFileReader;
  private io.confluent.connect.storage.format.Format<HdfsSinkConnectorConfig, Path> newFormat;
  private Partitioner partitioner;
  private HdfsSinkConnectorConfig connectorConfig;
  private AvroData avroData;
  private SinkTaskContext context;
  private ExecutorService executorService;
  private String hiveDatabase;
  private HiveMetaStore hiveMetaStore;
  private HiveUtil hive;
  private Queue<Future<Void>> hiveUpdateFutures;
  private Thread ticketRenewThread;
  private volatile boolean isRunning;

  public DataWriter(
      HdfsSinkConnectorConfig connectorConfig,
      SinkTaskContext context,
      AvroData avroData
  ) {
    this(connectorConfig, context, avroData, SYSTEM_TIME);

  }

  @SuppressWarnings("unchecked")
  public DataWriter(
      HdfsSinkConnectorConfig config,
      SinkTaskContext context,
      AvroData avroData,
      Time time
  ) {
    this.time = time;
    this.connectorConfig = config;
    this.avroData = avroData;
    this.context = context;
    topicDirs = new HashMap<>();
    logDirs = new HashMap<>();
    topicPartitionWriters = new HashMap<>();

    try {
      partitioner = newPartitioner(config);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new ConnectException(
          String.format("Unable to initialize partitioner: %s", e.getMessage()),
          e
      );
    }

    System.setProperty("hadoop.home.dir", config.hadoopHome());
    log.info("Hadoop configuration directory {}", config.hadoopConfDir());
    Configuration hadoopConfiguration = config.getHadoopConfiguration();
    if (!config.hadoopConfDir().equals("")) {
      hadoopConfiguration.addResource(new Path(config.hadoopConfDir() + "/core-site.xml"));
      hadoopConfiguration.addResource(new Path(config.hadoopConfDir() + "/hdfs-site.xml"));
    }

    // By default all FileSystem clients created through the java Hadoop clients are auto
    // closed at JVM shutdown. This can interfere with the normal shutdown logic of the connector
    // where the created clients are used to clean up temporary files (if the client was closed
    // prior, then this operation throws a Filesystem Closed error). To prevent this behavior we
    // set the Hadoop configuration fs.automatic.close to false. All created clients are closed as
    // part of the connector lifecycle. This is anyways necessary as during connector deletion the
    // connector lifecycle needs to close the clients, as the JVM shutdown hooks don't come into the
    // picture. Hence we should always operate with fs.automatic.close as false. If in future we
    // find that we are leaking client connections, we need to fix the lifecycle to close those.
    hadoopConfiguration.setBoolean("fs.automatic.close", false);

    if (config.kerberosAuthentication()) {
      configureKerberosAuthentication(hadoopConfiguration);
    }

    Class<? extends HdfsStorage> storageClass = config.storageClass();
    storage = io.confluent.connect.storage.StorageFactory.createStorage(
        storageClass,
        HdfsSinkConnectorConfig.class,
        config,
        connectorConfig.url()
    );

    for (TopicPartition tp : context.assignment()) {
      String topicDir = connectorConfig.getTopicsDirFromTopic(tp.topic());
      String logDir = connectorConfig.getLogsDirFromTopic(tp.topic());

      topicDirs.put(tp.topic(), topicDir);
      logDirs.put(tp.topic(), logDir);

      createDir(topicDir);
      createDir(topicDir + HdfsSinkConnectorConstants.TEMPFILE_DIRECTORY);
      createDir(logDir);
    }

    try {
      // Try to instantiate as a new-style storage-common type class, then fall back to old-style
      // with no parameters
      try {
        Class<io.confluent.connect.storage.format.Format> formatClass =
            (Class<io.confluent.connect.storage.format.Format>)
                config.getClass(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG);
        newFormat = formatClass.getConstructor(HdfsStorage.class).newInstance(storage);
        newWriterProvider = newFormat.getRecordWriterProvider();
        schemaFileReader = newFormat.getSchemaFileReader();
      } catch (NoSuchMethodException e) {
        Class<Format> formatClass =
            (Class<Format>) config.getClass(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG);
        format = formatClass.getConstructor().newInstance();
        writerProvider = format.getRecordWriterProvider();
        final io.confluent.connect.hdfs.SchemaFileReader oldReader
            = format.getSchemaFileReader(avroData);
        schemaFileReader = new SchemaFileReader<HdfsSinkConnectorConfig, Path>() {
          @Override
          public Schema getSchema(HdfsSinkConnectorConfig hdfsSinkConnectorConfig, Path path) {
            try {
              return oldReader.getSchema(hdfsSinkConnectorConfig.getHadoopConfiguration(), path);
            } catch (IOException e) {
              throw new ConnectException("Failed to get schema", e);
            }
          }

          @Override
          public Iterator<Object> iterator() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean hasNext() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object next() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void close() throws IOException {

          }
        };
      }

    } catch (IllegalAccessException
              | InstantiationException
              | InvocationTargetException
              | NoSuchMethodException e
    ) {
      throw new ConnectException("Reflection exception: ", e);
    }

    if (connectorConfig.hiveIntegrationEnabled()) {
      initializeHiveServices(hadoopConfiguration);
    }

    initializeTopicPartitionWriters(context.assignment());
  }

  private void configureKerberosAuthentication(Configuration hadoopConfiguration) {
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS,
        hadoopConfiguration
    );

    if (connectorConfig.connectHdfsPrincipal() == null
        || connectorConfig.connectHdfsKeytab() == null) {
      throw new ConfigException(
          "Hadoop is using Kerberos for authentication, you need to provide both a connect "
              + "principal and the path to the keytab of the principal.");
    }

    hadoopConfiguration.set("hadoop.security.authentication", "kerberos");
    hadoopConfiguration.set("hadoop.security.authorization", "true");

    try {
      String hostname = InetAddress.getLocalHost().getCanonicalHostName();

      String namenodePrincipal = SecurityUtil.getServerPrincipal(
          connectorConfig.hdfsNamenodePrincipal(),
          hostname
      );

      // namenode principal is needed for multi-node hadoop cluster
      if (hadoopConfiguration.get("dfs.namenode.kerberos.principal") == null) {
        hadoopConfiguration.set("dfs.namenode.kerberos.principal", namenodePrincipal);
      }
      log.info("Hadoop namenode principal: {}",
          hadoopConfiguration.get("dfs.namenode.kerberos.principal"));

      UserGroupInformation.setConfiguration(hadoopConfiguration);
      // replace the _HOST specified in the principal config to the actual host
      String principal = SecurityUtil.getServerPrincipal(
          connectorConfig.connectHdfsPrincipal(),
          hostname
      );
      UserGroupInformation.loginUserFromKeytab(principal, connectorConfig.connectHdfsKeytab());
      final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      log.info("Login as: " + ugi.getUserName());

      isRunning = true;
      ticketRenewThread = new Thread(() -> renewKerberosTicket(ugi));
    } catch (UnknownHostException e) {
      throw new ConnectException(
          String.format(
              "Could not resolve local hostname for Kerberos authentication: %s",
              e.getMessage()
          ),
          e
      );
    } catch (IOException e) {
      throw new ConnectException(
          String.format("Could not authenticate with Kerberos: %s", e.getMessage()),
          e
      );
    }

    log.info(
        "Starting the Kerberos ticket renew thread with period {} ms.",
        connectorConfig.kerberosTicketRenewPeriodMs()
    );
    ticketRenewThread.start();
  }

  private void initializeHiveServices(Configuration hadoopConfiguration) {
    hiveDatabase = connectorConfig.hiveDatabase();
    hiveMetaStore = new HiveMetaStore(hadoopConfiguration, connectorConfig);
    if (format != null) {
      hive = format.getHiveUtil(connectorConfig, hiveMetaStore);
    } else if (newFormat != null) {
      final io.confluent.connect.storage.hive.HiveUtil newHiveUtil
          = ((HiveFactory) newFormat.getHiveFactory())
          .createHiveUtil(connectorConfig, hiveMetaStore);
      hive = new HiveUtil(connectorConfig, hiveMetaStore) {
        @Override
        public void createTable(
            String database, String tableName, Schema schema,
            Partitioner partitioner, String topic
        ) {
          newHiveUtil.createTable(database, tableName, schema, partitioner, topic);
          log.debug("Created Hive table {}", tableName);
        }

        @Override
        public void alterSchema(String database, String tableName, Schema schema) {
          newHiveUtil.alterSchema(database, tableName, schema);
          log.debug("Altered Hive table {}", tableName);
        }
      };
    } else {
      throw new ConnectException("One of old or new format classes must be provided");
    }
    executorService = Executors.newSingleThreadExecutor();
    hiveUpdateFutures = new LinkedList<>();
  }

  private void initializeTopicPartitionWriters(Set<TopicPartition> assignment) {
    for (TopicPartition tp : assignment) {
      TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
          tp,
          storage,
          writerProvider,
          newWriterProvider,
          partitioner,
          connectorConfig,
          context,
          avroData,
          hiveMetaStore,
          hive,
          schemaFileReader,
          executorService,
          hiveUpdateFutures,
          time,
          connectorConfig.getHiveTableName(tp.topic())
      );
      topicPartitionWriters.put(tp, topicPartitionWriter);
    }
  }

  public void write(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition tp = new TopicPartition(topic, partition);
      topicPartitionWriters.get(tp).buffer(record);
    }

    if (connectorConfig.hiveIntegrationEnabled()) {
      Iterator<Future<Void>> iterator = hiveUpdateFutures.iterator();
      while (iterator.hasNext()) {
        try {
          Future<Void> future = iterator.next();
          if (future.isDone()) {
            future.get();
            iterator.remove();
          } else {
            break;
          }
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }

    for (TopicPartition tp : topicPartitionWriters.keySet()) {
      topicPartitionWriters.get(tp).write();
    }
  }

  public void recover(TopicPartition tp) {
    topicPartitionWriters.get(tp).recover();
  }

  public void syncWithHive() throws ConnectException {
    Set<String> topics = new HashSet<>();
    for (TopicPartition tp : topicPartitionWriters.keySet()) {
      topics.add(tp.topic());
    }

    try {
      for (String topic : topics) {
        String topicDir = FileUtils.topicDirectory(
            connectorConfig.url(),
            topicDirs.get(topic),
            topic
        );
        CommittedFileFilter filter = new TopicCommittedFileFilter(topic);
        FileStatus fileStatusWithMaxOffset = FileUtils.fileStatusWithMaxOffset(
            storage,
            new Path(topicDir),
            filter
        );
        if (fileStatusWithMaxOffset != null) {
          final Path path = fileStatusWithMaxOffset.getPath();
          final Schema latestSchema;
          latestSchema = schemaFileReader.getSchema(
              connectorConfig,
              path
          );
          String hiveTableName = connectorConfig.getHiveTableName(topic);
          hive.createTable(hiveDatabase, hiveTableName, latestSchema, partitioner, topic);
          List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase,
                  hiveTableName,
                  (short) -1);
          FileStatus[] statuses = FileUtils.getDirectories(storage, new Path(topicDir));
          for (FileStatus status : statuses) {
            String location = status.getPath().toString();
            if (!partitions.contains(location)) {
              String partitionValue = getPartitionValue(location);
              hiveMetaStore.addPartition(hiveDatabase, hiveTableName, partitionValue);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public void open(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
          tp,
          storage,
          writerProvider,
          newWriterProvider,
          partitioner,
          connectorConfig,
          context,
          avroData,
          hiveMetaStore,
          hive,
          schemaFileReader,
          executorService,
          hiveUpdateFutures,
          time,
          connectorConfig.getHiveTableName(tp.topic())
      );
      topicPartitionWriters.put(tp, topicPartitionWriter);
      // We need to immediately start recovery to ensure we pause consumption of messages for the
      // assigned topics while we try to recover offsets and rewind.
      recover(tp);
    }
  }

  public void checkWritersForNecessityOfRotation() {
    for (TopicPartitionWriter topicPartitionWriter : topicPartitionWriters.values()) {
      if (topicPartitionWriter.shouldRotateAndMaybeUpdateTimers()) {
        topicPartitionWriter.write();
      }
    }
  }

  public void close() {
    // Close any writers we have. We may get assigned the same partitions and end up duplicating
    // some effort since we'll have to reprocess those messages. It may be possible to hold on to
    // the TopicPartitionWriter and continue to use the temp file, but this can get significantly
    // more complex due to potential failures and network partitions. For example, we may get
    // this close, then miss a few generations of group membership, during which
    // data may have continued to be processed and we'd have to restart from the recovery stage,
    // make sure we apply the WAL, and only reuse the temp file if the starting offset is still
    // valid. For now, we prefer the simpler solution that may result in a bit of wasted effort.
    for (TopicPartitionWriter writer : topicPartitionWriters.values()) {
      try {
        if (writer != null) {
          // In some failure modes, the writer might not have been created for all assignments
          writer.close();
        }
      } catch (ConnectException e) {
        log.warn("Unable to close writer for topic partition {}: ", writer.topicPartition(), e);
      }
    }
    topicPartitionWriters.clear();
  }

  public void stop() {
    if (executorService != null) {
      boolean terminated = false;
      try {
        log.info("Shutting down Hive executor service.");
        executorService.shutdown();
        long shutDownTimeout = connectorConfig.getLong(
            HdfsSinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG
        );
        log.info("Awaiting termination.");
        terminated = executorService.awaitTermination(shutDownTimeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // ignored
      }

      if (!terminated) {
        log.warn(
            "Unclean Hive executor service shutdown, you probably need to sync with Hive next "
                + "time you start the connector"
        );
        executorService.shutdownNow();
      }
    }

    storage.close();

    if (ticketRenewThread != null) {
      synchronized (this) {
        isRunning = false;
        this.notifyAll();
      }
    }
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  /**
   * By convention, the consumer stores the offset that corresponds to the next record to consume.
   * To follow this convention, this methods returns each offset that is one more than the last
   * offset committed to HDFS.
   *
   * @return Map from TopicPartition to next offset after the most recently committed offset to HDFS
   */
  public Map<TopicPartition, Long> getCommittedOffsets() {
    Map<TopicPartition, Long> offsets = new HashMap<>();
    log.debug("Writer looking for last offsets for topic partitions {}",
        topicPartitionWriters.keySet()
    );
    for (TopicPartition tp : topicPartitionWriters.keySet()) {
      long committedOffset = topicPartitionWriters.get(tp).offset();
      log.debug("Writer found last offset {} for topic partition {}", committedOffset, tp);
      if (committedOffset >= 0) {
        offsets.put(tp, committedOffset);
      }
    }
    return offsets;
  }

  public TopicPartitionWriter getBucketWriter(TopicPartition tp) {
    return topicPartitionWriters.get(tp);
  }

  public Storage getStorage() {
    return storage;
  }

  Map<String, io.confluent.connect.storage.format.RecordWriter> getWriters(TopicPartition tp) {
    return topicPartitionWriters.get(tp).getWriters();
  }

  public Map<String, String> getTempFileNames(TopicPartition tp) {
    TopicPartitionWriter topicPartitionWriter = topicPartitionWriters.get(tp);
    return topicPartitionWriter.getTempFiles();
  }

  private void createDir(String dir) {
    String path = connectorConfig.url() + "/" + dir;
    if (!storage.exists(path)) {
      log.trace("Creating directory {}", path);
      storage.create(path);
    }
  }

  private Partitioner newPartitioner(HdfsSinkConnectorConfig config)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    Partitioner partitioner;
    try {
      @SuppressWarnings("unchecked")
      Class<? extends Partitioner> partitionerClass =
          (Class<? extends Partitioner>)
              config.getClass(PartitionerConfig.PARTITIONER_CLASS_CONFIG);
      partitioner = partitionerClass.newInstance();
    } catch (ClassCastException e) {
      @SuppressWarnings("unchecked")
      Class<? extends io.confluent.connect.storage.partitioner.Partitioner<FieldSchema>>
          partitionerClass =
          (Class<? extends io.confluent.connect.storage.partitioner.Partitioner<FieldSchema>>)
              config.getClass(PartitionerConfig.PARTITIONER_CLASS_CONFIG);
      partitioner = new PartitionerWrapper(partitionerClass.newInstance());
    }

    partitioner.configure(new HashMap<>(config.plainValues()));
    return partitioner;
  }

  public static class PartitionerWrapper implements Partitioner {
    public final io.confluent.connect.storage.partitioner.Partitioner<FieldSchema>  partitioner;

    public PartitionerWrapper(
        io.confluent.connect.storage.partitioner.Partitioner<FieldSchema> partitioner
    ) {
      this.partitioner = partitioner;
    }

    @Override
    public void configure(Map<String, Object> config) {
      partitioner.configure(config);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
      return partitioner.encodePartition(sinkRecord);
    }

    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
      return partitioner.generatePartitionedPath(topic, encodedPartition);
    }

    @Override
    public List<FieldSchema> partitionFields() {
      return partitioner.partitionFields();
    }
  }

  private String getPartitionValue(String path) {
    String[] parts = path.split("/");
    StringBuilder sb = new StringBuilder();
    sb.append("/");
    for (int i = 3; i < parts.length; ++i) {
      sb.append(parts[i]);
      sb.append("/");
    }
    return sb.toString();
  }

  private Partitioner createPartitioner(HdfsSinkConnectorConfig config)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    @SuppressWarnings("unchecked")
    Class<? extends Partitioner> partitionerClasss = (Class<? extends Partitioner>)
        Class.forName(config.getString(PartitionerConfig.PARTITIONER_CLASS_CONFIG));

    Map<String, Object> map = copyConfig(config);
    Partitioner partitioner = partitionerClasss.newInstance();
    partitioner.configure(map);
    return partitioner;
  }

  private Map<String, Object> copyConfig(HdfsSinkConnectorConfig config) {
    Map<String, Object> map = new HashMap<>();
    map.put(
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG,
        config.getString(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG)
    );
    map.put(
        PartitionerConfig.PARTITION_DURATION_MS_CONFIG,
        config.getLong(PartitionerConfig.PARTITION_DURATION_MS_CONFIG)
    );
    map.put(
        PartitionerConfig.PATH_FORMAT_CONFIG,
        config.getString(PartitionerConfig.PATH_FORMAT_CONFIG)
    );
    map.put(PartitionerConfig.LOCALE_CONFIG, config.getString(PartitionerConfig.LOCALE_CONFIG));
    map.put(PartitionerConfig.TIMEZONE_CONFIG, config.getString(PartitionerConfig.TIMEZONE_CONFIG));
    return map;
  }

  private void renewKerberosTicket(UserGroupInformation ugi) {
    synchronized (DataWriter.this) {
      while (isRunning) {
        try {
          DataWriter.this.wait(connectorConfig.kerberosTicketRenewPeriodMs());
          if (isRunning) {
            log.debug("Attempting re-login from keytab for user: {}", ugi.getUserName());
            ugi.reloginFromKeytab();
          }
        } catch (IOException e) {
          // We ignore this exception during relogin as each successful relogin gives
          // additional 24 hours of authentication in the default config. In normal
          // situations, the probability of failing relogin 24 times is low and if
          // that happens, the task will fail eventually.
          log.error("Error renewing the ticket", e);
        } catch (InterruptedException e) {
          // ignored
        }
      }
    }
  }
}
