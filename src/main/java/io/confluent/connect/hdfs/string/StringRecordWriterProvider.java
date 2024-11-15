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

package io.confluent.connect.hdfs.string;

import io.confluent.connect.hdfs.FileSizeAwareRecordWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.RecordWriterProvider;

/**
 * Provider of a text record writer.
 */
public class StringRecordWriterProvider implements RecordWriterProvider<HdfsSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(StringRecordWriterProvider.class);
  private static final String EXTENSION = ".txt";
  public static final int WRITER_BUFFER_SIZE = 128 * 1024;
  private final HdfsStorage storage;

  /**
   * Constructor.
   *
   * @param storage the underlying storage implementation.
   */
  StringRecordWriterProvider(HdfsStorage storage) {
    this.storage = storage;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public FileSizeAwareRecordWriter getRecordWriter(
      final HdfsSinkConnectorConfig conf,
      final String filename
  ) {
    return new FileSizeAwareRecordWriter() {
      private long fileSize;
      final FSDataOutputStream out = storage.create(filename, true);
      final OutputStreamWriter streamWriter = new OutputStreamWriter(out, Charset.defaultCharset());
      final BufferedWriter writer = new BufferedWriter(streamWriter, WRITER_BUFFER_SIZE);

      @Override
      public void write(SinkRecord record) {
        try {
          String value = (String) record.value();
          writer.write(value);
          writer.newLine();
          fileSize = out.getPos();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {}

      @Override
      public void close() {
        try {
          writer.close();
          fileSize = out.getPos();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public long getFileSize() {
        return fileSize;
      }
    };
  }
}
