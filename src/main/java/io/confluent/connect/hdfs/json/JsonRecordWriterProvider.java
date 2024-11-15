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

package io.confluent.connect.hdfs.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.hdfs.FileSizeAwareRecordWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.RecordWriterProvider;

/**
 * Provider of a JSON record writer.
 */
public class JsonRecordWriterProvider implements RecordWriterProvider<HdfsSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
  private static final String EXTENSION = ".json";
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private static final byte[] LINE_SEPARATOR_BYTES
      = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);
  private final HdfsStorage storage;
  private final ObjectMapper mapper;
  private final JsonConverter converter;

  /**
   * Constructor.
   *
   * @param storage the underlying storage implementation.
   * @param converter the JSON converter to be used to convert records from Kafka Connect format.
   */
  JsonRecordWriterProvider(HdfsStorage storage, JsonConverter converter) {
    this.storage = storage;
    this.mapper = new ObjectMapper();
    this.converter = converter;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public FileSizeAwareRecordWriter getRecordWriter(HdfsSinkConnectorConfig conf, String filename) {
    try {
      return new FileSizeAwareRecordWriter() {

        private long fileSize;
        final FSDataOutputStream out = storage.create(filename, true);
        final JsonGenerator writer = mapper.getFactory()
            .createGenerator((OutputStream) out)
            .setRootValueSeparator(null);

        @Override
        public void write(SinkRecord record) {
          try {
            Object value = record.value();
            if (value instanceof Struct) {
              byte[] rawJson = converter.fromConnectData(
                  record.topic(),
                  record.valueSchema(),
                  value
              );
              out.write(rawJson);
              out.write(LINE_SEPARATOR_BYTES);
            } else {
              writer.writeObject(value);
              writer.writeRaw(LINE_SEPARATOR);
            }
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
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }
}
