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

package io.confluent.connect.hdfs.parquet;

import io.confluent.connect.hdfs.FileSizeAwareRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;

public class ParquetRecordWriterProvider
    implements io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";
  private final AvroData avroData;

  ParquetRecordWriterProvider(AvroData avroData) {
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public FileSizeAwareRecordWriter getRecordWriter(HdfsSinkConnectorConfig conf, String filename) {
    return new FileSizeAwareRecordWriter() {
      private long fileSize;
      final CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
      final int blockSize = 256 * 1024 * 1024;
      final int pageSize = 64 * 1024;
      Path path = new Path(filename);
      Schema schema;
      ParquetWriter<GenericRecord> writer;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          // may still be null at this point
        }

        if (writer == null) {
          try {
            log.info("Opening record writer for: {}", filename);
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
            writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(avroSchema)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize(blockSize)
                .withPageSize(pageSize)
                .withDictionaryEncoding(true)
                .withConf(conf.getHadoopConfiguration())
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
            log.debug("Opened record writer for: {}", filename);
          } catch (IOException e) {
            // Ultimately caught and logged in TopicPartitionWriter,
            // but log in debug to provide more context
            log.warn(
                "Error creating {} for file '{}', {}, and schema {}: ",
                AvroParquetWriter.class.getSimpleName(),
                filename,
                compressionCodecName,
                schema,
                e
            );
            throw new ConnectException(e);
          }
        }

        Object value = avroData.fromConnectData(record.valueSchema(), record.value());
        try {
          writer.write((GenericRecord) value);
          fileSize = writer.getDataSize();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {
        if (writer != null) {
          try {
            writer.close();
            // ParquetWriter keeps track of buffer and the actual written size,
            // so we do not need to update fileSize
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
      }

      @Override
      public void commit() {}

      @Override
      public long getFileSize() {
        return fileSize;
      }
    };
  }
}
