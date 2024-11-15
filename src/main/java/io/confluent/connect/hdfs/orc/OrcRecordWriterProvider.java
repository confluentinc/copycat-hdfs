/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.connect.hdfs.orc;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.FileSizeAwareRecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrcRecordWriterProvider implements RecordWriterProvider<HdfsSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(OrcRecordWriterProvider.class);
  private static final String EXTENSION = ".orc";

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public FileSizeAwareRecordWriter getRecordWriter(HdfsSinkConnectorConfig conf, String filename) {
    Path path = new Path(filename);

    return new FileSizeAwareRecordWriter() {
      private long fileSize;
      Writer writer;
      TypeInfo typeInfo;
      Schema schema;

      @Override
      public void write(SinkRecord record) {
        try {
          if (schema == null) {
            schema = record.valueSchema();
            if (schema.type() == Schema.Type.STRUCT) {

              OrcFile.WriterCallback writerCallback = new OrcFile.WriterCallback() {
                @Override
                public void preStripeWrite(OrcFile.WriterContext writerContext) {
                }

                @Override
                public void preFooterWrite(OrcFile.WriterContext writerContext) {
                }
              };

              typeInfo = HiveSchemaConverter.convertMaybeLogical(schema);
              ObjectInspector objectInspector = OrcStruct.createObjectInspector(typeInfo);

              log.info("Opening ORC record writer for: {}", filename);
              writer = OrcFile
                  .createWriter(path, OrcFile.writerOptions(conf.getHadoopConfiguration())
                      .inspector(objectInspector)
                      .callback(writerCallback));
            }
          }

          if (schema.type() == Schema.Type.STRUCT) {
            log.trace(
                "Writing record from topic {} partition {} offset {}",
                record.topic(),
                record.kafkaPartition(),
                record.kafkaOffset()
            );

            Struct struct = (Struct) record.value();
            OrcStruct row = (OrcStruct) OrcUtil.convert(typeInfo, struct.schema(), struct);
            writer.addRow(row);

          } else {
            throw new ConnectException(
                "Top level type must be STRUCT but was " + schema.type().getName()
            );
          }
          fileSize = writer.getRawDataSize();
        } catch (IOException e) {
          throw new ConnectException("Failed to write record: ", e);
        }
      }

      @Override
      public void close() {
        try {
          if (writer != null) {
            writer.close();
            fileSize = writer.getRawDataSize();
          }
        } catch (IOException e) {
          throw new ConnectException("Failed to close ORC writer:", e);
        }
      }

      @Override
      public void commit() { }

      @Override
      public long getFileSize() {
        return fileSize;
      }
    };
  }
}
