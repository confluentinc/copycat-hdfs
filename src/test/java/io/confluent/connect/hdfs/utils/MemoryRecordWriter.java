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

package io.confluent.connect.hdfs.utils;

import io.confluent.connect.hdfs.FileSizeAwareRecordWriter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

public class MemoryRecordWriter implements FileSizeAwareRecordWriter {
  private String filename;
  private static final Map<String, List<Object>> data = Data.getData();
  private Failure failure = Failure.noFailure;

  public enum Failure {
    noFailure,
    writeFailure,
    closeFailure
  }

  public MemoryRecordWriter(String filename) {
    this.filename = filename;
  }

  @Override
  public void write(SinkRecord record) {
    if (failure == Failure.writeFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("write failed.");
    }
    data.get(filename).add(record);
  }

  @Override
  public void commit() {}

  @Override
  public void close() {
    if (failure == Failure.closeFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("close failed.");
    }
  }

  public void setFailure(Failure failure) {
    this.failure = failure;
  }

  @Override
  public long getFileSize() {
    return 0;
  }
}
