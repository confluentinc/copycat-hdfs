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

package io.confluent.connect.hdfs.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A wrapper for `DataFileWriter` which exposes the inner file stream. This is helpful for
 * monitoring the file size of the underlying file.
 */
public class TransparentDataFileWriter<D> implements Closeable, Flushable {
  private final DataFileWriter<D> dataFileWriter;

  private FSDataOutputStream innerFileStream;

  public TransparentDataFileWriter(DataFileWriter<D> dataFileWriter) {
    this.dataFileWriter = dataFileWriter;
  }

  public DataFileWriter<D> create(Schema schema, File file) throws IOException {
    throw new NotImplementedException();
  }

  public DataFileWriter<D> create(Schema schema, FSDataOutputStream outs) throws IOException {
    this.innerFileStream = outs;
    return dataFileWriter.create(schema, outs);
  }

  public DataFileWriter<D> create(
      Schema schema,
      FSDataOutputStream outs,
      byte[] sync
  ) throws IOException {
    this.innerFileStream = outs;
    return dataFileWriter.create(schema, outs, sync);
  }

  public DataFileWriter<D> create(
      Schema schema,
      OutputStream outs,
      byte[] sync
  ) throws IOException {
    return dataFileWriter.create(schema, outs, sync);
  }

  public DataFileWriter<D> setCodec(CodecFactory c) {
    return dataFileWriter.setCodec(c);
  }

  public DataFileWriter<D> setSyncInterval(int syncInterval) {
    return dataFileWriter.setSyncInterval(syncInterval);
  }

  public void setFlushOnEveryBlock(boolean flushOnEveryBlock) {
    dataFileWriter.setFlushOnEveryBlock(flushOnEveryBlock);
  }

  public boolean isFlushOnEveryBlock() {
    return dataFileWriter.isFlushOnEveryBlock();
  }

  public DataFileWriter<D> appendTo(File file) throws IOException {
    return dataFileWriter.appendTo(file);
  }

  public DataFileWriter<D> appendTo(SeekableInput in, OutputStream out) throws IOException {
    return dataFileWriter.appendTo(in, out);
  }

  public DataFileWriter<D> setMeta(String key, byte[] value) {
    return dataFileWriter.setMeta(key, value);
  }

  public DataFileWriter<D> setMeta(String key, String value) {
    return dataFileWriter.setMeta(key, value);
  }

  public DataFileWriter<D> setMeta(String key, long value) {
    return dataFileWriter.setMeta(key, value);
  }

  public static boolean isReservedMeta(String key) {
    return DataFileWriter.isReservedMeta(key);
  }

  public void append(D datum) throws IOException {
    dataFileWriter.append(datum);
  }

  public void appendEncoded(ByteBuffer datum) throws IOException {
    dataFileWriter.appendEncoded(datum);
  }

  public void appendAllFrom(DataFileStream<D> otherFile, boolean recompress) throws IOException {
    dataFileWriter.appendAllFrom(otherFile, recompress);
  }

  public long sync() throws IOException {
    return dataFileWriter.sync();
  }

  @Override
  public void flush() throws IOException {
    dataFileWriter.flush();
  }

  @Override
  public void close() throws IOException {
    dataFileWriter.close();
  }

  public FSDataOutputStream getInnerFileStream() {
    return innerFileStream;
  }
}
