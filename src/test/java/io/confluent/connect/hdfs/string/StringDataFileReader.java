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

import io.confluent.connect.hdfs.DataFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;

import static io.confluent.connect.hdfs.HdfsSinkConnectorConfig.STRING_FORMAT_COMPRESSION_NONE;

public class StringDataFileReader implements DataFileReader {

  private CompressionCodec compressionCodec;

  public StringDataFileReader(Configuration conf, String compression) {
    if (compression != null && !compression.equalsIgnoreCase(STRING_FORMAT_COMPRESSION_NONE)) {
      CompressionCodecFactory factory = new CompressionCodecFactory(conf);
      compressionCodec = factory.getCodecByName(compression);
    }
  }

  @Override
  public Collection<Object> readData(Configuration conf, Path path) throws IOException {
    String uri = "hdfs://127.0.0.1:9001";
    FileSystem fs;
    try {
      fs = FileSystem.get(new URI(uri), conf);
    } catch (URISyntaxException e) {
      throw new IOException("Failed to create URI: " + uri);
    }
    InputStream in = fs.open(path);
    if (compressionCodec != null) {
      in = compressionCodec.createInputStream(in);
    }

    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    ArrayList<Object> records = new ArrayList<>();

    String line;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
      records.add(line);
    }

    reader.close();
    return records;
  }
}
