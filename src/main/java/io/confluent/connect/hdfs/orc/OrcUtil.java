/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.hdfs.orc;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class OrcUtil {

  /**
   * Create an object of OrcStruct given a type string and a list of objects
   */
  public static OrcStruct createOrcStruct(TypeInfo typeInfo, Object... objs) {
    SettableStructObjectInspector oi = (SettableStructObjectInspector) 
            OrcStruct.createObjectInspector(typeInfo);
    @SuppressWarnings("unchecked")
    List<StructField> fields = (List<StructField>) oi.getAllStructFieldRefs();
    OrcStruct result = (OrcStruct) oi.create();
    result.setNumFields(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      oi.setStructFieldData(result, fields.get(i), objs[i]);
    }
    return result;
  }

  /**
   * Convert a Struct into a Writable array
   */
  public static Object[] convertStruct(Struct struct) {
    List<Object> data = new LinkedList<>();
    for (Field f : struct.schema().fields()) {
      if (struct.get(f) == null) {
        data.add((Writable) null);
      } else {
        Schema.Type schemaType = f.schema().type();
        switch (schemaType) {
          case BOOLEAN:
            data.add(new BooleanWritable(struct.getBoolean(f.name())));
            break;
          case STRING:
            data.add(new Text(struct.getString(f.name())));
            break;
          case BYTES:
            data.add(new BytesWritable(struct.getBytes(f.name())));
            break;
          case INT8:
            data.add(new ByteWritable(struct.getInt8(f.name())));
            break;
          case INT16:
            data.add(new ShortWritable(struct.getInt16(f.name())));
            break;
          case INT32:
            if (Date.LOGICAL_NAME.equals(f.schema().name())) {
              java.util.Date date = (java.util.Date) struct.get(f);
              data.add(new DateWritable(new java.sql.Date(date.getTime())));
            } else if (Time.LOGICAL_NAME.equals(f.schema().name())) {
              java.util.Date date = (java.util.Date) struct.get(f);
              data.add(new TimestampWritable(new java.sql.Timestamp(date.getTime())));
            } else {
              data.add(new IntWritable(struct.getInt32(f.name())));
            }
            break;
          case INT64:
            if (Timestamp.LOGICAL_NAME.equals(f.schema().name())) {
              java.util.Date date = (java.util.Date) struct.get(f);
              data.add(new TimestampWritable(new java.sql.Timestamp(date.getTime())));
            } else {
              data.add(new LongWritable(struct.getInt64(f.name())));
            }
            break;
          case FLOAT32:
            data.add(new FloatWritable(struct.getFloat32(f.name())));
            break;
          case FLOAT64:
            data.add(new DoubleWritable(struct.getFloat64(f.name())));
            break;
          case ARRAY:
            data.add(new ArrayPrimitiveWritable(struct.getArray(f.name()).toArray()));
            break;
          case MAP:
            MapWritable mapWritable = new MapWritable();
            Map<Object, Object> map = struct.getMap(f.name());
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
              mapWritable.put(new ObjectWritable(entry.getKey()),
                              new ObjectWritable(entry.getValue()));
            }
            data.add(mapWritable);
            break;
          case STRUCT:
            data.add(convertStruct(struct.getStruct(f.name())));
            break;
          default:
            break;
        }
      }
    }

    return data.toArray(new Object[0]);
  }

}
