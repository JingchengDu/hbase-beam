/**
 *
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
package org.apache.hadoop.hbase.beam;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.beam.io.HBaseCellCoder;
import org.apache.hadoop.hbase.beam.io.SerializableBytesArray;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Instant;

public class HBasePipelineFunctions {

  public static <I, O> PCollection<O> bulkGet(Configuration conf, String tableName, int batchSize,
      PCollection<I> inputs, MakeFunction<I, Get> inputFunc, MakeFunction<I, Instant> instantFunc,
      MakeFunction<Result, O> outputFunc, Coder<O> outputCoder) {
    SerializableConfiguration serConf = new SerializableConfiguration(conf);
    return inputs.apply("bulkGet" + UUID.randomUUID().toString(), new BulkGetTransform<>(serConf,
        tableName, batchSize, inputFunc, instantFunc, outputFunc, outputCoder));
  }

  public static <I> PDone bulkPut(Configuration conf, String tableName, PCollection<I> inputs,
      MakeFunction<I, Mutation> inputFunc) {
    SerializableConfiguration serConf = new SerializableConfiguration(conf);
    return inputs.apply("bulkPut" + UUID.randomUUID().toString(),
      new BulkMutateTransform<>(serConf, tableName, inputFunc));
  }

  public static <I> PDone bulkDelete(Configuration conf, String tableName, PCollection<I> inputs,
      MakeFunction<I, Mutation> inputFunc) {
    SerializableConfiguration serConf = new SerializableConfiguration(conf);
    return inputs.apply("bulkDelete" + UUID.randomUUID().toString(),
      new BulkMutateTransform<>(serConf, tableName, inputFunc));
  }

  public static <I> void bulkLoad(Configuration conf, String tableName, PCollection<I> inputs,
      URI stagingPathUri, URI tmpPathUri, MakeFunction<I, Cell> cellFunc) throws Exception {
    bulkLoad(conf, tableName, inputs, stagingPathUri, tmpPathUri, cellFunc, false);
  }

  public static <I> void bulkLoad(Configuration conf, String tableName, PCollection<I> inputs,
      URI stagingPathUri, URI tmpPathUri, MakeFunction<I, Cell> cellFunc,
      boolean minorCompactionExclude) throws Exception {
    Path stagingPath = new Path(stagingPathUri);
    FileSystem fs = stagingPath.getFileSystem(conf);
    if (fs.exists(stagingPath)) {
      throw new FileAlreadyExistsException("Path " + stagingPath + " already exists");
    }
    Path tmpPath = new Path(tmpPathUri);
    if (fs.exists(tmpPath)) {
      throw new FileAlreadyExistsException("Path " + tmpPath + " already exists");
    }
    SerializableConfiguration serConf = new SerializableConfiguration(conf);
    Connection conn = ConnectionFactory.createConnection(conf);
    Admin admin = null;
    Table table = null;
    try {
      admin = conn.getAdmin();
      List<HRegionInfo> regions = admin.getTableRegions(TableName.valueOf(tableName));
      int regionNum = regions.size();
      byte[][] startKeys = new byte[regionNum][];
      for (int i = 0; i < regionNum; i++) {
        startKeys[i] = regions.get(i).getStartKey();
      }
      Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
      SerializableBytesArray serStartKeys = new SerializableBytesArray(startKeys);
      inputs.getPipeline().getCoderRegistry().registerCoderForClass(Cell.class,
        HBaseCellCoder.of());
      inputs.apply("bulkLoad" + UUID.randomUUID().toString(),
        new BulkLoadTransform<>(serConf, tableName, stagingPathUri, tmpPathUri, serStartKeys, false,
            cellFunc, minorCompactionExclude));
      inputs.getPipeline().run().waitUntilFinish();
      // bulk load the directory into HBase
      LoadIncrementalHFiles bulkload = new LoadIncrementalHFiles(conf);
      table = conn.getTable(TableName.valueOf(tableName));
      bulkload.doBulkLoad(stagingPath, admin, table,
        conn.getRegionLocator(TableName.valueOf(tableName)));
    } finally {
      try {
        if (table != null) {
          table.close();
        }
      } catch (IOException e) {
      }
      try {
        if (admin != null) {
          admin.close();
        }
      } catch (IOException e) {
      }
      try {
        conn.close();
      } catch (IOException e) {
      }
    }
  }

  public static <I> void bulkLoadThinRows(Configuration conf, String tableName,
      PCollection<I> inputs, URI stagingPathUri, URI tmpPathUri, MakeFunction<I, byte[]> rowKeyFunc,
      MakeFunction<I, Iterable<Cell>> cellsFunc, Coder<I> inputCoder) throws Exception {
    bulkLoadThinRows(conf, tableName, inputs, stagingPathUri, tmpPathUri, rowKeyFunc, cellsFunc,
      inputCoder, false);
  }

  public static <I> void bulkLoadThinRows(Configuration conf, String tableName,
      PCollection<I> inputs, URI stagingPathUri, URI tmpPathUri, MakeFunction<I, byte[]> rowKeyFunc,
      MakeFunction<I, Iterable<Cell>> cellsFunc, Coder<I> inputCoder,
      boolean minorCompactionExclude) throws Exception {
    Path stagingPath = new Path(stagingPathUri);
    FileSystem fs = stagingPath.getFileSystem(conf);
    if (fs.exists(stagingPath)) {
      throw new FileAlreadyExistsException("Path " + stagingPath + " already exists");
    }
    Path tmpPath = new Path(tmpPathUri);
    if (fs.exists(tmpPath)) {
      throw new FileAlreadyExistsException("Path " + tmpPath + " already exists");
    }
    SerializableConfiguration serConf = new SerializableConfiguration(conf);
    Connection conn = ConnectionFactory.createConnection(conf);
    Admin admin = null;
    Table table = null;
    try {
      admin = conn.getAdmin();
      List<HRegionInfo> regions = admin.getTableRegions(TableName.valueOf(tableName));
      int regionNum = regions.size();
      byte[][] startKeys = new byte[regionNum][];
      for (int i = 0; i < regionNum; i++) {
        startKeys[i] = regions.get(i).getStartKey();
      }
      Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
      SerializableBytesArray serStartKeys = new SerializableBytesArray(startKeys);
      inputs.getPipeline().getCoderRegistry().registerCoderForClass(Cell.class,
        HBaseCellCoder.of());
      inputs.apply("bulkLoadThinRows" + UUID.randomUUID().toString(),
        new BulkLoadThinRowsTransform<>(serConf, tableName, stagingPathUri, tmpPathUri,
            serStartKeys, false, rowKeyFunc, cellsFunc, inputCoder, minorCompactionExclude));
      inputs.getPipeline().run().waitUntilFinish();
      // bulk load the directory into HBase
      LoadIncrementalHFiles bulkload = new LoadIncrementalHFiles(conf);
      table = conn.getTable(TableName.valueOf(tableName));
      bulkload.doBulkLoad(stagingPath, admin, table,
        conn.getRegionLocator(TableName.valueOf(tableName)));
    } finally {
      try {
        if (table != null) {
          table.close();
        }
      } catch (IOException e) {
      }
      try {
        if (admin != null) {
          admin.close();
        }
      } catch (IOException e) {
      }
      try {
        conn.close();
      } catch (IOException e) {
      }
    }
  }

  public static <I> PDone foreachPartition(Configuration conf, PCollection<I> inputs,
      PartitionKeyFunc<I> partitionKeyFunc, ForeachPartitionFunc<I> foreachPartitionFunc) {
    SerializableConfiguration serConf = new SerializableConfiguration(conf);
    return inputs.apply("foreachPartition" + UUID.randomUUID().toString(),
      new ForeachPartitionTransform<>(serConf, partitionKeyFunc, foreachPartitionFunc));
  }

  public static <I, O> PCollection<KV<byte[], Iterable<O>>> mapPartitions(Configuration conf,
      PCollection<I> inputs, PartitionKeyFunc<I> partitionKeyFunc,
      MapPartitionsFunc<I, O> mapPartitionsFunc, Coder<O> outputCoder) {
    SerializableConfiguration serConf = new SerializableConfiguration(conf);
    return inputs.apply("mapPartitions" + UUID.randomUUID().toString(),
      new MapPartitionsTransform<>(serConf, partitionKeyFunc, mapPartitionsFunc, outputCoder));
  }
}
