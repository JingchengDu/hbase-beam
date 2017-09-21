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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.beam.io.HBaseResultCoder;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("serial")
public class TestBulkOperations implements Serializable {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Admin admin;
  private static Configuration conf;
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[][] FAMILIES =
      new byte[][] { Bytes.toBytes("fam0"), Bytes.toBytes("fam1"), Bytes.toBytes("fam2") };
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[][] QUALIFIERS =
      new byte[][] { Bytes.toBytes("qualifier0"), Bytes.toBytes("qualifier1") };
  private static final byte[][] SPLIT_KEYS = { Bytes.toBytes("aaa"), Bytes.toBytes("bbb") };

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1, 2);
    conf = TEST_UTIL.getConfiguration();
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (admin != null) {
      admin.close();
    }
    if (TEST_UTIL != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testIO() {
    PCollection<String> inputs =
        pipeline.apply(TextIO.read().from("/Users/jingcheng/Desktop/input.txt"));
    inputs.apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
        for (String e : c.element().split(" ")) {
          c.output(e);
        }
      }
    })).apply(Count.<String> perElement())
        .apply("FormatResults", ParDo.of(new DoFn<KV<String, Long>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
            KV<String, Long> kv = c.element();
            c.output(kv.getKey() + ":" + kv.getValue());
          }
        })).apply(TextIO.write().to("/Users/jingcheng/Desktop/output.txt").withSuffix(".txt")
            .withNumShards(1));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testBulkGet() throws Exception {
    String tableNameAsString = "testBulkGet";
    TableName tn = TableName.valueOf(tableNameAsString);
    createTable(tn);
    int rowNum = 10;
    writeData(tn, rowNum);
    List<byte[]> rows = createRows(rowNum);
    PCollection<byte[]> inputs =
        pipeline.apply("createDataset" + UUID.randomUUID().toString(), Create.of(rows));
    PCollection<byte[]> results = HBasePipelineFunctions.bulkGet(conf, tableNameAsString, 10,
      inputs, new MakeFunction<byte[], Get>() {

        @Override
        public Get make(byte[] input) {
          return new Get(input);
        }
      }, null, new MakeFunction<Result, byte[]>() {

        @Override
        public byte[] make(Result input) {
          return input.getRow();
        }
      }, ByteArrayCoder.of());
    long expectedValue = SPLIT_KEYS.length * rowNum;
    PAssert.thatSingleton(results.apply("count", Count.<byte[]> globally()))
        .isEqualTo(expectedValue);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testBulkPut() throws IOException {
    String tableNameAsString = "testBulkPut";
    TableName tn = TableName.valueOf(tableNameAsString);
    createTable(tn);
    int rowNum = 50;
    List<String> cellStrings = createKeyAndValueStrings(rowNum);
    PCollection<String> inputs =
        pipeline.apply("createDataset" + UUID.randomUUID().toString(), Create.of(cellStrings));
    HBasePipelineFunctions.bulkPut(conf, tableNameAsString, inputs,
      new MakeFunction<String, Mutation>() {

        @Override
        public Mutation make(String input) {
          String[] strs = input.split(" ");
          if (strs.length == 2) {
            Put put = new Put(Bytes.toBytes(strs[0]));
            put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(strs[1]));
            return put;
          } else {
            return null;
          }
        }
      });
    pipeline.run().waitUntilFinish();
    Table table = null;
    try {
      table = TEST_UTIL.getConnection().getTable(tn);
      ResultScanner scanner = table.getScanner(new Scan());
      Result r = null;
      int count = 0;
      do {
        r = scanner.next();
        if (r != null) {
          count++;
        }
      } while (r != null);
      Assert.assertEquals(rowNum * SPLIT_KEYS.length, count);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testBulkDelete() throws IOException {
    String tableNameAsString = "testBulkDelete";
    TableName tn = TableName.valueOf(tableNameAsString);
    createTable(tn);
    int rowNum = 50;
    writeData(tn, rowNum);
    List<byte[]> deleteRows = createRows(rowNum / 2);
    PCollection<byte[]> inputs =
        pipeline.apply("createDataset" + UUID.randomUUID().toString(), Create.of(deleteRows));
    HBasePipelineFunctions.bulkDelete(conf, tableNameAsString, inputs,
      new MakeFunction<byte[], Mutation>() {

        @Override
        public Mutation make(byte[] input) {
          return new Delete(input);
        }
      });
    pipeline.run().waitUntilFinish();
    Table table = null;
    try {
      table = TEST_UTIL.getConnection().getTable(tn);
      ResultScanner scanner = table.getScanner(new Scan());
      Result r = null;
      int count = 0;
      do {
        r = scanner.next();
        if (r != null) {
          count++;
        }
      } while (r != null);
      Assert.assertEquals(rowNum * SPLIT_KEYS.length - deleteRows.size(), count);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testBulkLoad() throws Exception {
    String tableNameAsString = "testBulkLoad";
    TableName tn = TableName.valueOf(tableNameAsString);
    createTableWithMultipleFamilies(tn);
    int rowNum = 100;
    List<String> cellStrings = createCellStringsWithFamilies(rowNum);
    PCollection<String> inputs = pipeline.apply("createDataset" + UUID.randomUUID().toString(),
      Create.of(cellStrings).withCoder(StringUtf8Coder.of()));
    Path stagingPath =
        new Path(TEST_UTIL.getDataTestDir(), UUID.randomUUID().toString().replaceAll("-", ""));
    Path tmpPath =
        new Path(TEST_UTIL.getDataTestDir(), UUID.randomUUID().toString().replaceAll("-", ""));
    HBasePipelineFunctions.bulkLoad(conf, tableNameAsString, inputs, stagingPath.toUri(),
      tmpPath.toUri(), new MakeFunction<String, Cell>() {

        @Override
        public Cell make(String input) {
          String[] strs = input.split(" ");
          if (strs.length == 4) {
            return new KeyValue(Bytes.toBytes(strs[0]), Bytes.toBytes(strs[1]),
                Bytes.toBytes(strs[2]), EnvironmentEdgeManager.currentTime(),
                Bytes.toBytes(strs[3]));
          }
          return null;
        }
      });
    Table table = null;
    try {
      table = TEST_UTIL.getConnection().getTable(tn);
      ResultScanner scanner = table.getScanner(new Scan());
      Result r = null;
      int count = 0;
      do {
        r = scanner.next();
        if (r != null) {
          for (byte[] family : FAMILIES) {
            if (r.getValue(family, QUALIFIER) != null) {
              count++;
            }
          }
        }
      } while (r != null);
      Assert.assertEquals(rowNum * SPLIT_KEYS.length * FAMILIES.length, count);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testBulkLoadThinsRows() throws Exception {
    String tableNameAsString = "testBulkLoadThinRows";
    TableName tn = TableName.valueOf(tableNameAsString);
    createTableWithMultipleFamilies(tn);
    int rowNum = 20;
    List<String> cellStrings = createCellStringsWithFamiliesAndQualifiers(rowNum);
    PCollection<String> inputs = pipeline.apply("createDataset" + UUID.randomUUID().toString(),
      Create.of(cellStrings).withCoder(StringUtf8Coder.of()));
    Path stagingPath =
        new Path(TEST_UTIL.getDataTestDir(), UUID.randomUUID().toString().replaceAll("-", ""));
    Path tmpPath =
        new Path(TEST_UTIL.getDataTestDir(), UUID.randomUUID().toString().replaceAll("-", ""));
    HBasePipelineFunctions.bulkLoadThinRows(conf, tableNameAsString, inputs, stagingPath.toUri(),
      tmpPath.toUri(), new MakeFunction<String, byte[]>() {
        @Override
        public byte[] make(String input) {
          String[] strs = input.split(" ");
          if (strs.length > 0) {
            return Bytes.toBytes(strs[0]);
          }
          return null;
        }
      }, new MakeFunction<String, Iterable<Cell>>() {
        @Override
        public Iterable<Cell> make(String input) {
          String[] strs = input.split(" ");
          if (strs.length == 2 + QUALIFIERS.length * 2) {
            List<Cell> cells = new ArrayList<>();
            int index = 2;
            while (index < strs.length) {
              cells.add(new KeyValue(Bytes.toBytes(strs[0]), Bytes.toBytes(strs[1]),
                  Bytes.toBytes(strs[index++]), EnvironmentEdgeManager.currentTime(),
                  Bytes.toBytes(strs[index++])));
            }
            return cells;
          }
          return null;
        }
      }, StringUtf8Coder.of());
    Table table = null;
    try {
      table = TEST_UTIL.getConnection().getTable(tn);
      ResultScanner scanner = table.getScanner(new Scan());
      Result r = null;
      int count = 0;
      do {
        r = scanner.next();
        if (r != null) {
          for (byte[] family : FAMILIES) {
            for (byte[] qualifier : QUALIFIERS) {
              if (r.getValue(family, qualifier) != null) {
                count++;
              }
            }
          }
        }
      } while (r != null);
      Assert.assertEquals(rowNum * SPLIT_KEYS.length * FAMILIES.length * QUALIFIERS.length, count);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testForeachPartition() throws IOException {
    String tableNameAsString = "testForeachPartition";
    TableName tn = TableName.valueOf(tableNameAsString);
    createTable(tn);
    int rowNum = 50;
    List<String> cellStrings = createKeyAndValueStrings(rowNum);
    PCollection<String> inputs =
        pipeline.apply("createDataset" + UUID.randomUUID().toString(), Create.of(cellStrings));
    Configuration tempConf = new Configuration(conf);
    tempConf.set("beam.test.tablename", tableNameAsString);
    HBasePipelineFunctions.foreachPartition(tempConf, inputs, null,
      new ForeachPartitionFunc<String>() {

        @Override
      public void execute(Configuration conf, Connection conn, Iterable<String> partition)
          throws IOException {
        String tableName = conf.get("beam.test.tablename");
        BufferedMutator mutator = conn.getBufferedMutator(TableName.valueOf(tableName));
        try {
          for (String element : partition) {
            String[] strs = element.split(" ");
            if (strs.length == 2) {
              Put put = new Put(Bytes.toBytes(strs[0]));
              put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(strs[1]));
              mutator.mutate(put);
            }
          }
          mutator.flush();
        } finally {
          mutator.close();
        }
      }
    });
    pipeline.run().waitUntilFinish();
    Table table = null;
    try {
      table = TEST_UTIL.getConnection().getTable(tn);
      ResultScanner scanner = table.getScanner(new Scan());
      Result r = null;
      int count = 0;
      do {
        r = scanner.next();
        if (r != null) {
          count++;
        }
      } while (r != null);
      Assert.assertEquals(rowNum * SPLIT_KEYS.length, count);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testMapPartitions() throws IOException {
    String tableNameAsString = "testMapPartitions";
    TableName tn = TableName.valueOf(tableNameAsString);
    createTable(tn);
    int rowNum = 50;
    List<String> cellStrings = createKeyAndValueStrings(rowNum);
    PCollection<String> inputs =
        pipeline.apply("createDataset" + UUID.randomUUID().toString(), Create.of(cellStrings));
    Configuration tempConf = new Configuration(conf);
    tempConf.set("beam.test.tablename", tableNameAsString);
    HBasePipelineFunctions.mapPartitions(tempConf, inputs, null,
      new MapPartitionsFunc<String, Result>() {

        @Override
        public Iterable<Result> execute(Configuration conf, Connection conn,
            Iterable<String> partition) throws IOException {
          String tableName = conf.get("beam.test.tablename");
          BufferedMutator mutator = conn.getBufferedMutator(TableName.valueOf(tableName));
          List<Get> gets = new ArrayList<>();
          try {
            for (String element : partition) {
              String[] strs = element.split(" ");
              if (strs.length == 2) {
                Put put = new Put(Bytes.toBytes(strs[0]));
                gets.add(new Get(Bytes.toBytes(strs[0])));
                put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(strs[1]));
                mutator.mutate(put);
              }
            }
            mutator.flush();
          } finally {
            mutator.close();
          }
          return gets.isEmpty() ? Collections.emptyList()
              : Arrays.asList(conn.getTable(TableName.valueOf(tableName)).get(gets));
        }
      }, HBaseResultCoder.of());
    pipeline.run().waitUntilFinish();
    Table table = null;
    try {
      table = TEST_UTIL.getConnection().getTable(tn);
      ResultScanner scanner = table.getScanner(new Scan());
      Result r = null;
      int count = 0;
      do {
        r = scanner.next();
        if (r != null) {
          count++;
        }
      } while (r != null);
      Assert.assertEquals(rowNum * SPLIT_KEYS.length, count);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  private void createTable(TableName tn) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tn);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    htd.addFamily(hcd);
    admin.createTable(htd, SPLIT_KEYS);
  }
  
  private void createTableWithMultipleFamilies(TableName tn) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tn);
    for (byte[] family : FAMILIES) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);
    }
    admin.createTable(htd, SPLIT_KEYS);
  }

  private void writeData(TableName tn, int rowNum) throws IOException {
    BufferedMutator mutator = TEST_UTIL.getConnection().getBufferedMutator(tn);
    for (int i = 0; i < rowNum; i++) {
      for(byte[] splitKey : SPLIT_KEYS) {
        byte[] row = Bytes.add(splitKey, Bytes.toBytes(Integer.toString(i)));
        Put put = new Put(row);
        put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(Integer.toString(i)));
        mutator.mutate(put);
      }
    }
    mutator.flush();
    mutator.close();
  }

  private List<byte[]> createRows(int rowNum) {
    List<byte[]> rows = new ArrayList<>();
    for (int i = 0; i < rowNum; i++) {
      for(byte[] splitKey : SPLIT_KEYS) {
        byte[] row = Bytes.add(splitKey, Bytes.toBytes(Integer.toString(i)));
        rows.add(row);
      }
    }
    return rows;
  }

  private List<String> createKeyAndValueStrings(int rowNum) {
    List<String> cells = new ArrayList<>();
    for (int i = 0; i < rowNum; i++) {
      for (byte[] splitKey : SPLIT_KEYS) {
        cells.add(Bytes.toString(splitKey) + i + " " + "v[" + i + "]");
      }
    }
    return cells;
  }

  private List<String> createCellStringsWithFamilies(int rowNum) {
    List<String> cells = new ArrayList<>();
    for (int i = 0; i < rowNum; i++) {
      for (byte[] splitKey : SPLIT_KEYS) {
        for (byte[] family : FAMILIES) {
          cells.add(Bytes.toString(splitKey) + i + " " + Bytes.toString(family) + " "
              + Bytes.toString(QUALIFIER) + " " + "v[" + i + "]");
        }
      }
    }
    return cells;
  }

  private List<String> createCellStringsWithFamiliesAndQualifiers(int rowNum) {
    List<String> cells = new ArrayList<>();
    for (int i = 0; i < rowNum; i++) {
      for (byte[] splitKey : SPLIT_KEYS) {
        for (byte[] family : FAMILIES) {
          StringBuilder sr =
              new StringBuilder(Bytes.toString(splitKey) + i + " " + Bytes.toString(family) + " ");
          int index = 0;
          for (byte[] qualifier : QUALIFIERS) {
            sr.append(Bytes.toString(qualifier) + " " + "v" + index + "[" + i + "]" + " ");
            index++;
          }
          cells.add(sr.toString().trim());
        }
      }
    }
    return cells;
  }
}
