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
package org.apache.hadoop.hbase.beam.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.beam.HBasePipelineFunctions;
import org.apache.hadoop.hbase.beam.MakeFunction;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class HBaseBulkLoad {

  private static final byte[][] FAMILIES =
      new byte[][] { Bytes.toBytes("fam0"), Bytes.toBytes("fam1"), Bytes.toBytes("fam2") };
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[][] SPLIT_KEYS = { Bytes.toBytes("aaa"), Bytes.toBytes("bbb") };

  public interface BulkLoadOptions extends PipelineOptions {

    @Description("The default FS")
    String getDefaultFS();

    void setDefaultFS(String defaultFS);

    @Description("The split keys that are used to create tables, the keys are separated by comma")
    String getSplitKeys();

    void setSplitKeys(String splitKeys);

    @Description("The generated cell number per region, works only when inputFile is not used")
    Integer getGeneratedCellNumberPerRegion();

    void setGeneratedCellNumberPerRegion(Integer number);

    @Description("The Zookeeper Quorum")
    String getClusterAddress();

    void setClusterAddress(String address);

    @Description("The table name")
    String getTableName();

    void setTableName(String tableName);

    @Description("Path of the file to read from")
    String getInputFile();

    void setInputFile(String path);

    @Description("The staging directory")
    String getStagingDir();

    void setStagingDir(String path);

    @Description("The temp directory")
    String getTempDir();

    void setTempDir(String path);

    @Description("The cell function name")
    String getCellFuncClassName();

    void setCellFuncClassName(String className);

    @Description("Batch interval for Spark streaming in milliseconds.")
    @Default.Boolean(false)
    Boolean getMinorCompactionExclude();

    void setMinorCompactionExclude(Boolean minorCompactionExclude);
  }

  private static void createTable(Configuration conf, TableName tn, byte[][] splitKeys)
      throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      if (!admin.isTableAvailable(tn)) {
        HTableDescriptor htd = new HTableDescriptor(tn);
        for (byte[] family : FAMILIES) {
          HColumnDescriptor hcd = new HColumnDescriptor(family);
          htd.addFamily(hcd);
        }
        admin.createTable(htd, splitKeys);
      } else if (!admin.isTableEnabled(tn)) {
        admin.enableTable(tn);
      }
    } finally {
      admin.close();
    }
  }

  private static List<String> createCellStrings(int rowNum, byte[][] splitKeys) {
    List<String> cells = new ArrayList<>();
    for (int i = 0; i < rowNum; i++) {
      for (byte[] splitKey : splitKeys) {
        for (byte[] family : FAMILIES) {
          cells.add(Bytes.toString(splitKey) + i + " " + Bytes.toString(family) + " "
              + Bytes.toString(QUALIFIER) + " " + "v[" + i + "]");
        }
      }
    }
    return cells;
  }

  private static byte[][] parseSplitKeys(String keyString) {
    byte[][] splitKeys = null;
    if (keyString != null) {
      String[] keys = keyString.split(",");
      splitKeys = new byte[keys.length][];
      for (int i = 0; i < keys.length; i++) {
        splitKeys[i] = Bytes.toBytes(keys[i]);
      }
    } else {
      splitKeys = SPLIT_KEYS;
    }
    return splitKeys;
  }

  public static class BulkLoadCellFunc implements MakeFunction<String, Cell> {
    @Override
    public Cell make(String input) {
      String[] strs = input.split(" ");
      if (strs.length == 4) {
        return new KeyValue(Bytes.toBytes(strs[0]), Bytes.toBytes(strs[1]), Bytes.toBytes(strs[2]),
            EnvironmentEdgeManager.currentTime(), Bytes.toBytes(strs[3]));
      }
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    BulkLoadOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BulkLoadOptions.class);

    // validate the options
    if (options.getDefaultFS() == null) {
      throw new IllegalArgumentException("The argument defaultFS cannot be empty!");
    }
    if (options.getClusterAddress() == null) {
      throw new IllegalArgumentException("The argument clusterAddress cannot be empty!");
    }
    if (options.getTableName() == null) {
      throw new IllegalArgumentException("The argument tableName cannot be empty!");
    }
    if (options.getStagingDir() == null) {
      throw new IllegalArgumentException("The argument stagingDir cannot be empty!");
    }
    if (options.getTempDir() == null) {
      throw new IllegalArgumentException("The argument tempDir cannot be empty!");
    }

    Path stagingPath = new Path(options.getStagingDir());
    Path tempPath = new Path(options.getTempDir());
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", options.getClusterAddress());
    conf.set("fs.defaultFS", options.getDefaultFS());
    TableName tableName = TableName.valueOf(options.getTableName());
    byte[][] splitKeys = parseSplitKeys(options.getSplitKeys());
    createTable(conf, tableName, splitKeys);
    Pipeline p = Pipeline.create(options);
    PCollection<String> inputs = null;
    String inputFile = options.getInputFile();
    if (inputFile != null) {
      inputs = p.apply("ReadLines" + UUID.randomUUID().toString(), TextIO.read().from(inputFile));
    } else {
      int number = options.getGeneratedCellNumberPerRegion() != null
          ? options.getGeneratedCellNumberPerRegion().intValue() : 5;
      List<String> cellStrings = createCellStrings(number, splitKeys);
      inputs = p.apply("createDataset" + UUID.randomUUID().toString(),
        Create.of(cellStrings).withCoder(StringUtf8Coder.of()));
    }
    String className = options.getCellFuncClassName() == null ? BulkLoadCellFunc.class.getName()
        : options.getCellFuncClassName();
    MakeFunction<String, Cell> cellFunc = null;
    try {
      cellFunc = (MakeFunction<String, Cell>) Class.forName(className).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Failed to instantiate the cell function class " + className, e);
    }

    HBasePipelineFunctions.bulkLoad(conf, options.getTableName(), inputs, stagingPath.toUri(),
      tempPath.toUri(), cellFunc, options.getMinorCompactionExclude());
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(stagingPath)) {
      try {
        fs.delete(stagingPath, true);
      } catch (IOException e) {
      }
    }
    if (fs.exists(tempPath)) {
      try {
        fs.delete(tempPath, true);
      } catch (IOException e) {
      }
    }
  }
}
