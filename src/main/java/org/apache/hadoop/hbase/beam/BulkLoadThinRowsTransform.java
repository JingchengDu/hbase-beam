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
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.beam.io.SerializableBytesArray;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.KeyValueHeap;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class BulkLoadThinRowsTransform<I> extends PTransform<PCollection<I>, PDone> {

  private SerializableConfiguration conf;
  private String tableName;
  private SerializableBytesArray startKeys;
  private MakeFunction<I, byte[]> rowKeyFunc;
  private MakeFunction<I, Iterable<Cell>> cellsFunc;
  private Coder<I> inputCoder;
  private long sizelimit;
  private URI stagingPathUri;
  private URI tmpPathUri;
  private boolean minorCompactionExclude;
  private int maxFilesPerRegionPerFamily;

  public BulkLoadThinRowsTransform(SerializableConfiguration conf, String tableName,
      URI stagingPathUri, URI tmpPathUri, SerializableBytesArray startKeys, boolean sorted,
      MakeFunction<I, byte[]> rowKeyFunc, MakeFunction<I, Iterable<Cell>> cellsFunc,
      Coder<I> inputCoder) {
    this(conf, tableName, stagingPathUri, tmpPathUri, startKeys, sorted, rowKeyFunc, cellsFunc,
        inputCoder, false);
  }

  public BulkLoadThinRowsTransform(SerializableConfiguration conf, String tableName,
      URI stagingPathUri, URI tmpPathUri, SerializableBytesArray startKeys, boolean sorted,
      MakeFunction<I, byte[]> rowKeyFunc, MakeFunction<I, Iterable<Cell>> cellsFunc,
      Coder<I> inputCoder, boolean minorCompactionExclude) {
    this.conf = conf;
    this.sizelimit = conf.get().getLong(HBaseBeamConstants.HBASE_BEAM_BULKLOAD_SIZELIMIT_KEY,
      HBaseBeamConstants.DEFAULT_HBASE_BEAM_BULKLOAD_SIZELIMIT);
    maxFilesPerRegionPerFamily =
        conf.get().getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32);
    this.tableName = tableName;
    this.stagingPathUri = stagingPathUri;
    this.tmpPathUri = tmpPathUri;
    this.startKeys = startKeys;
    this.cellsFunc = cellsFunc;
    this.rowKeyFunc = rowKeyFunc;
    this.inputCoder = inputCoder;
    this.minorCompactionExclude = minorCompactionExclude;
    if (!sorted) {
      Arrays.sort(startKeys.get(), Bytes.BYTES_RAWCOMPARATOR);
    }
  }

  @Override
  public PDone expand(PCollection<I> inputs) {
    // 1. Prepare the inputs with <key, value> style
    PCollection<KV<byte[], I>> elements = map(inputs);
    // 2. Run GroupByKey
    PCollection<KV<byte[], Iterable<I>>> cellsPartitions = group(elements);
    // 3. Sort and output in each group
    foreachGroup(cellsPartitions);
    return PDone.in(inputs.getPipeline());
  }

  private PCollection<KV<byte[], Iterable<I>>> group(PCollection<KV<byte[], I>> cells) {
    return cells.apply("groupByKey" + UUID.randomUUID().toString(), GroupByKey.<byte[], I> create())
        .setCoder(KvCoder.of(ByteArrayCoder.of(), IterableCoder.of(inputCoder)));
  }

  private void foreachGroup(PCollection<KV<byte[], Iterable<I>>> cellsPartitions) { 
    cellsPartitions.apply(ParDo.of(new DoFn<KV<byte[], Iterable<I>>, Void>() {

      private transient Connection conn;

      @Setup
      public void setup() throws Exception {
        conn = ConnectionFactory.createConnection(conf.get());
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        HTableDescriptor htd = conn.getAdmin().getTableDescriptor(TableName.valueOf(tableName));
        ConcurrentSkipListMap<Cell, Cell> cellMap =
            new ConcurrentSkipListMap<Cell, Cell>(KeyValue.COMPARATOR);
        long maxFileSize =
            conf.get().getLong(HConstants.HREGION_MAX_FILESIZE, HConstants.DEFAULT_MAX_FILE_SIZE);
        Map<String, Writer> familyWriters = new HashMap<>();
        Map<String, List<Path>> writerPaths = new HashMap<>();
        Path stagingPath = new Path(stagingPathUri);
        FileSystem fs = stagingPath.getFileSystem(conf.get());
        Path tmpPath = new Path(tmpPathUri);
        Path workingPath = new Path(tmpPath, UUID.randomUUID().toString().replaceAll("-", ""));
        long memCellsSize = 0;
        // find the region location.
        byte[] startKey = c.element().getKey();
        HRegionLocation location =
            conn.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(startKey);
        String host = location.getHostname();
        int port = location.getPort();
        InetSocketAddress isa = new InetSocketAddress(host, port);
        InetSocketAddress[] favoredNodes = new InetSocketAddress[] { isa };
        Iterable<I> elements = c.element().getValue();
        if (elements != null) {
          Cell lastCell = null;
          for (I element : elements) {
            Iterable<Cell> cells = cellsFunc.make(element);
            if (cells != null) {
              for (Cell cell : cells) {
                if (cell != null) {
                  long cellSize = CellUtil.estimatedHeapSizeOf(cell);
                  memCellsSize += cellSize;
                  cellMap.put(cell, cell);
                  boolean couldFlush =
                      lastCell == null || Bytes.BYTES_RAWCOMPARATOR.compare(cell.getRowArray(),
                        cell.getRowOffset(), cell.getRowLength(), lastCell.getRowArray(),
                        lastCell.getRowOffset(), lastCell.getRowLength()) != 0;
                  if (couldFlush && (memCellsSize >= sizelimit || (memCellsSize >= maxFileSize))) {
                    // flush the memory to a hfile
                    flush(htd, fs, workingPath, startKey, favoredNodes, cellMap, familyWriters,
                      writerPaths);
                    cellMap.clear();
                    memCellsSize = 0;
                  }
                  lastCell = cell;
                }
              }
            }
          }
          if (!cellMap.isEmpty()) {
            flush(htd, fs, workingPath, startKey, favoredNodes, cellMap, familyWriters,
              writerPaths);
            cellMap.clear();
            memCellsSize = 0;
          }
          for (Entry<String, List<Path>> pathEntry : writerPaths.entrySet()) {
            String familyName = pathEntry.getKey();
            List<Path> paths = pathEntry.getValue();
            if (paths != null && paths.size() > maxFilesPerRegionPerFamily) {
              HColumnDescriptor family = htd.getFamily(Bytes.toBytes(familyName));
              compactHFiles(fs, conf.get(), family, paths, startKey, favoredNodes);
            }
          }
          if (!writerPaths.isEmpty()) {
            // commit the files
            commitHFiles(fs, workingPath, stagingPath);
          }
        }
      }

      @Teardown
      public void tearDown() throws Exception {
        if (conn != null) {
          try {
            conn.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }));
  }

  private PCollection<KV<byte[], I>> map(PCollection<I> inputs) {
    return inputs
        .apply("prepareKV" + UUID.randomUUID().toString(), ParDo.of(new DoFn<I, KV<byte[], I>>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            I e = c.element();
            byte[] rowKey = rowKeyFunc.make(e);
            if (rowKey != null) {
              c.outputWithTimestamp(KV.of(rowKey, e), c.timestamp());
            }
          }
        })).setCoder(KvCoder.of(ByteArrayCoder.of(), inputCoder));
  }

  protected int partitionFor(byte[] startKey) {
    int index = Bytes.binarySearch(startKeys.get(), startKey, 0, startKey.length,
      Bytes.BYTES_RAWCOMPARATOR);
    if (index >= 0) {
      return index;
    } else {
      return -1 * index - 2;
    }
  }

  private void flush(HTableDescriptor htd, FileSystem fs, Path workingPath, byte[] startKey,
      InetSocketAddress[] favoredNodes, ConcurrentSkipListMap<Cell, Cell> cellMap,
      Map<String, Writer> familyWriters, Map<String, List<Path>> writerPaths) throws IOException {
    for (Cell cell : cellMap.values()) {
      String familyName =
          Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
      Writer writer = familyWriters.get(familyName);
      if (writer == null) {
        Path familyPath = new Path(workingPath, familyName);
        if (!fs.mkdirs(familyPath)) {
          if (!fs.exists(familyPath)) {
            throw new IOException("The directory " + familyPath + " cannot be created");
          }
        }
        // create writer
        HColumnDescriptor hcd = htd.getFamily(Bytes.toBytes(familyName));
        writer = HBaseBeamUtils.createWriter(fs, conf.get(), hcd, familyPath, favoredNodes);
        familyWriters.put(familyName, writer);
      }
      writer.append(cell);
    }
    // append the metadata and close writers
    for (Entry<String, Writer> writerEntry : familyWriters.entrySet()) {
      String familyName = writerEntry.getKey();
      Writer writer = writerEntry.getValue();
      if (writer != null) {
        List<Path> paths = writerPaths.get(familyName);
        if (paths == null) {
          paths = new ArrayList<>();
          writerPaths.put(familyName, paths);
        }
        paths.add(writer.getPath());
        writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
          Bytes.toBytes(EnvironmentEdgeManager.currentTime()));
        writer.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY, Bytes.toBytes(partitionFor(startKey)));
        writer.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
          Bytes.toBytes(minorCompactionExclude));
        writer.appendTrackedTimestampsToMetadata();
        try {
          writer.close();
        } catch (IOException e) {
        }
      }
    }
    // clear the writers map
    familyWriters.clear();
  }

  private void compactHFiles(FileSystem fs, Configuration conf, HColumnDescriptor family,
      List<Path> compactedFiles, byte[] startKey, InetSocketAddress[] favoredNodes)
      throws IOException {
    // disable block cache in writing
    Configuration tmpConf = new Configuration(conf);
    tmpConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    CacheConfig cacheConf = new CacheConfig(tmpConf);
    // compact the files
    Path familyPath = compactedFiles.get(0).getParent();
    List<StoreFile> storeFiles = new ArrayList<>();
    for (Path path : compactedFiles) {
      storeFiles.add(new StoreFile(fs, path, conf, cacheConf, BloomType.NONE));
    }
    List<StoreFileScanner> storeFileScanners =
        StoreFileScanner.getScannersForStoreFiles(storeFiles, false, false, Long.MAX_VALUE);
    // seek the dummy in the scanner, for now we do it in sequence.
    KeyValue seekedKey = KeyValueUtil.createFirstDeleteFamilyOnRow(startKey, family.getName());
    for (StoreFileScanner storeFileScanner : storeFileScanners) {
      storeFileScanner.seek(seekedKey);
    }
    KeyValueHeap heap = new KeyValueHeap(storeFileScanners, KeyValue.COMPARATOR);
    Writer writer = null;
    try {
      writer = HBaseBeamUtils.createWriter(fs, conf, family, familyPath, favoredNodes);
      Cell next = heap.next();
      while (next != null) {
        writer.append(next);
        next = heap.next();
      }
    } finally {
      heap.close();
    }
    if (writer != null) {
      writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
        Bytes.toBytes(EnvironmentEdgeManager.currentTime()));
      writer.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY, Bytes.toBytes(partitionFor(startKey)));
      writer.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, Bytes.toBytes(true));
      writer.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
        Bytes.toBytes(minorCompactionExclude));
      writer.appendTrackedTimestampsToMetadata();
      try {
        writer.close();
      } catch (IOException e) {
      }
      // remove old hfiles.
      for (Path path : compactedFiles) {
        fs.delete(path, true);
      }
    }
  }

  private void commitHFiles(FileSystem fs, Path workingPath, Path stagingPath) throws IOException {
    FileStatus[] familyDirs = fs.listStatus(workingPath);
    for (FileStatus familyDir : familyDirs) {
      FileStatus[] hfiles = fs.listStatus(familyDir.getPath());
      Path targetDir = new Path(stagingPath, familyDir.getPath().getName());
      if (!fs.mkdirs(targetDir)) {
        if (!fs.exists(targetDir)) {
          throw new IOException("The directory " + targetDir + " cannot be created");
        }
      }
      for (FileStatus hfile : hfiles) {
        Path targetPath = new Path(targetDir, hfile.getPath().getName());
        if (!fs.rename(hfile.getPath(), targetPath)) {
          throw new IOException(
              "The file " + hfile.getPath() + " cannot be renamed to " + targetPath);
        }
      }
    }
  }
}
