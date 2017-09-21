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
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.joda.time.Instant;

public class BulkGetTransform<I, O> extends PTransform<PCollection<I>, PCollection<O>> {

  private SerializableConfiguration conf;
  private String tableName;
  private int batchSize;
  private MakeFunction<I, Get> inputFunc;
  private MakeFunction<I, Instant> instantFunc;
  private MakeFunction<Result, O> outputFunc;
  private Coder<O> outputCoder;

  public BulkGetTransform(SerializableConfiguration conf, String tableName, int batchSize,
      MakeFunction<I, Get> inputFunc, MakeFunction<I, Instant> instantFunc,
      MakeFunction<Result, O> outputFunc, Coder<O> outputCoder) {
    this.conf = conf;
    this.tableName = tableName;
    this.batchSize = batchSize;
    this.inputFunc = inputFunc;
    this.instantFunc = instantFunc;
    this.outputFunc = outputFunc;
    this.outputCoder = outputCoder;
  }

  @Override
  public PCollection<O> expand(PCollection<I> inputs) {
    return inputs.apply(ParDo.of(new DoFn<I, O>() {

      private transient Connection conn;
      private transient Table table;
      private transient List<Get> gets;
      private transient List<Instant> instants;
      private transient BoundedWindow window;

      @Setup
      public void setup() throws Exception {
        conn = ConnectionFactory.createConnection(conf.get());
        table = conn.getTable(TableName.valueOf(tableName));
        gets = new ArrayList<>();
        if (instantFunc != null) {
          instants = new ArrayList<>();
        }
      }

      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
        I input = c.element();
        Get get = inputFunc.make(input);
        if (get != null) {
          gets.add(get);
          if (instantFunc != null) {
            instants.add(instantFunc.make(input));
          }
        }
        if (gets.size() == batchSize) {
          Result[] results = table.get(gets);
          int index = 0;
          for (Result result : results) {
            O o = outputFunc.make(result);
            if (o != null) {
              Instant instant = window.maxTimestamp();
              if (instantFunc != null) {
                instant = instants.get(index);
              }
              c.outputWithTimestamp(o, instant);
            }
            index++;
          }
          gets.clear();
          if (instants != null) {
            instants.clear();
          }
        }
        this.window = window;
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext c) throws Exception {
        if (this.window != null) {
          if (gets.size() > 0) {
            Result[] results = table.get(gets);
            int index = 0;
            for (Result result : results) {
              O o = outputFunc.make(result);
              if (o != null) {
                Instant instant = window.maxTimestamp();
                if (instantFunc != null) {
                  instant = instants.get(index);
                }
                c.output(o, instant, window);
              }
              index++;
            }
            gets.clear();
            if (instants != null) {
              instants.clear();
            }
          }
          this.window = null;
        }
      }

      @Teardown
      public void tearDown() throws Exception {
        if (table != null) {
          try {
            table.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
        if (conn != null) {
          try {
            conn.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    })).setCoder(outputCoder);
  }

}
