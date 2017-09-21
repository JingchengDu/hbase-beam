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
import java.util.UUID;

import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class ForeachPartitionTransform<I> extends PTransform<PCollection<I>, PDone> {

  private SerializableConfiguration conf;
  private PartitionKeyFunc<I> partitionKeyFunc;
  private ForeachPartitionFunc<I> foreachPartitionFunc;

  public ForeachPartitionTransform(SerializableConfiguration conf,
      PartitionKeyFunc<I> partitionKeyFunc, ForeachPartitionFunc<I> foreachPartitionFunc) {
    this.conf = conf;
    this.partitionKeyFunc = partitionKeyFunc;
    this.foreachPartitionFunc = foreachPartitionFunc;
  }

  @Override
  public PDone expand(PCollection<I> inputs) {
    PCollection<KV<byte[], Iterable<I>>> inputsPartitions = inputs
        .apply("prepareKV" + UUID.randomUUID().toString(), ParDo.of(new DoFn<I, KV<byte[], I>>() {
          private byte[] keyPerBundle;

          @StartBundle
          public void startBundle() {
            if (partitionKeyFunc == null) {
              // if partitionKeyFunc is null, use bundle as partition
              keyPerBundle = Bytes.toBytes(UUID.randomUUID().toString());
            }
          }

          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            I input = c.element();
            byte[] key = keyPerBundle;
            if (partitionKeyFunc != null) {
              key = partitionKeyFunc.getPartitionKey(input);
            }
            if (key != null) {
              c.outputWithTimestamp(KV.of(key, input), c.timestamp());
            }
          }
        })).apply("groupByKey" + UUID.randomUUID().toString(), GroupByKey.<byte[], I> create());

    inputsPartitions.apply(ParDo.of(new DoFn<KV<byte[], Iterable<I>>, Void>() {
      private transient Connection conn;

      @Setup
      public void setup() throws Exception {
        conn = ConnectionFactory.createConnection(conf.get());
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        foreachPartitionFunc.execute(conf.get(), conn, c.element().getValue());
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
    return PDone.in(inputs.getPipeline());
  }
}
