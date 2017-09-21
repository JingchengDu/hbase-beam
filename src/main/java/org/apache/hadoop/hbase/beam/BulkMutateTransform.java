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

import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;

public class BulkMutateTransform<I> extends PTransform<PCollection<I>, PDone> {

  private SerializableConfiguration conf;
  private String tableName;
  private MakeFunction<I, Mutation> inputFunc;

  public BulkMutateTransform(SerializableConfiguration conf, String tableName,
      MakeFunction<I, Mutation> inputFunc) {
    this.conf = conf;
    this.tableName = tableName;
    this.inputFunc = inputFunc;
  }

  @Override
  public PDone expand(PCollection<I> inputs) {
    inputs.apply(ParDo.of(new DoFn<I, Void>() {

      private transient Connection conn;
      private transient BufferedMutator mutator;

      @Setup
      public void setup() throws Exception {
        conn = ConnectionFactory.createConnection(conf.get());
        mutator = conn.getBufferedMutator(TableName.valueOf(tableName));
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        I input = c.element();
        Mutation mutation = inputFunc.make(input);
        if (mutation != null) {
          mutator.mutate(mutation);
        }
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        mutator.flush();
      }

      @Teardown
      public void tearDown() throws Exception {
        if (mutator != null) {
          try {
            mutator.close();
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
    }));
    return PDone.in(inputs.getPipeline());
  }
}
