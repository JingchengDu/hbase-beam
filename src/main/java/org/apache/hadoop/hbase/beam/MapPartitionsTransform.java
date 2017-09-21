package org.apache.hadoop.hbase.beam;

import java.io.IOException;
import java.util.UUID;

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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class MapPartitionsTransform<I, O>
    extends PTransform<PCollection<I>, PCollection<KV<byte[], Iterable<O>>>> {

  private SerializableConfiguration conf;
  private PartitionKeyFunc<I> partitionKeyFunc;
  private MapPartitionsFunc<I, O> mapPartitionsFunc;
  private Coder<O> outputCoder;

  public MapPartitionsTransform(SerializableConfiguration conf,
      PartitionKeyFunc<I> partitionKeyFunc, MapPartitionsFunc<I, O> mapPartitionsFunc,
      Coder<O> outputCoder) {
    this.conf = conf;
    this.partitionKeyFunc = partitionKeyFunc;
    this.mapPartitionsFunc = mapPartitionsFunc;
    this.outputCoder = outputCoder;
  }

  @Override
  public PCollection<KV<byte[], Iterable<O>>> expand(PCollection<I> inputs) {
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

    return inputsPartitions
        .apply(ParDo.of(new DoFn<KV<byte[], Iterable<I>>, KV<byte[], Iterable<O>>>() {
          private transient Connection conn;

          @Setup
          public void setup() throws Exception {
            conn = ConnectionFactory.createConnection(conf.get());
          }

          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            Iterable<O> results =
                mapPartitionsFunc.execute(conf.get(), conn, c.element().getValue());
            if (results != null) {
              c.outputWithTimestamp(KV.of(c.element().getKey(), results), c.timestamp());
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
        })).setCoder(KvCoder.of(ByteArrayCoder.of(), IterableCoder.of(outputCoder)));
  }
}
