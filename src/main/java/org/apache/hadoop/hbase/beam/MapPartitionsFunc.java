package org.apache.hadoop.hbase.beam;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public interface MapPartitionsFunc<I, O> extends Serializable {
  Iterable<O> execute(Configuration conf, Connection conn, Iterable<I> partition)
      throws IOException;
}
