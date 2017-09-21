package org.apache.hadoop.hbase.beam.io;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

public class SerializableGet implements Serializable {

  private transient Get get;

  public SerializableGet() {
  }

  public SerializableGet(Get get) {
    if (get == null) {
      throw new NullPointerException("Get must not be null.");
    }
    this.get = get;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    ProtobufUtil.toGet(get).writeDelimitedTo(out);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    get = ProtobufUtil.toGet(ClientProtos.Get.parseDelimitedFrom(in));
  }

  public Get get() {
    return get;
  }
}
