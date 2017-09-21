package org.apache.hadoop.hbase.beam.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

public class HBaseGetCoder extends CustomCoder<Get> implements Serializable {

  private static final HBaseGetCoder INSTANCE = new HBaseGetCoder();

  private HBaseGetCoder() {
  }

  public static HBaseGetCoder of() {
    return INSTANCE;
  }

  @Override
  public Get decode(InputStream inputStream) throws CoderException, IOException {
    return ProtobufUtil.toGet(ClientProtos.Get.parseDelimitedFrom(inputStream));
  }

  @Override
  public void encode(Get get, OutputStream outputStream) throws CoderException, IOException {
    ProtobufUtil.toGet(get).writeDelimitedTo(outputStream);
  }

}
