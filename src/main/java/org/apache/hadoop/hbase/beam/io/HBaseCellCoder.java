package org.apache.hadoop.hbase.beam.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;

public class HBaseCellCoder extends CustomCoder<Cell> implements Serializable {

  private static final HBaseCellCoder INSTANCE = new HBaseCellCoder();

  private HBaseCellCoder() {
  }

  public static HBaseCellCoder of() {
    return INSTANCE;
  }

  @Override
  public Cell decode(InputStream inputStream) throws CoderException, IOException {
    return ProtobufUtil.toCell(CellProtos.Cell.parseDelimitedFrom(inputStream));
  }

  @Override
  public void encode(Cell cell, OutputStream outputStream) throws CoderException, IOException {
    ProtobufUtil.toCell(cell).writeDelimitedTo(outputStream);
  }
}
