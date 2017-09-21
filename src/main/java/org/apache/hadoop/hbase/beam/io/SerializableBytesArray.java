package org.apache.hadoop.hbase.beam.io;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.hbase.HConstants;

public class SerializableBytesArray implements Serializable {

  private transient byte[][] bytesArray;

  public SerializableBytesArray(byte[][] bytesArray) {
    if (bytesArray == null) {
      throw new NullPointerException("bytes array must not be null.");
    }
    this.bytesArray = bytesArray;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    int length = bytesArray.length;
    out.writeInt(length);
    for (byte[] bytes : bytesArray) {
      if (bytes == null) {
        out.writeInt(0);
      } else {
        out.writeInt(bytes.length);
        out.write(bytes);
      }
    }
  }

  private void readObject(ObjectInputStream in) throws IOException {
    int length = in.readInt();
    if (length == 0) {
      bytesArray = new byte[][] {};
    } else {
      bytesArray = new byte[length][];
      for (int i = 0; i < length; i++) {
        int bytesLength = in.readInt();
        if (bytesLength == 0) {
          bytesArray[i] = HConstants.EMPTY_BYTE_ARRAY;
        } else {
          bytesArray[i] = new byte[bytesLength];
          in.readFully(bytesArray[i], 0, bytesLength);
        }
      }
    }
  }

  public byte[][] get() {
    return bytesArray;
  }
}
