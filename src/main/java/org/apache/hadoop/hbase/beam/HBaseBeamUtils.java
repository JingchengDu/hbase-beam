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
import java.security.Key;
import java.security.KeyException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFile.WriterBuilder;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class HBaseBeamUtils {

  public static Encryption.Context createEncryptionContext(Configuration conf,
      HColumnDescriptor family) throws IOException {
    // Copy the code directly from HBase since this is not a static method yet in branch-1.
    // Crypto context for new store files
    String cipherName = family.getEncryptionType();
    if (cipherName != null) {
      Cipher cipher;
      Key key;
      byte[] keyBytes = family.getEncryptionKey();
      if (keyBytes != null) {
        // Family provides specific key material
        String masterKeyName =
            conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName());
        try {
          // First try the master key
          key = EncryptionUtil.unwrapKey(conf, masterKeyName, keyBytes);
        } catch (KeyException e) {
          String alternateKeyName = conf.get(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY);
          if (alternateKeyName != null) {
            try {
              key = EncryptionUtil.unwrapKey(conf, alternateKeyName, keyBytes);
            } catch (KeyException ex) {
              throw new IOException(ex);
            }
          } else {
            throw new IOException(e);
          }
        }
        // Use the algorithm the key wants
        cipher = Encryption.getCipher(conf, key.getAlgorithm());
        if (cipher == null) {
          throw new RuntimeException("Cipher '" + key.getAlgorithm() + "' is not available");
        }
        // Fail if misconfigured
        // We use the encryption type specified in the column schema as a sanity check on
        // what the wrapped key is telling us
        if (!cipher.getName().equalsIgnoreCase(cipherName)) {
          throw new RuntimeException(
              "Encryption for family '" + family.getNameAsString() + "' configured with type '"
                  + cipherName + "' but key specifies algorithm '" + cipher.getName() + "'");
        }
      } else {
        // Family does not provide key material, create a random key
        cipher = Encryption.getCipher(conf, cipherName);
        if (cipher == null) {
          throw new RuntimeException("Cipher '" + cipherName + "' is not available");
        }
        key = cipher.getRandomKey();
      }
      Encryption.Context cryptoContext = Encryption.newContext(conf);
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
      return cryptoContext;
    }
    return Encryption.Context.NONE;
  }

  public static Writer createWriter(FileSystem fs, Configuration conf, HColumnDescriptor family,
      Path familyPath, InetSocketAddress[] favoredNodes) throws IOException {
    Algorithm compression = family.getCompressionType();
    if (compression == null) {
      compression = HFile.DEFAULT_COMPRESSION_ALGORITHM;
    }
    Encryption.Context cryptoContext = createEncryptionContext(conf, family);
    boolean includeTags = false;
    if (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
      includeTags = true;
    }
    HFileContext hFileContext = new HFileContextBuilder().withCompression(compression)
        .withHBaseCheckSum(true).withChecksumType(HStore.getChecksumType(conf))
        .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
        .withBlockSize(family.getBlocksize())
        .withDataBlockEncoding(family.getDataBlockEncoding())
        .withEncryptionContext(cryptoContext).withIncludesTags(includeTags)
        .withCreateTime(EnvironmentEdgeManager.currentTime()).build();
    // disable block cache in writing
    Configuration tmpConf = new Configuration(conf);
    tmpConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    return new WriterBuilder(conf, new CacheConfig(tmpConf), new HFileSystem(fs))
        .withBloomType(family.getBloomFilterType()).withComparator(KeyValue.COMPARATOR)
        .withFileContext(hFileContext)
        .withFilePath(new Path(familyPath, UUID.randomUUID().toString().replaceAll("-", "")))
        .withFavoredNodes(favoredNodes).build();
  }
}
