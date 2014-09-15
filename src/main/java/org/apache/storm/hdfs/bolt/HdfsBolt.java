/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.hdfs.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class HdfsBolt extends AbstractHdfsBolt {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsBolt.class);

  private transient OutputStream out;
  private RecordFormat format;
  private long offset = 0;
  private String compressionCodecClassname = null;
  private CompressionCodec compressionCodec = null;

  public HdfsBolt withFsUrl(String fsUrl) {
    this.fsUrl = fsUrl;
    return this;
  }

  public HdfsBolt withConfigKey(String configKey) {
    this.configKey = configKey;
    return this;
  }

  public HdfsBolt withFileNameFormat(FileNameFormat fileNameFormat) {
    this.fileNameFormat = fileNameFormat;
    return this;
  }

  public HdfsBolt withRecordFormat(RecordFormat format) {
    this.format = format;
    return this;
  }

  public HdfsBolt withSyncPolicy(SyncPolicy syncPolicy) {
    this.syncPolicy = syncPolicy;
    return this;
  }

  public HdfsBolt withRotationPolicy(FileRotationPolicy rotationPolicy) {
    this.rotationPolicy = rotationPolicy;
    return this;
  }

  public HdfsBolt setHdfsConfig(String key, String value) {
    this.customHdfsConfig.put(key, value);
    return this;
  }

  public HdfsBolt withCompressionCodec(String codeClass) {
    this.compressionCodecClassname = codeClass;
    return this;
  }

  public HdfsBolt withConfigXML(String xmlPath) throws IOException {
    Configuration conf = new Configuration();
    InputStream in = null;
    File f = new File(xmlPath);
    if (f.exists()) {
      in = new FileInputStream(f);
    } else {
      in = ClassLoader.class.getResourceAsStream(xmlPath);
    }
    if (in != null) {
      conf.addResource(in);
      Iterator<Entry<String, String>> itr = conf.iterator();
      Entry<String, String> entry = null;
      while (itr.hasNext()) {
        entry = itr.next();
        this.customHdfsConfig.put(entry.getKey(), entry.getValue());
      }
      in.close();
    }

    return this;
  }

  public HdfsBolt addRotationAction(RotationAction action) {
    this.rotationActions.add(action);
    return this;
  }

  @Override
  public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
    LOG.info("Preparing HDFS Bolt...");
    Iterator<Entry<String, String>> itr = hdfsConfig.iterator();
    while (itr.hasNext()) {
      Entry<String, String> entry = itr.next();
    }
    this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
  }

  @Override
  public void execute(Tuple tuple) {
    try {
      byte[] bytes = this.format.format(tuple);
      synchronized (this.writeLock) {
        out.write(bytes);
        this.offset += bytes.length;

        if (this.syncPolicy.mark(tuple, this.offset)) {
          if (this.out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
          } else {
            this.out.flush();
          }
          this.syncPolicy.reset();

          // Rotate only after sync
          if (this.rotationPolicy.mark(tuple, this.offset)) {
            rotateOutputFile(); // synchronized
            this.offset = 0;
            this.rotationPolicy.reset();
          }
        }
      }

      this.collector.ack(tuple);

    } catch (IOException e) {
      LOG.warn("write/sync failed.", e);
      this.collector.fail(tuple);
    }
  }

  @Override
  void closeOutputFile() throws IOException {
    this.out.close();
  }

  @Override
  Path createOutputFile() throws IOException {
    compressionCodec = getCompressionCodec() ;
    String codecExtn = "";
    if (compressionCodec != null) {
      codecExtn = compressionCodec.getDefaultExtension();
    }
    Path path = new Path(this.fileNameFormat.getPath(), this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()) + codecExtn);
    this.out = this.fs.create(path);

    if (compressionCodec != null) {
        this.out = compressionCodec.createOutputStream(out);
    }

    return path;
  }

  private CompressionCodec getCompressionCodec() throws IOException {
    if (compressionCodec == null && compressionCodecClassname != null) {
      try{
      Class<?> codecClass = Class.forName(compressionCodecClassname);
      compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, hdfsConfig);
      } catch (ClassNotFoundException e) {
        LOG.error("Failed to locate compression codec class " + compressionCodecClassname, e);
        throw new IOException(e);
      }
    }
    return compressionCodec;
  }
}
