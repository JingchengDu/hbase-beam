package org.apache.hadoop.hbase.beam;

import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;

public class HBasePipelineFactory {

  public Pipeline createPipeline(Configuration conf) throws IOException {
    String className = conf.get(HBaseBeamConstants.HBASE_BEAM_RUNNER_KEY,
      HBaseBeamConstants.DEFAULT_HBASE_BEAM_RUNNER);
    Class runnerClazz = null;
    try {
      runnerClazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new DoNotRetryIOException("Failed to find the class " + className, e);
    }
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(runnerClazz);
    return Pipeline.create(options);
  }
}
