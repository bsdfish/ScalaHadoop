package com.asimma.ScalaHadoop;

abstract class ScalaHadoopTool extends org.apache.hadoop.conf.Configured with org.apache.hadoop.util.Tool  {

  def main(args: Array[String]) = {
    org.apache.hadoop.util.ToolRunner.run(this, args);
  }
}
