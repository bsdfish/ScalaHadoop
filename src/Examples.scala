package com.asimma.ScalaHadoop;

import org.apache.hadoop.io._;
import MapReduceTaskChain._;
import ImplicitConversion._;
import scala.reflect.Manifest;
import scala.collection.JavaConversions._ 


  

object TokenizerMap extends TypedMapper[LongWritable, Text, Text, LongWritable] {
  override def map(k: LongWritable, v: Text, context: ContextType) : Unit =  
    v split " \t" foreach ((word) => context.write(word, 1L))
}

object TokenizerMap1 extends TypedMapper[LongWritable, Text, Text, LongWritable] {
  override def doMap : Unit = v split " |\t" foreach ((word) => context.write(word, 1L))
}

object FlipKeyValueMap extends TypedMapper[Text, LongWritable, LongWritable, Text] {
  override def map(k: Text, v:LongWritable, context: ContextType) : Unit = 
    context.write(v, k);
}

object SumReducer extends TypedReducer[Text, LongWritable, Text, LongWritable] {
  override def reduce(k: Text, v: java.lang.Iterable[LongWritable], context: ContextType) : Unit =
    context.write(k, (0L /: v) ((total, next) => total+next))
}


object SumReducer1 extends TypedReducer[Text, LongWritable, Text, LongWritable] {
  override def  doReduce :Unit = context.write(k, (0L /: v) ((total, next) => total+next))
}

object WordListReducer extends TypedReducer[LongWritable, Text, LongWritable, Text] {
  override def doReduce: Unit = context write (k, (new StringBuilder /: v) ((soFar, newString) => soFar.append(newString + " ")));

  /*If you're not comfortable with reduce, could also be written as
    val builder = newStringBuilder;
    v foreach(t => builder.append  (t + " "))
    context write (k,v toString)
    */
}



object WordCount extends ScalaHadoopTool{ 
  def run(args: Array[String]) : Int = {  
    val c = MapReduceTaskChain.init() -->
    IO.Text[LongWritable, Text](args(0)).input                    -->  
    MapReduceTask.MapReduceTask(TokenizerMap1, SumReducer)         -->
    IO.Text[Text, LongWritable](args(1)).output;
    c.execute();
    return 0;
  }
}

object WordsWithSameCount extends ScalaHadoopTool {
  def run(args: Array[String]) : Int = {  
    val c = MapReduceTaskChain.init() -->
    IO.Text[LongWritable, Text](args(0)).input                    -->  
    MapReduceTask.MapReduceTask(TokenizerMap1, SumReducer)        -->
    MapReduceTask.MapReduceTask(FlipKeyValueMap, WordListReducer) -->
    IO.Text[LongWritable, Text](args(1)).output;
    c.execute();
    return 0;
  }
}
