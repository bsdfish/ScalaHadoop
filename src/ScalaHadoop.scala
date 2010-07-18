package com.asimma.ScalaHadoop;

import org.apache.hadoop.mapreduce._;
import org.apache.hadoop.conf._;
import org.apache.hadoop.io._;
import org.apache.hadoop.fs.Path

import scala.reflect.Manifest;

import MapReduceTaskChain._;
import IO._;

object IO {

  class Input[K, V] (
     val dirName       : String,
     val inFormatClass : java.lang.Class[_ <: InputFormat[K,V]]
  ) {}

  class Output[K,V] (
    val dirName        : String,
    val outFormatClass : java.lang.Class[_ <: lib.output.FileOutputFormat[K,V]]
  ){}


  /** This is a general class for inputs and outputs into the Map Reduce jobs.  Note that it's possible to
      write one type to a file and then have it be read as something else.  */
      
  class IO[KWRITE, VWRITE, KREAD, VREAD] 
    (dirName        : String, 
     inFormatClass  : Class[_ <: InputFormat[KREAD, VREAD]], 
     outFormatClass : Class[lib.output.FileOutputFormat[KWRITE,VWRITE]]) { 
       val input:  Input[KREAD, VREAD]   = new Input(dirName,  inFormatClass);
       val output: Output[KWRITE,VWRITE] = new Output(dirName, outFormatClass);
      }


  def SeqFile[K,V](dirName : String) 
    (implicit mIn:   Manifest[lib.input.SequenceFileInputFormat[K,V]],
              mOut:  Manifest[lib.output.SequenceFileOutputFormat[K,V]]) = 
    new IO[K,V,K,V](dirName, 
                    mIn .erasure.asInstanceOf[Class[lib.input.FileInputFormat[K,V]]],
                    mOut.erasure.asInstanceOf[Class[lib.output.FileOutputFormat[K,V]]]);


  def Text[K,V](dirName : String)
    (implicit mIn:   Manifest[lib.input.TextInputFormat],
              mOut:  Manifest[lib.output.TextOutputFormat[K,V]]) = 
    new IO[K,V,LongWritable,Text](dirName, 
                    mIn .erasure.asInstanceOf[Class[lib.input.FileInputFormat[LongWritable,Text]]],
                    mOut.erasure.asInstanceOf[Class[lib.output.FileOutputFormat[K,V]]]);

}


object MapReduceTaskChain {
  val rand = new scala.util.Random();

  // Generic parameter setter
  trait ConfModifier { 
    def apply(c: Configuration) : Unit; 
  }

  class SetParam(val param: String, val value: String) extends ConfModifier {
     def apply(c: Configuration) = {c.set(param, value);} 
  }
  def Param(p: String, v: String) = new SetParam(p,v); 


  def init(conf: Configuration ) : MapReduceTaskChain[None.type, None.type, None.type, None.type] =  {
     val c = new MapReduceTaskChain[None.type, None.type, None.type, None.type]();
     c.conf  = conf;
     return c; 
   }
  def init(): MapReduceTaskChain[None.type, None.type, None.type, None.type] = init(new Configuration);
}

/**
  A class representing a bunch (one or more) of map and reduce operations, as well as 
  all the additional parameters passed on to the Hadoop engine relating to the operations.  

  <p>
  The expected usage is basically

  <pre>
  var a = Input("foo") --> Mapper1 --> Mapper2 --> Reducer --> Mapper3 --> Reducer2 --> Output("bar");
  a.execute;
  </pre>
  */
class MapReduceTaskChain[KIN, VIN, KOUT, VOUT] extends Cloneable {

  // The type of this chain link, to make some of the other functions more concise.
  type thisType =  MapReduceTaskChain[KIN, VIN, KOUT, VOUT];

  /** A pointer to the previous node in the chain; is null for the first link.  The type of prev is
  MapReduceTaskChain[_,_,K_FOR_MAP_TASK, V_FOR_MAP_TASK] but I don't want to introduce extra types into
  the parameters */
  var prev: MapReduceTaskChain[_, _,_,_] = null;

  /** The task that we need to execute, the first try type parameters have to be equal to 
      the last 2 type parameters of prev */
  var task: MapReduceTask[_,_,KOUT,VOUT] = null;

  var conf          : Configuration      = null;
  var confModifiers : List[ConfModifier] = List[ConfModifier]();

  val tmpDir : String  = "tmp/tmp-" + MapReduceTaskChain.rand.nextLong();

  // TODO:  This is a type system disaster, but the alternatives are worse
  var nextInput:  IO.Input[KOUT,VOUT]    = 
      new IO.Input(tmpDir,  classOf[lib.input.SequenceFileInputFormat[KOUT,VOUT]]);
  var output:     IO.Output[KOUT,VOUT]   = 
      new IO.Output(tmpDir,  classOf[lib.output.SequenceFileOutputFormat[KOUT,VOUT]]);


  def cloneTypesafe() : thisType  = clone().asInstanceOf[thisType];


  /** Returns a new MapReduceTaskChain that, in addition to performing everything specified by
     the current chain, also performs the MapReduceTask passed in */
  def -->[KOUT1, VOUT1](mrt: MapReduceTask[KOUT, VOUT, KOUT1, VOUT1]) : MapReduceTaskChain[KIN, VIN, KOUT1, VOUT1] = {
    val  chain= new  MapReduceTaskChain[KIN, VIN, KOUT1, VOUT1]();
    chain.prev = this;
    chain.task = mrt;
    return chain;
  }

  /** Adds a new link in the chain with a new node corresponding to executing that Map task */
  def -->[KOUT1, VOUT1](mapper: TypedMapper[KOUT, VOUT, KOUT1, VOUT1])
                : MapReduceTaskChain[KIN, VIN, KOUT1, VOUT1] = this --> MapReduceTask.fromMapper(mapper);


  /** Add a confModifier to the current task by returning a copy of this chain 
      with the confModifier pushed onto the confModifier list */
  def -->(confModifier: ConfModifier) : thisType = {
    val chain = cloneTypesafe;
    chain.confModifiers = confModifier::chain.confModifiers;
    return chain;
  }


  /** Adds an input source to the chain */
  def -->[K,V](in : IO.Input[K,V]): MapReduceTaskChain[KIN, VIN, K, V] = {
    val  chain = new  MapReduceTaskChain[KIN, VIN, K, V]();
    chain.prev      = this;
    chain.nextInput = in;
    return chain;
  }

  /** Adds an output sink to the chain */
  def -->(out : IO.Output[KOUT,VOUT]): thisType  = {
    this.output = out;
    return this;
  }


  def execute() : Boolean = {
    if(prev != null)
      prev.execute();

    if(task != null) {
      val conf = getConf; 
      // Off the bat, apply the modifications from all the ConfModifiers we have queued up at this node.
      confModifiers map ((mod : ConfModifier) => mod(conf));

      val job  = new Job(conf, task.name);
      job setJarByClass          classOf[MapOnlyTask[_,_,_,_]];
      task initJob job ;

      // Should never be null; set to SequenceFile with a random temp dirname by default
      job setInputFormatClass    prev.nextInput.inFormatClass; 
      job setOutputFormatClass   output.outFormatClass;

      lib.input.FileInputFormat.addInputPath(job, new Path(prev.nextInput.dirName));
      lib.output.FileOutputFormat.setOutputPath(job, new Path(output.dirName));


      job waitForCompletion true;
      return true;
    }

    return true;
  }

  def getConf : Configuration = if(conf == null) prev.getConf else conf;
}


abstract class MapReduceTask[KIN, VIN, KOUT, VOUT]   {

  // TODO: Should this be Writable?
  protected var mapper   : TypedMapper[KIN, VIN, _, _] = null ;
  protected var reducer  : TypedReducer[_, _, KOUT, VOUT]   = null;

  var name = "NO NAME"; 

  def initJob(job: Job) = {
    job setMapperClass         mapper.getClass.asInstanceOf[java.lang.Class[ Mapper[_,_,_,_]]];
    if(reducer != null) {
        job setReducerClass       reducer.getClass.asInstanceOf[java.lang.Class[ Reducer[_,_,_,_]]];
        job setOutputKeyClass     reducer.kType;
        job setOutputValueClass   reducer.vType;
      }
      else {
        job setOutputKeyClass     mapper.kType;
        job setOutputValueClass   mapper.vType;
      }
    }

}

class MapOnlyTask[KIN, VIN, KOUT, VOUT] 
      extends MapReduceTask[KIN, VIN, KOUT, VOUT]    { }

class MapAndReduceTask[KIN, VIN, KOUT, VOUT] 
      extends MapReduceTask[KIN, VIN, KOUT, VOUT]    { }


object MapReduceTask {

  // KINT and VINT are the key/value types of the intermediate steps
    implicit def fromMapper[KIN, VIN, KOUT, VOUT](mapper: TypedMapper[KIN, VIN, KOUT, VOUT]) : MapOnlyTask[KIN,VIN,KOUT,VOUT] = {
      val mapReduceTask= new MapOnlyTask[KIN, VIN, KOUT, VOUT]();
      mapReduceTask.mapper = mapper;
      return mapReduceTask;
    }


    def MapReduceTask[KIN, VIN, KOUT, VOUT, KTMP, VTMP]
         (mapper: TypedMapper[KIN, VIN, KTMP, VTMP], reducer: TypedReducer[KTMP, VTMP, KOUT, VOUT], name: String)
         : MapAndReduceTask[KIN, VIN, KOUT, VOUT] = {
           val mapReduceTask= new MapAndReduceTask[KIN, VIN, KOUT, VOUT]();
           mapReduceTask.mapper  = mapper;
           mapReduceTask.reducer = reducer;
           mapReduceTask.name    = name;
           return mapReduceTask;
         }

    def MapReduceTask[KIN, VIN, KOUT, VOUT, KTMP, VTMP]
         (mapper: TypedMapper[KIN, VIN, KTMP, VTMP], reducer: TypedReducer[KTMP, VTMP, KOUT, VOUT])
         : MapAndReduceTask[KIN, VIN, KOUT, VOUT] = MapReduceTask(mapper, reducer, "NO NAME");

}


trait OutTyped[KOUT, VOUT] {
  def kType : java.lang.Class[KOUT];
  def vType : java.lang.Class[VOUT];
}

abstract class TypedMapper[KIN, VIN, KOUT, VOUT] (implicit kTypeM: Manifest[KOUT], vTypeM: Manifest[VOUT]) 
         extends Mapper[KIN, VIN, KOUT, VOUT] with OutTyped[KOUT, VOUT] 
  { 
    def kType = kTypeM.erasure.asInstanceOf[Class[KOUT]];
    def vType = vTypeM.erasure.asInstanceOf[Class[VOUT]];
    type ContextType =  Mapper[KIN, VIN, KOUT, VOUT]#Context;

    var k: KIN = _;
    var v: VIN = _;
    var context : ContextType = _;

    override def map(k: KIN, v:VIN, context: ContextType) : Unit =  { 
      this.k = k;
      this.v = v;
      this.context = context;
      doMap;
    }

    def doMap : Unit = {}

  } 
abstract class TypedReducer[KIN, VIN, KOUT, VOUT] (implicit kTypeM: Manifest[KOUT], vTypeM: Manifest[VOUT])  
         extends Reducer[KIN, VIN, KOUT, VOUT] with OutTyped[KOUT, VOUT] {
  type ContextType =  Reducer[KIN, VIN, KOUT, VOUT]#Context
  def kType = kTypeM.erasure.asInstanceOf[Class[KOUT]];
  def vType = vTypeM.erasure.asInstanceOf[Class[VOUT]];

  var k: KIN = _;
  var v: java.lang.Iterable[VIN] = _;
  var context: ContextType = _;

  override def reduce(k: KIN, v: java.lang.Iterable[VIN], context: ContextType) : Unit = {
      this.k = k;
      this.v = v;
      this.context = context;
      doReduce;
   }
   def doReduce :Unit = {}
} 


