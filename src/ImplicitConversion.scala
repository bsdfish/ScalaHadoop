// This is inspired by Shadoop 
// (http://blog.jonhnnyweslley.net/2008/05/shadoop.html) 
package com.asimma.ScalaHadoop

import org.apache.hadoop.io._

object ImplicitConversion {
  // Handle BooleanWritable
  implicit def BooleanWritableUnbox(v: BooleanWritable) = v.get
  implicit def BooleanWritableBox  (v: Boolean) = new BooleanWritable(v)

  // Handle DoubleWritable
  implicit def DoubleWritableUnbox(v: DoubleWritable) = v.get
  implicit def DoubleWritableBox  (v: Double) = new DoubleWritable(v)

  // Handle FloatWritable
  implicit def FloatWritableUnbox(v: FloatWritable) = v.get
  implicit def FloatWritableBox  (v: Float) = new FloatWritable(v)

  // Handle IntWritable
  implicit def IntWritableUnbox(v: IntWritable) = v.get
  implicit def IntWritableBox  (v: Int) = new IntWritable(v)

  // Handle LongWritable
  implicit def LongWritableUnbox(v: LongWritable) = v.get
  implicit def LongWritableBox  (v: Long) = new LongWritable(v)

  // Handle Text
  implicit def TextUnbox(v: Text) = v.toString
  implicit def TextBox  (v: String) = new Text(v)
  implicit def StringBuilderBox  (v: StringBuilder) = new Text(v.toString)
  implicit def StringBufferBox  (v: StringBuffer) = new Text(v.toString)


  implicit def MapWritableBox[X <: Writable,Y <: Writable](value: scala.collection.Map[X,Y]): MapWritable  =  {
      var newMap = new MapWritable();
      value.foreach{case (k, v)  => newMap.put(k, v)};
      return newMap; }
}
