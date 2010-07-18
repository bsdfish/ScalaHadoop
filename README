This code provides some syntactic sugar on top of Hadoop in order to make
it more usable from Scala.  Take a look at Examples.scala for more
details.

A basic mapper looks like

object TokenizerMap extends TypedMapper[LongWritable, Text, Text, LongWritable] {
  override def map(k: LongWritable, v: Text, context: ContextType) : Unit =  
    v split " \t" foreach ((word) => context.write(word, 1L))
}

or, you can also write it as 

object TokenizerMap1 extends TypedMapper[LongWritable, Text, Text, LongWritable] {
  override def doMap : Unit = v split " |\t" foreach ((word) => context.write(word, 1L))
}

and a reducer

object SumReducer1 extends TypedReducer[Text, LongWritable, Text, LongWritable] {
  override def  doReduce :Unit = context.write(k, (0L /: v) ((total, next) => total+next))
}

Note that implicit conversion is used to convert between LongWritable and longs, as well as Text
and Strings.  The types of the input and output parameters only need to be stated as the
generic specliazers of the class it extends.

These mappers and reducers can be chained together with the --> operator 

object WordCount extends ScalaHadoopTool{ 
  def run(args: Array[String]) : Int = {  
    (MapReduceTaskChain.init() -->
     IO.Text[LongWritable, Text](args(0)).input                    -->  
     MapReduceTask.MapReduceTask(TokenizerMap1, SumReducer)         -->
     IO.Text[Text, LongWritable](args(1)).output) execute;
    return 0;
  }
}

Multiple map/reduce runs can be chained together

object WordsWithSameCount extends ScalaHadoopTool {
  def run(args: Array[String]) : Int = {  
    (MapReduceTaskChain.init() -->
    IO.Text[LongWritable, Text](args(0)).input                    -->  
    MapReduceTask.MapReduceTask(TokenizerMap1, SumReducer)        -->
    MapReduceTask.MapReduceTask(FlipKeyValueMap, WordListReducer) -->
    IO.Text[LongWritable, Text](args(1)).output) execute;
    return 0;
  }
}
