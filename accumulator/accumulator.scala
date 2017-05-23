import org.apache.spark.{SparkConf, SparkContext, TaskContext, AccumulatorParam}                    
                                                                                                    
object RunJob {                                                                                     
  val sparkConf = new SparkConf().setMaster("spark://fang-m:7077").setAppName("RunJob2")            
  val sc = new SparkContext(sparkConf)                                                              
                                                                                                    
var workers2 = sc.broadcast(Array("lev1", "lev2", "lev3", "lev4"))                               
  var workers = sc.accumulator(Seq[String]())(new StringAcc)                                        
  workers += List("lev1", "lev2", "lev3", "lev4")                                                   
                                                                                                    
  def main(args: Array[String]){                                                                    
                                                                                                    
    println(sc.defaultParallelism)                                                                  
    sc.parallelize(1 to 1024).collect                                                               
    println(sc.defaultParallelism)                                                                  
                                                                                                    
    //goo                                                                                           
                                                                                                    
    //workers += List("lev5", "lev6")                                                               
                                                                                                    
    val rdd = sc.parallelize(1 to 100)                                                     
    val res = sc.runJob(rdd, foo _)                                                                 
                                                                                                    
    res.foreach(println)                                                                            
    println("partitions:" + rdd.getNumPartitions)                                                   
    doo                                                                                             
                                                                                                    
    /*                                                                                              
    val res2 = sc.runJob(rdd, foo _)                                                                
    res2.foreach(println)                                                                           
    doo                                                                                             
    */                                                                                              
                                                                                                    
  }                                                                                                 
                                                                                                    
  def goo: Unit = {
    sc.runJob(sc.parallelize(1 to 10), foo _)
  }                                                        
                                                                                                    
  def foo(context: TaskContext, iterator: Iterator[Int]): String = {                                
    val nodeId = context.taskMetrics().hostname                                                     
    workers += List(nodeId)                                                                         
    //workers.value.mkString(",")   
    //workers2.value.mkString(" ") 
    nodeId
  }                                                                                                 
                                                                                                    
  def doo: Unit = {                                                                                 
    val rdd = sc.parallelize(1 to 10)
    rdd.foreach(i => workers += List(i.toString))
  }                                                                                                 
                                                                                                    
  class StringAcc extends AccumulatorParam[Seq[String]] {                                           
    def addInPlace(t1: Seq[String], t2: Seq[String]): Seq[String] = t1 ++ t2                        
    def zero(initialValue: Seq[String]): Seq[String] = Seq[String]()                                
  }    

   def run(){                                                                    
                                                                                                                                                                                                        
     val rdd = sc.parallelize(1 to 100)                                                     
    val res = sc.runJob(rdd, foo _)                                                                 
                                                                                                    
    res.foreach(println)                                                                            
    println("partitions:" + rdd.getNumPartitions)                                                   
    //doo                                                                                             
                                                                                                    
    /*                                                                                              
    val res2 = sc.runJob(rdd, foo _)                                                                
    res2.foreach(println)                                                                           
    doo                                                                                             
    */                                                                                              
                                                                                                    
  }   


                          