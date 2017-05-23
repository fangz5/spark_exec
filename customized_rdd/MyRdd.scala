import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.{TaskContext, SparkContext, Partition}

// Import did not work for class definition when using spark shell. Full class pathes used below.

case class MyPartition(index: Int) extends org.apache.spark.Partition


class MyRDD(sc: org.apache.spark.SparkContext, numParts: Int, limit: Int) extends org.apache.spark.rdd.RDD[Int](sc, Nil) {
  import org.apache.spark.{Partition, TaskContext}

  override def getPartitions: Array[Partition] = {
    /**
     * Note that # of partitions is passed by a parameter, instead of using sc.defaultParallelism.
     * This is because SparkContext is NOT serializable. 
     */
    (0 until numParts).toArray.map(MyPartition)
  }

  override def compute(part: Partition, context: TaskContext) = new Iterator[Int] {
    var count: Int = 0

    def hasNext: Boolean = count < limit

    def next() = {
      val nextInt: Int = count
      count += 1
      nextInt
    }
  }

}

val rdd = new MyRDD(sc, sc.defaultParallelism, 10)

rdd.collect.foreach(println)