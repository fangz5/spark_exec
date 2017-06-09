import org.apache.spark.streaming.StreamingContext._

val ssc = new StreamingContext(sc, Seconds(5))

// Set Stream source
val lines1 = ssc.socketTextStream("localhost", 9999)
val lines2 = ssc.socketTextStream("localhost", 9998)

// Define Stream operations
val words1 = lines1.flatMap(_.split(" "))
val words2 = lines2.flatMap(_.split(" "))
val pairs1 = words1.map(word => (word, 1))
val pairs2 = words2.map(word => (word, 1))
val wordCounts1 = pairs1.reduceByKey(_ + _)
val wordCounts2 = pairs2.reduceByKey(_ + _)
wordCounts1.print()
wordCounts2.print()

// Start service
// Input source should be started using NetCat: nc -lk 9999
ssc.start()

// To stop
// ssc.stop(stopSparkContext = false)