import org.apache.spark.streaming.StreamingContext._

val ssc = new StreamingContext(sc, Seconds(3))

// Set Stream source
val lines = ssc.socketTextStream("localhost", 9999)

// Define Stream operations
val words = lines.flatMap(_.split(" "))
val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey(_ + _)
wordCounts.print()

// Start service
// Input source should be started using NetCat: nc -lk 9999
ssc.start()

// To stop
// ssc.stop(stopSparkContext = false)