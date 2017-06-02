class CustomSparkListener extends org.apache.spark.scheduler.SparkListener {
  override def onJobStart(jobStart: org.apache.spark.scheduler.SparkListenerJobStart) {
    println(s"Job started with ${jobStart.stageInfos.size} stages: $jobStart")
  }

  override def onStageCompleted(stageCompleted: org.apache.spark.scheduler.SparkListenerStageCompleted): Unit = {
    println(s"Stage ${stageCompleted.stageInfo.stageId} completed with ${stageCompleted.stageInfo.numTasks} tasks.")
  }
}

sc.addSparkListener(new CustomSparkListener)

sc.parallelize(1 to 50).collect
