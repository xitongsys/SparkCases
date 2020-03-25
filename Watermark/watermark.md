# watermark

In `Dataset.scala`, withWatermark create the EventTimeWatermark logical node.
```scala
  def withWatermark(eventTime: String, delayThreshold: String): Dataset[T] = withTypedPlan {
    val parsedDelay =
      try {
        CalendarInterval.fromCaseInsensitiveString(delayThreshold)
      } catch {
        case e: IllegalArgumentException =>
          throw new AnalysisException(
            s"Unable to parse time delay '$delayThreshold'",
            cause = Some(e))
      }
    require(parsedDelay.milliseconds >= 0 && parsedDelay.months >= 0,
      s"delay threshold ($delayThreshold) should not be negative.")
    EliminateEventTimeWatermark(
      EventTimeWatermark(UnresolvedAttribute(eventTime), parsedDelay, logicalPlan))
  }
  ```

In `SparkStrategies.scala`, it convert logical node to physical node EventTimeWatermark
```scala
 /**
   * Used to plan streaming aggregation queries that are computed incrementally as part of a
   * [[StreamingQuery]]. Currently this rule is injected into the planner
   * on-demand, only when planning in a [[org.apache.spark.sql.execution.streaming.StreamExecution]]
   */
  object StatefulAggregationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case _ if !plan.isStreaming => Nil

      case EventTimeWatermark(columnName, delay, child) =>
        EventTimeWatermarkExec(columnName, delay, planLater(child)) :: Nil
```
`EventTimeWatermarkExec.scala` define the physical node
```scala
/**
 * Used to mark a column as the containing the event time for a given record. In addition to
 * adding appropriate metadata to this column, this operator also tracks the maximum observed event
 * time. Based on the maximum observed time and a user specified delay, we can calculate the
 * `watermark` after which we assume we will no longer see late records for a particular time
 * period. Note that event time is measured in milliseconds.
 */
case class EventTimeWatermarkExec(
    eventTime: Attribute,
    delay: CalendarInterval,
    child: SparkPlan) extends UnaryExecNode {

  val eventTimeStats = new EventTimeStatsAccum()
  val delayMs = EventTimeWatermark.getDelayMs(delay)

  sparkContext.register(eventTimeStats)

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      val getEventTime = UnsafeProjection.create(eventTime :: Nil, child.output)
      iter.map { row =>
        eventTimeStats.add(getEventTime(row).getLong(0) / 1000)
        row
      }
    }
  }
```

It defines a accumulate variable `eventTimeStats` to record the statistical event time.

`MicroBatchExecution.scala` will update the global watermark after run batch
```scala
    withProgressLocked {
      watermarkTracker.updateWatermark(lastExecution.executedPlan)
      commitLog.add(currentBatchId, CommitMetadata(watermarkTracker.currentWatermark))
      committedOffsets ++= availableOffsets
    }
    logDebug(s"Completed batch ${currentBatchId}")
  }
```

`WatermarkTracker.scala` define the updateWatermark. This function fill collect all EventTimeWatermarkExec in the physical plan and get the eventtime of every node. **If no data int that node, it will use 0 as the watermark of that node. It means if no data of some stage, the wathermark will be 0 which is the smallest value.**

```scala
 /** Tracks the watermark value of a streaming query based on a given `policy` */
case class WatermarkTracker(policy: MultipleWatermarkPolicy) extends Logging {
  private val operatorToWatermarkMap = mutable.HashMap[Int, Long]()
  private var globalWatermarkMs: Long = 0

  def setWatermark(newWatermarkMs: Long): Unit = synchronized {
    globalWatermarkMs = newWatermarkMs
  }

  def updateWatermark(executedPlan: SparkPlan): Unit = synchronized {
    val watermarkOperators = executedPlan.collect {
      case e: EventTimeWatermarkExec => e
    }
    if (watermarkOperators.isEmpty) return

    watermarkOperators.zipWithIndex.foreach {
      case (e, index) if e.eventTimeStats.value.count > 0 =>
        logDebug(s"Observed event time stats $index: ${e.eventTimeStats.value}")
        val newWatermarkMs = e.eventTimeStats.value.max - e.delayMs
        val prevWatermarkMs = operatorToWatermarkMap.get(index)
        if (prevWatermarkMs.isEmpty || newWatermarkMs > prevWatermarkMs.get) {
          operatorToWatermarkMap.put(index, newWatermarkMs)
        }

      // Populate 0 if we haven't seen any data yet for this watermark node.
      case (_, index) =>
        if (!operatorToWatermarkMap.isDefinedAt(index)) {
          operatorToWatermarkMap.put(index, 0)
        }
    }

    // Update the global watermark to the minimum of all watermark nodes.
    // This is the safest option, because only the global watermark is fault-tolerant. Making
    // it the minimum of all individual watermarks guarantees it will never advance past where
    // any individual watermark operator would be if it were in a plan by itself.
    val chosenGlobalWatermark = policy.chooseGlobalWatermark(operatorToWatermarkMap.values.toSeq)
    if (chosenGlobalWatermark > globalWatermarkMs) {
      logInfo(s"Updating event-time watermark from $globalWatermarkMs to $chosenGlobalWatermark ms")
      globalWatermarkMs = chosenGlobalWatermark
    } else {
      logDebug(s"Event time watermark didn't move: $chosenGlobalWatermark < $globalWatermarkMs")
    }
  }

  def currentWatermark: Long = synchronized { globalWatermarkMs }
}
```
GlobalWatermark will be written in checkpoints `offsets` and `commits`, which are defined in `StreamExecution`
```scala
  /**
   * A write-ahead-log that records the offsets that are present in each batch. In order to ensure
   * that a given batch will always consist of the same data, we write to this log *before* any
   * processing is done.  Thus, the Nth record in this log indicated data that is currently being
   * processed and the N-1th entry indicates which offsets have been durably committed to the sink.
   */
  val offsetLog = new OffsetSeqLog(sparkSession, checkpointFile("offsets"))

  /**
   * A log that records the batch ids that have completed. This is used to check if a batch was
   * fully processed, and its output was committed to the sink, hence no need to process it again.
   * This is used (for instance) during restart, to help identify which batch to run next.
   */
  val commitLog = new CommitLog(sparkSession, checkpointFile("commits"))
```

`offsets` example: `cf adl cat application_1583869491667_14542/checkpoint/offsets/0`
```json
v1
{"batchWatermarkMs":0,"batchTimestampMs":1584612918417,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStorePr
ovider","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion"
:"2","spark.sql.shuffle.partitions":"512"}}
{"uetevents-raw":{"137":10605526,"146":10605427,"218":1
```

`commits` example: ` cf adl cat application_1583869491667_14542/checkpoint/commits/0`
```json
v1
{"nextBatchWatermarkMs":0}
```

In `IncrementalExecution.scala`, when construct next batch execution, it will set the new watermark in some operators(i.e. FlatMapGroupWithStates) which need it.
```scala
      case m: FlatMapGroupsWithStateExec =>
        m.copy(
          stateInfo = Some(nextStatefulOperationStateInfo),
          batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
          eventTimeWatermark = Some(offsetSeqMetadata.batchWatermarkMs))
```