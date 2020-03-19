# Go through flatMapGroupsWithState codes

In my case, I need to redefine `flatMapGroupsWithState`. Firstly I will go through the codes in Spark.

Using `flatMapGroupsWithState` codes
```java
Dataset<Row02> data01 = data
                .withWatermark("timestamp", "30s")
                .groupByKey((MapFunction<Row01, Integer>)e -> e.GetKey(), Encoders.INT())
                .flatMapGroupsWithState(
                        new OwnReducer(),
                        OutputMode.Append(),
                        Encoders.bean(OwnState.class),
                        Encoders.bean(Row02.class),
                        GroupStateTimeout.NoTimeout());
```

After call `groupByKey`(a function of Dataset), it will return a `KeyValueGroupedDataset`
```scala
  @InterfaceStability.Evolving
  def groupByKey[K: Encoder](func: T => K): KeyValueGroupedDataset[K, T] = {
    val withGroupingKey = AppendColumns(func, logicalPlan)
    val executed = sparkSession.sessionState.executePlan(withGroupingKey)

    new KeyValueGroupedDataset(
      encoderFor[K],
      encoderFor[T],
      executed,
      logicalPlan.output,
      withGroupingKey.newColumns)
  }
```

Actually it just appends new columns to every row, which used as the key of the row. 

`flatMapGroupsWithState` is a function of `KeyValueGroupedDataset`
```scala
 /**
   * ::Experimental::
   * (Scala-specific)
   * Applies the given function to each group of data, while maintaining a user-defined per-group
   * state. The result Dataset will represent the objects returned by the function.
   * For a static batch Dataset, the function will be invoked once per group. For a streaming
   * Dataset, the function will be invoked for each group repeatedly in every trigger, and
   * updates to each group's state will be saved across invocations.
   * See `GroupState` for more details.
   *
   * @tparam S The type of the user-defined state. Must be encodable to Spark SQL types.
   * @tparam U The type of the output objects. Must be encodable to Spark SQL types.
   * @param func Function to be called on every group.
   * @param outputMode The output mode of the function.
   * @param timeoutConf Timeout configuration for groups that do not receive data for a while.
   *
   * See [[Encoder]] for more details on what types are encodable to Spark SQL.
   * @since 2.2.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def flatMapGroupsWithState[S: Encoder, U: Encoder](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] = {
    if (outputMode != OutputMode.Append && outputMode != OutputMode.Update) {
      throw new IllegalArgumentException("The output mode of function should be append or update")
    }
    Dataset[U](
      sparkSession,
      FlatMapGroupsWithState[K, V, S, U](
        func.asInstanceOf[(Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any]],
        groupingAttributes,
        dataAttributes,
        outputMode,
        isMapGroupsWithState = false,
        timeoutConf,
        child = logicalPlan))
  }
```

`FlatMapGroupsWithState` is a `LogicalPlan` node.
```scala
/**
 * Applies func to each unique group in `child`, based on the evaluation of `groupingAttributes`,
 * while using state data.
 * Func is invoked with an object representation of the grouping key an iterator containing the
 * object representation of all the rows with that key.
 *
 * @param func function called on each group
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 * @param groupingAttributes used to group the data
 * @param dataAttributes used to read the data
 * @param outputObjAttr used to define the output object
 * @param stateEncoder used to serialize/deserialize state before calling `func`
 * @param outputMode the output mode of `func`
 * @param isMapGroupsWithState whether it is created by the `mapGroupsWithState` method
 * @param timeout used to timeout groups that have not received data in a while
 */
case class FlatMapGroupsWithState(
    func: (Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    outputObjAttr: Attribute,
    stateEncoder: ExpressionEncoder[Any],
    outputMode: OutputMode,
    isMapGroupsWithState: Boolean = false,
    timeout: GroupStateTimeout,
    child: LogicalPlan) extends UnaryNode with ObjectProducer {

  if (isMapGroupsWithState) {
    assert(outputMode == OutputMode.Update)
  }
}
```

For now, the logical plan is created. It has only one node: `FlatMapGroupsWithState`.

In `SparkStrategies.scala` it convert the logical plan node to physical plan(SparkPlan) node.
```scala
  /**
   * Strategy to convert [[FlatMapGroupsWithState]] logical operator to physical operator
   * in streaming plans. Conversion for batch plans is handled by [[BasicOperators]].
   */
  object FlatMapGroupsWithStateStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case FlatMapGroupsWithState(
        func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, stateEnc, outputMode, _,
        timeout, child) =>
        val execPlan = FlatMapGroupsWithStateExec(
          func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, None, stateEnc, outputMode,
          timeout, batchTimestampMs = None, eventTimeWatermark = None, planLater(child))
        execPlan :: Nil
```

In [FlatMapGroupsWithStateExec](FlatMapGroupsWithStateExec.scala), it overrides some specifical fields.
```scala
  /** Distribute by grouping attributes */
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes, stateInfo.map(_.numPartitions)) :: Nil

  /** Ordering needed for using GroupingIterator */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))
```

These two methods will let the Spark create two new nodes:
* Exchange(Shuffle): Exchange node is used to shuffle the data by group key. 
* Sort: Sort node is used for sort the data in one partition by key. After sort all the data with same key will be adjacent and can be processed by the user defined function.

In Spark Streaming, it will use `IncrementalExecution` to generate next micro-batch execution plan. It will copy the StateInfo to next batch.
```scala
  /** Locates save/restore pairs surrounding aggregation. */
  val state = new Rule[SparkPlan] {

    override def apply(plan: SparkPlan): SparkPlan = plan transform {

      case m: FlatMapGroupsWithStateExec =>
        m.copy(
          stateInfo = Some(nextStatefulOperationStateInfo),
          batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
          eventTimeWatermark = Some(offsetSeqMetadata.batchWatermarkMs))
```



