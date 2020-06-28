# Spark StateStore

## StateStore Files
**StateStore.scala**

**StateStoreConf.scala**

**StateStoreCoordinator.scala**

**StateStoreRDD.scala**

**HDFSBackedStateStoreProvider.scala**

## Mechanism
1. StateStoreCoordinator runs on driver and create an RPC endpoint for executors.

2. Executors has a StageStore which use a HashMap to store the Key-Value.

3. Executors use the RPC endpoint to report its StateStore status(StateStoreProviderId, host, executorId).

4. Executors write a delta file of state change every micro-batch.

5. Executors use a maintanced thread to write snapshots of state(HashMap) to consisted storeage periodically.

6. State data is used as StateStoreRDD, which get the location from the StateStoreCoordinator.


## Some config classes
**StateStoreId**
```scala
/**
 * Unique identifier for a bunch of keyed state data.
 * @param checkpointRootLocation Root directory where all the state data of a query is stored
 * @param operatorId Unique id of a stateful operator
 * @param partitionId Index of the partition of an operators state data
 * @param storeName Optional, name of the store. Each partition can optionally use multiple state
 *                  stores, but they have to be identified by distinct names.
 */
case class StateStoreId(
    checkpointRootLocation: String,
    operatorId: Long,
    partitionId: Int,
    storeName: String = StateStoreId.DEFAULT_STORE_NAME) {

  /**
   * Checkpoint directory to be used by a single state store, identified uniquely by the tuple
   * (operatorId, partitionId, storeName). All implementations of [[StateStoreProvider]] should
   * use this path for saving state data, as this ensures that distinct stores will write to
   * different locations.
   */
  def storeCheckpointLocation(): Path = {
    if (storeName == StateStoreId.DEFAULT_STORE_NAME) {
      // For reading state store data that was generated before store names were used (Spark <= 2.2)
      new Path(checkpointRootLocation, s"$operatorId/$partitionId")
    } else {
      new Path(checkpointRootLocation, s"$operatorId/$partitionId/$storeName")
    }
  }
}
```

**StateStoreProviderId**
```scala
/**
 * Unique identifier for a provider, used to identify when providers can be reused.
 * Note that `queryRunId` is used uniquely identify a provider, so that the same provider
 * instance is not reused across query restarts.
 */
case class StateStoreProviderId(storeId: StateStoreId, queryRunId: UUID)
```

**StateStoreConf**
```scala
/** A class that contains configuration parameters for [[StateStore]]s. */
class StateStoreConf(@transient private val sqlConf: SQLConf)
  extends Serializable {

  def this() = this(new SQLConf)

  /**
   * Minimum number of delta files in a chain after which HDFSBackedStateStore will
   * consider generating a snapshot.
   */
  val minDeltasForSnapshot: Int = sqlConf.stateStoreMinDeltasForSnapshot

  /** Minimum versions a State Store implementation should retain to allow rollbacks */
  val minVersionsToRetain: Int = sqlConf.minBatchesToRetain

  /** Maximum count of versions a State Store implementation should retain in memory */
  val maxVersionsToRetainInMemory: Int = sqlConf.maxBatchesToRetainInMemory

  /**
   * Optional fully qualified name of the subclass of [[StateStoreProvider]]
   * managing state data. That is, the implementation of the State Store to use.
   */
  val providerClass: String = sqlConf.stateStoreProviderClass

  /**
   * Additional configurations related to state store. This will capture all configs in
   * SQLConf that start with `spark.sql.streaming.stateStore.` */
  val confs: Map[String, String] =
    sqlConf.getAllConfs.filter(_._1.startsWith("spark.sql.streaming.stateStore."))
}

object StateStoreConf {
  val empty = new StateStoreConf()

  def apply(conf: SQLConf): StateStoreConf = new StateStoreConf(conf)
}
```



## StateStoreCoordinator
```scala
/**
 * Class for coordinating instances of [[StateStore]]s loaded in executors across the cluster,
 * and get their locations for job scheduling.
 */
private class StateStoreCoordinator(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {
```


It runs on driver and will create an RPC endpoint for executors.

```scala
/** Helper object used to create reference to [[StateStoreCoordinator]]. */
object StateStoreCoordinatorRef extends Logging {

  private val endpointName = "StateStoreCoordinator"

  /**
   * Create a reference to a [[StateStoreCoordinator]]
   */
  def forDriver(env: SparkEnv): StateStoreCoordinatorRef = synchronized {
    try {
      val coordinator = new StateStoreCoordinator(env.rpcEnv)
      val coordinatorRef = env.rpcEnv.setupEndpoint(endpointName, coordinator)
      logInfo("Registered StateStoreCoordinator endpoint")
      new StateStoreCoordinatorRef(coordinatorRef)
    } catch {
      case e: IllegalArgumentException =>
        val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName, env.conf, env.rpcEnv)
        logDebug("Retrieved existing StateStoreCoordinator endpoint")
        new StateStoreCoordinatorRef(rpcEndpointRef)
    }
  }

  def forExecutor(env: SparkEnv): StateStoreCoordinatorRef = synchronized {
    val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName, env.conf, env.rpcEnv)
    logDebug("Retrieved existing StateStoreCoordinator endpoint")
    new StateStoreCoordinatorRef(rpcEndpointRef)
  }
}
```

Executors use the endpoint to report its status

```scala
  private[sql] def reportActiveInstance(
      stateStoreProviderId: StateStoreProviderId,
      host: String,
      executorId: String): Unit = {
    rpcEndpointRef.send(ReportActiveInstance(stateStoreProviderId, host, executorId))
  }
```

StateStoreRDD use the endpoint to get the RDD location info:

***StateStoreCoordinator.scala**
```scala
  /** Get the location of the state store */
  private[sql] def getLocation(stateStoreProviderId: StateStoreProviderId): Option[String] = {
    rpcEndpointRef.askSync[Option[String]](GetLocation(stateStoreProviderId))
  }
```

**StateStoreRDD.scala**
```scala
  /**
   * Set the preferred location of each partition using the executor that has the related
   * [[StateStoreProvider]] already loaded.
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val stateStoreProviderId = StateStoreProviderId(
      StateStoreId(checkpointLocation, operatorId, partition.index),
      queryRunId)
    storeCoordinator.flatMap(_.getLocation(stateStoreProviderId)).toSeq
```

## StateStoreProvider
```scala
/**
 * Trait representing a provider that provide [[StateStore]] instances representing
 * versions of state data.
 *
 * The life cycle of a provider and its provide stores are as follows.
 *
 * - A StateStoreProvider is created in a executor for each unique [[StateStoreId]] when
 *   the first batch of a streaming query is executed on the executor. All subsequent batches reuse
 *   this provider instance until the query is stopped.
 *
 * - Every batch of streaming data request a specific version of the state data by invoking
 *   `getStore(version)` which returns an instance of [[StateStore]] through which the required
 *   version of the data can be accessed. It is the responsible of the provider to populate
 *   this store with context information like the schema of keys and values, etc.
 *
 * - After the streaming query is stopped, the created provider instances are lazily disposed off.
 */
trait StateStoreProvider {

  /**
   * Initialize the provide with more contextual information from the SQL operator.
   * This method will be called first after creating an instance of the StateStoreProvider by
   * reflection.
   *
   * @param stateStoreId Id of the versioned StateStores that this provider will generate
   * @param keySchema Schema of keys to be stored
   * @param valueSchema Schema of value to be stored
   * @param keyIndexOrdinal Optional column (represent as the ordinal of the field in keySchema) by
   *                        which the StateStore implementation could index the data.
   * @param storeConfs Configurations used by the StateStores
   * @param hadoopConf Hadoop configuration that could be used by StateStore to save state data
   */
  def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyIndexOrdinal: Option[Int], // for sorting the data by their keys
      storeConfs: StateStoreConf,
      hadoopConf: Configuration): Unit

  /**
   * Return the id of the StateStores this provider will generate.
   * Should be the same as the one passed in init().
   */
  def stateStoreId: StateStoreId

  /** Called when the provider instance is unloaded from the executor */
  def close(): Unit

  /** Return an instance of [[StateStore]] representing state data of the given version */
  def getStore(version: Long): StateStore

  /** Optional method for providers to allow for background maintenance (e.g. compactions) */
  def doMaintenance(): Unit = { }

  /**
   * Optional custom metrics that the implementation may want to report.
   * @note The StateStore objects created by this provider must report the same custom metrics
   * (specifically, same names) through `StateStore.metrics`.
   */
  def supportedCustomMetrics: Seq[StateStoreCustomMetric] = Nil
}

```



## StateStore
```scala
package org.apache.spark.sql.execution.streaming.state

/**
 * Base trait for a versioned key-value store. Each instance of a `StateStore` represents a specific
 * version of state data, and such instances are created through a [[StateStoreProvider]].
 */
trait StateStore {

  /** Unique identifier of the store */
  def id: StateStoreId

  /** Version of the data in this store before committing updates. */
  def version: Long

  /**
   * Get the current value of a non-null key.
   * @return a non-null row if the key exists in the store, otherwise null.
   */
  def get(key: UnsafeRow): UnsafeRow

  /**
   * Put a new value for a non-null key. Implementations must be aware that the UnsafeRows in
   * the params can be reused, and must make copies of the data as needed for persistence.
   */
  def put(key: UnsafeRow, value: UnsafeRow): Unit

  /**
   * Remove a single non-null key.
   */
  def remove(key: UnsafeRow): Unit

  /**
   * Get key value pairs with optional approximate `start` and `end` extents.
   * If the State Store implementation maintains indices for the data based on the optional
   * `keyIndexOrdinal` over fields `keySchema` (see `StateStoreProvider.init()`), then it can use
   * `start` and `end` to make a best-effort scan over the data. Default implementation returns
   * the full data scan iterator, which is correct but inefficient. Custom implementations must
   * ensure that updates (puts, removes) can be made while iterating over this iterator.
   *
   * @param start UnsafeRow having the `keyIndexOrdinal` column set with appropriate starting value.
   * @param end UnsafeRow having the `keyIndexOrdinal` column set with appropriate ending value.
   * @return An iterator of key-value pairs that is guaranteed not miss any key between start and
   *         end, both inclusive.
   */
  def getRange(start: Option[UnsafeRow], end: Option[UnsafeRow]): Iterator[UnsafeRowPair] = {
    iterator()
  }

  /**
   * Commit all the updates that have been made to the store, and return the new version.
   * Implementations should ensure that no more updates (puts, removes) can be after a commit in
   * order to avoid incorrect usage.
   */
  def commit(): Long

  /**
   * Abort all the updates that have been made to the store. Implementations should ensure that
   * no more updates (puts, removes) can be after an abort in order to avoid incorrect usage.
   */
  def abort(): Unit

  /**
   * Return an iterator containing all the key-value pairs in the StateStore. Implementations must
   * ensure that updates (puts, removes) can be made while iterating over this iterator.
   */
  def iterator(): Iterator[UnsafeRowPair]

  /** Current metrics of the state store */
  def metrics: StateStoreMetrics

  /**
   * Whether all updates have been committed
   */
  def hasCommitted: Boolean
}

```


For now, it only implements the ```HDFSBackedStateStoreProvider```. 

StateStore is a ```type MapType = java.util.concurrent.ConcurrentHashMap[UnsafeRow, UnsafeRow]```

In ```StateStore.scala```, it has a global variable HashMap to store different StateStore
```scala
/**
 * Companion object to [[StateStore]] that provides helper methods to create and retrieve stores
 * by their unique ids. In addition, when a SparkContext is active (i.e. SparkEnv.get is not null),
 * it also runs a periodic background task to do maintenance on the loaded stores. For each
 * store, it uses the [[StateStoreCoordinator]] to ensure whether the current loaded instance of
 * the store is the active instance. Accordingly, it either keeps it loaded and performs
 * maintenance, or unloads the store.
 */
object StateStore extends Logging {

  val MAINTENANCE_INTERVAL_CONFIG = "spark.sql.streaming.stateStore.maintenanceInterval"
  val MAINTENANCE_INTERVAL_DEFAULT_SECS = 60

  @GuardedBy("loadedProviders")
  private val loadedProviders = new mutable.HashMap[StateStoreProviderId, StateStoreProvider]()

```

## HDFSBackedStateStoreProvider

You can use ```StateStore.get()``` to get a version of store.
```scala
  /** Get or create a store associated with the id. */
  def get(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      indexOrdinal: Option[Int],
      version: Long,
      storeConf: StateStoreConf,
      hadoopConf: Configuration): StateStore = {
    require(version >= 0)
    val storeProvider = loadedProviders.synchronized {
      startMaintenanceIfNeeded()
      val provider = loadedProviders.getOrElseUpdate(
        storeProviderId,
        StateStoreProvider.createAndInit(
          storeProviderId.storeId, keySchema, valueSchema, indexOrdinal, storeConf, hadoopConf)
      )
      reportActiveStoreInstance(storeProviderId)
      provider
    }
    storeProvider.getStore(version)
  }
```

In this function, if your store to get is in the Map, it just return. Or it will load it from the StateStore checkpoint location. 

The StateLocation is defined as 

```scala
/**
   * Checkpoint directory to be used by a single state store, identified uniquely by the tuple
   * (operatorId, partitionId, storeName). All implementations of [[StateStoreProvider]] should
   * use this path for saving state data, as this ensures that distinct stores will write to
   * different locations.
   */
  def storeCheckpointLocation(): Path = {
    if (storeName == StateStoreId.DEFAULT_STORE_NAME) {
      // For reading state store data that was generated before store names were used (Spark <= 2.2)
      new Path(checkpointRootLocation, s"$operatorId/$partitionId")
    } else {
      new Path(checkpointRootLocation, s"$operatorId/$partitionId/$storeName")
    }
  }
}
```
In Spark it will asign an unique operatorId for every state operator.

Loading files is: version.delta/(version-1).delta/.../(version-x).snapshot, where (version-x) is the latest snapshot file.

When the executor runs, it will start a maintence thread to write snapshot files
```scala
  /** Do maintenance backing data files, including creating snapshots and cleaning up old files */
  override def doMaintenance(): Unit = {
    try {
      doSnapshot()
      cleanup()
    } catch {
      case NonFatal(e) =>
        logWarning(s"Error performing snapshot and cleaning up $this")
    }
  }
```


