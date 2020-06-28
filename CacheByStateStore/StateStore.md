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

**StateStoreCoordinator.scala**
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


In ```StateStore.scala```, it has a global variable HashMap to store different StateStoreProvider
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
You can use ```StateStore.get()``` to get a version of store provider.
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


## HDFSBackedStateStoreProvider
It uses a TreeMap to store different version(micro-batch) of state and use a ConcurrentHashMap to store real Key-Value of state.
```loadMaps``` is a memory cache to avoid reading state file from HDFS every time.

```scala
  type MapType = java.util.concurrent.ConcurrentHashMap[UnsafeRow, UnsafeRow]

  private lazy val loadedMaps = new util.TreeMap[Long, MapType](Ordering[Long].reverse)

```

Get StateStore of required version. 

```scala
  /** Get the state store for making updates to create a new `version` of the store. */
  override def getStore(version: Long): StateStore = synchronized {
    require(version >= 0, "Version cannot be less than 0")
    val newMap = new MapType()
    if (version > 0) {
      newMap.putAll(loadMap(version))
    }
    val store = new HDFSBackedStateStore(version, newMap)
    logInfo(s"Retrieved version $version of ${HDFSBackedStateStoreProvider.this} for update")
    store
  }
```

```loadMap``` will check the cache (```loadedMaps```) firstly. If it is in cache, just return it. If not, It will load the latest Snapshot file and the delta files after the Snapshot and before the version.

```scala
  /** Load the required version of the map data from the backing files */
  private def loadMap(version: Long): MapType = {

    // Shortcut if the map for this version is already there to avoid a redundant put.
    val loadedCurrentVersionMap = synchronized { Option(loadedMaps.get(version)) }
    if (loadedCurrentVersionMap.isDefined) {
      loadedMapCacheHitCount.increment()
      return loadedCurrentVersionMap.get
    }

    logWarning(s"The state for version $version doesn't exist in loadedMaps. " +
      "Reading snapshot file and delta files if needed..." +
      "Note that this is normal for the first batch of starting query.")

    loadedMapCacheMissCount.increment()

    val (result, elapsedMs) = Utils.timeTakenMs {
      val snapshotCurrentVersionMap = readSnapshotFile(version)
      if (snapshotCurrentVersionMap.isDefined) {
        synchronized { putStateIntoStateCacheMap(version, snapshotCurrentVersionMap.get) }
        return snapshotCurrentVersionMap.get
      }

      // Find the most recent map before this version that we can.
      // [SPARK-22305] This must be done iteratively to avoid stack overflow.
      var lastAvailableVersion = version
      var lastAvailableMap: Option[MapType] = None
      while (lastAvailableMap.isEmpty) {
        lastAvailableVersion -= 1

        if (lastAvailableVersion <= 0) {
          // Use an empty map for versions 0 or less.
          lastAvailableMap = Some(new MapType)
        } else {
          lastAvailableMap =
            synchronized { Option(loadedMaps.get(lastAvailableVersion)) }
              .orElse(readSnapshotFile(lastAvailableVersion))
        }
      }

      // Load all the deltas from the version after the last available one up to the target version.
      // The last available version is the one with a full snapshot, so it doesn't need deltas.
      val resultMap = new MapType(lastAvailableMap.get)
      for (deltaVersion <- lastAvailableVersion + 1 to version) {
        updateFromDeltaFile(deltaVersion, resultMap)
      }

      synchronized { putStateIntoStateCacheMap(version, resultMap) }
      resultMap
    }

    logDebug(s"Loading state for $version takes $elapsedMs ms.")

    result
  }
```

## HDFSBackedStateStore

Get/Put/Remove key-value and write to delta file.

```scala
  /** Implementation of [[StateStore]] API which is backed by a HDFS-compatible file system */
  class HDFSBackedStateStore(val version: Long, mapToUpdate: MapType)
    extends StateStore {

    /** Trait and classes representing the internal state of the store */
    trait STATE
    case object UPDATING extends STATE
    case object COMMITTED extends STATE
    case object ABORTED extends STATE

    private val newVersion = version + 1
    @volatile private var state: STATE = UPDATING
    private val finalDeltaFile: Path = deltaFile(newVersion)
    private lazy val deltaFileStream = fm.createAtomic(finalDeltaFile, overwriteIfPossible = true)
    private lazy val compressedStream = compressStream(deltaFileStream)

    override def id: StateStoreId = HDFSBackedStateStoreProvider.this.stateStoreId

    override def get(key: UnsafeRow): UnsafeRow = {
      mapToUpdate.get(key)
    }

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      val keyCopy = key.copy()
      val valueCopy = value.copy()
      mapToUpdate.put(keyCopy, valueCopy)
      writeUpdateToDeltaFile(compressedStream, keyCopy, valueCopy)
    }

    override def remove(key: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      val prevValue = mapToUpdate.remove(key)
      if (prevValue != null) {
        writeRemoveToDeltaFile(compressedStream, key)
      }
    }

    override def getRange(
        start: Option[UnsafeRow],
        end: Option[UnsafeRow]): Iterator[UnsafeRowPair] = {
      verify(state == UPDATING, "Cannot getRange after already committed or aborted")
      iterator()
    }

    /** Commit all the updates that have been made to the store, and return the new version. */
    override def commit(): Long = {
      verify(state == UPDATING, "Cannot commit after already committed or aborted")

      try {
        commitUpdates(newVersion, mapToUpdate, compressedStream)
        state = COMMITTED
        logInfo(s"Committed version $newVersion for $this to file $finalDeltaFile")
        newVersion
      } catch {
        case NonFatal(e) =>
          throw new IllegalStateException(
            s"Error committing version $newVersion into $this", e)
      }
    }

    /** Abort all the updates made on this store. This store will not be usable any more. */
    override def abort(): Unit = {
      // This if statement is to ensure that files are deleted only if there are changes to the
      // StateStore. We have two StateStores for each task, one which is used only for reading, and
      // the other used for read+write. We don't want the read-only to delete state files.
      if (state == UPDATING) {
        state = ABORTED
        cancelDeltaFile(compressedStream, deltaFileStream)
      } else {
        state = ABORTED
      }
      logInfo(s"Aborted version $newVersion for $this")
    }

    /**
     * Get an iterator of all the store data.
     * This can be called only after committing all the updates made in the current thread.
     */
    override def iterator(): Iterator[UnsafeRowPair] = {
      val unsafeRowPair = new UnsafeRowPair()
      mapToUpdate.entrySet.asScala.iterator.map { entry =>
        unsafeRowPair.withRows(entry.getKey, entry.getValue)
      }
    }

    override def metrics: StateStoreMetrics = {
      // NOTE: we provide estimation of cache size as "memoryUsedBytes", and size of state for
      // current version as "stateOnCurrentVersionSizeBytes"
      val metricsFromProvider: Map[String, Long] = getMetricsForProvider()

      val customMetrics = metricsFromProvider.flatMap { case (name, value) =>
        // just allow searching from list cause the list is small enough
        supportedCustomMetrics.find(_.name == name).map(_ -> value)
      } + (metricStateOnCurrentVersionSizeBytes -> SizeEstimator.estimate(mapToUpdate))

      StateStoreMetrics(mapToUpdate.size(), metricsFromProvider("memoryUsedBytes"), customMetrics)
    }

    /**
     * Whether all updates have been committed
     */
    override def hasCommitted: Boolean = {
      state == COMMITTED
    }

    override def toString(): String = {
      s"HDFSStateStore[id=(op=${id.operatorId},part=${id.partitionId}),dir=$baseDir]"
    }
  }
```


Delta file format:

```scala
 private def writeUpdateToDeltaFile(
      output: DataOutputStream,
      key: UnsafeRow,
      value: UnsafeRow): Unit = {
    val keyBytes = key.getBytes()
    val valueBytes = value.getBytes()
    output.writeInt(keyBytes.size)
    output.write(keyBytes)
    output.writeInt(valueBytes.size)
    output.write(valueBytes)
  }

  private def writeRemoveToDeltaFile(output: DataOutputStream, key: UnsafeRow): Unit = {
    val keyBytes = key.getBytes()
    output.writeInt(keyBytes.size)
    output.write(keyBytes)
    output.writeInt(-1)
  }

  private def finalizeDeltaFile(output: DataOutputStream): Unit = {
    output.writeInt(-1)  // Write this magic number to signify end of file
    output.close()
  }

  private def updateFromDeltaFile(version: Long, map: MapType): Unit = {
    val fileToRead = deltaFile(version)
    var input: DataInputStream = null
    val sourceStream = try {
      fm.open(fileToRead)
    } catch {
      case f: FileNotFoundException =>
        throw new IllegalStateException(
          s"Error reading delta file $fileToRead of $this: $fileToRead does not exist", f)
    }
    try {
      input = decompressStream(sourceStream)
      var eof = false

      while(!eof) {
        val keySize = input.readInt()
        if (keySize == -1) {
          eof = true
        } else if (keySize < 0) {
          throw new IOException(
            s"Error reading delta file $fileToRead of $this: key size cannot be $keySize")
        } else {
          val keyRowBuffer = new Array[Byte](keySize)
          ByteStreams.readFully(input, keyRowBuffer, 0, keySize)

          val keyRow = new UnsafeRow(keySchema.fields.length)
          keyRow.pointTo(keyRowBuffer, keySize)

          val valueSize = input.readInt()
          if (valueSize < 0) {
            map.remove(keyRow)
          } else {
            val valueRowBuffer = new Array[Byte](valueSize)
            ByteStreams.readFully(input, valueRowBuffer, 0, valueSize)
            val valueRow = new UnsafeRow(valueSchema.fields.length)
            // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
            // This is a workaround for the following:
            // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
            // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
            valueRow.pointTo(valueRowBuffer, (valueSize / 8) * 8)
            map.put(keyRow, valueRow)
          }
        }
      }
    } finally {
      if (input != null) input.close()
    }
    logInfo(s"Read delta file for version $version of $this from $fileToRead")
  }

  private def writeSnapshotFile(version: Long, map: MapType): Unit = {
    val targetFile = snapshotFile(version)
    var rawOutput: CancellableFSDataOutputStream = null
    var output: DataOutputStream = null
    try {
      rawOutput = fm.createAtomic(targetFile, overwriteIfPossible = true)
      output = compressStream(rawOutput)
      val iter = map.entrySet().iterator()
      while(iter.hasNext) {
        val entry = iter.next()
        val keyBytes = entry.getKey.getBytes()
        val valueBytes = entry.getValue.getBytes()
        output.writeInt(keyBytes.size)
        output.write(keyBytes)
        output.writeInt(valueBytes.size)
        output.write(valueBytes)
      }
      output.writeInt(-1)
      output.close()
    } catch {
      case e: Throwable =>
        cancelDeltaFile(compressedStream = output, rawStream = rawOutput)
        throw e
    }
    logInfo(s"Written snapshot file for version $version of $this at $targetFile")
  }

```







