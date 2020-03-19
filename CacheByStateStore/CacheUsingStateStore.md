# Cache by StateStore
In my usage, I implements a cache by Spark StateStore, which leveage the StateStore directly instead of using Spark method(like flatMapGroupWithState).

## Cache Codes
```scala
/**
  * Copyright (c) Microsoft. All rights reserved.
  *
  * Cache implemented by StateStore.
  */

package com.microsoft.ads.bi.cache

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.ObjectOperator
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
  * Cache key.
  * @param key key to store.
  */
case class CacheKey(key: Any)

/**
  * Cache value.
  * @param value value to store.
  * @param timeout timeout.
  */
case class CacheValue(value: Any, timeout: Timestamp)

/**
  * Cache implemented by StateStore.
  * @param conf Cache config.
  */
class Cache(var conf: CacheConfig) extends ICache {
  val partitionId: Int = TaskContext.getPartitionId
  val batchId: Int = TaskContext.get.getLocalProperty("streaming.sql.batchId").toInt
  val storeId = new StateStoreId(conf.path, conf.operatiorId, partitionId, conf.name)
  val providerId = new StateStoreProviderId(storeId, UUID.randomUUID)
  val storeConf = conf.storeConf

  val keyEncoder = encoderFor[CacheKey](Encoders.kryo[CacheKey])
  val valueEncoder = encoderFor[CacheValue](Encoders.kryo[CacheValue])
  val keySerializer = keyEncoder.serializer
  val keyDeserializer = keyEncoder.resolveAndBind().deserializer
  val valueSerializer = valueEncoder.serializer
  val valueDeserializer = valueEncoder.resolveAndBind().deserializer

  private var store: StateStore = StateStore.get(
    providerId,
    keyEncoder.schema,
    valueEncoder.schema,
    None,
    batchId,
    storeConf,
    conf.hadoopConf)

  /**
    * Put key-value to cache.
    * @param key key to put.
    * @param value value to put.
    * @throws
    */
  @throws[Exception]
  override def put(key: Any, value: Any): Unit = {
    val keyRow = keyToRow(key)
    val valueRow = valueToRow(value, new Timestamp(0))
    store.put(keyRow, valueRow)
  }

  /**
    * Get value of key from cache.
    * @param key key to get.
    * @throws
    * @return value of the key.
    */
  @throws[Exception]
  override def get(key: Any): Any = {
    val keyRow = keyToRow(key)
    val valueRow = store.get(keyRow)
    if(valueRow == null) {
      null
    }else{
      rowToValue(valueRow)
    }
  }

  /**
    * Remove key from cache.
    * @param key key to remove.
    * @throws
    */
  @throws[Exception]
  override def remove(key: Any): Unit = {
    store.remove(keyToRow(key))
  }

  /**
    * Commit changes.
    * @throws
    */
  @throws[Exception]
  override def commit(): Unit = {
    store.commit
  }

  /**
    * Abort changes.
    * @throws
    */
  @throws[Exception]
  override def abort(): Unit = {
    store.abort()
  }

  @throws[Exception]
  private def keyToRow(keyObject: Any): UnsafeRow =
    ObjectOperator.serializeObjectToRow(keySerializer)(CacheKey(keyObject))

  @throws[Exception]
  private def valueToRow(valueObject: Any, timeout: Timestamp): UnsafeRow =
    ObjectOperator.serializeObjectToRow(valueSerializer)(CacheValue(valueObject, timeout))

  @throws[Exception]
  private def rowToKey(keyRow: UnsafeRow): Any = {
    val cacheKey = ObjectOperator.deserializeRowToObject(keyDeserializer)(keyRow)
      .asInstanceOf[CacheKey]
    cacheKey.key
  }

  @throws[Exception]
  private def rowToValue(valueRow: UnsafeRow): Any = {
    val cacheValue = ObjectOperator.deserializeRowToObject(valueDeserializer)(valueRow)
      .asInstanceOf[CacheValue]
    cacheValue.value
  }
}

```

## Cache Config Codes
```scala
/**
  * Copyright (c) Microsoft. All rights reserved.
  *
  * Cache Configuration.
  */

package com.microsoft.ads.bi.cache

import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable

import com.microsoft.ads.bi.util.UetConstants
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.execution.streaming.state.StateStoreConf
import org.apache.spark.sql.SparkSession
import scala.util.control.NonFatal

/**
  * Cache configuration.
  * @param sparkSession sparkSession.
  * @param operatiorId operator id. It's better to choose enough big to avoid conflicts with system value.
  * @param name name of the cache.
  * @param path path to store cache files.
  */
class CacheConfig(sparkSession: SparkSession,
                  var operatiorId: Long,
                  var name: String,
                  var path: String) extends Serializable {
  var hadoopConf: Configuration = null
  var storeConf: StateStoreConf = null

  if(sparkSession != null){
    var sparkContext = sparkSession.sparkContext
    hadoopConf = sparkContext.hadoopConfiguration
    storeConf = new StateStoreConf(sparkSession.sessionState.conf)
  }

  private def readObject(input: ObjectInputStream): Unit = tryOrIOException {
    hadoopConf = new Configuration(false)
    hadoopConf.readFields(input)
    storeConf = input.readObject().asInstanceOf[StateStoreConf]
    operatiorId = input.readObject().asInstanceOf[Long]
    name = input.readObject().asInstanceOf[String]
    path = input.readObject().asInstanceOf[String]
  }

  private def writeObject(output: ObjectOutputStream): Unit = tryOrIOException {
    hadoopConf.write(output)
    output.writeObject(storeConf)
    output.writeObject(operatiorId)
    output.writeObject(name)
    output.writeObject(path)
  }

  /**
    * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
    * exceptions as IOException. This is used when implementing Externalizable and Serializable's
    * read and write methods, since Java's serializer will not report non-IOExceptions properly;
    * see SPARK-4080 for more context.
    */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        throw e
      case NonFatal(e) =>
        throw new IOException(e)
    }
  }
}

```