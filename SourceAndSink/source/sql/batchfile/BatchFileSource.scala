package org.apache.spark.sql.batchfile

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Static constants for BatchFileSource.
  */
object BatchFileSource {
  //schema of the BatchFileSource.
  val SCHEMA = StructType(
    StructField(name = "name", dataType = StringType, false) ::
      StructField(name = "cti", dataType = TimestampType, false) ::
      StructField(name = "value", dataType = StringType, false) ::
      Nil)

  //The number of source offsets that must be retained and made recoverable.
  val MINLOGSTOMAINTAIN = 128L
}

/**
  * Batch stream file datasource extends Source. Called by Spark.
  * Similar with the KafkaSource and FileStreamSource
  *
  * @see <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources" > input source </a>
  * @param sqlContext SQLContext of Spark.
  * @param metadataPath metadata log path.
  * @param reader     BatchFileReader.
  */
class BatchFileSource(
                       sqlContext: SQLContext,
                       metadataPath: String,
                       reader: BatchFileReader)
  extends Source with Logging {

  override def schema: StructType = BatchFileSource.SCHEMA
  private val metadataLog = new BatchFileSourceLog[BatchFileLog](BatchFileSourceLog.VERSION, sqlContext.sparkSession, metadataPath)
  private var metadataLogCurrentOffset = metadataLog.getLatest().map(_._1).getOrElse(-1L)

  override def getOffset: Option[Offset] = {
    if(metadataLogCurrentOffset < 0){
      val logs = reader.getInitLogs()
      if(logs.nonEmpty){
        metadataLogCurrentOffset += 1
        metadataLog.add(metadataLogCurrentOffset, logs)
        return Option(LongOffset(metadataLogCurrentOffset))
      }
      return None

    }else{
      val lastLogs = metadataLog.get(metadataLogCurrentOffset).getOrElse(
        throw new IllegalArgumentException("Can't get BatchFileSource metadata logs.")
      )

      val logs = reader.next(lastLogs)

      if(!logs.sameElements(lastLogs)){
        metadataLogCurrentOffset += 1
        metadataLog.add(metadataLogCurrentOffset, logs)
      }

      return Option(LongOffset(metadataLogCurrentOffset))
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startIndex = start.getOrElse(LongOffset(-1L)).json().toLong + 1
    val endIndex = end.json().toLong
    var logs = List[BatchFileLog]()

    for(index <- startIndex to endIndex){
      logs ++= metadataLog.get(index).getOrElse(
        throw new IllegalArgumentException(s"Can't get BatchFileSource metatdata $index")
      )
    }

    val rdds = logs
      .filter(log=>log.isReady)
      .map(log => {
      new BatchFileRDD(sqlContext.sparkContext, log, reader)
    })

    val rdd = sqlContext.sparkContext.union(rdds)
      .map(row => InternalRow(UTF8String.fromString(row.tableName), row.cti.getTime * 1000, UTF8String.fromString(row.value)))
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = {
    metadataLog.purge(end.json().toLong - BatchFileSource.MINLOGSTOMAINTAIN)
  }

  override def stop(): Unit = {}
}

