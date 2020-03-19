package org.apache.spark.sql.batchfile

import java.sql.Timestamp

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

case class BatchFilePartition(index: Int) extends Partition

/**
  * RDD of batch file.
  *
  * @param sc sparkContext.
  * @param log batch file log.
  * @param reader batch file reader.
  */
class BatchFileRDD(sc: SparkContext,
                   log: BatchFileLog,
                   reader: BatchFileReader
                  ) extends RDD[BatchFileRecord](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[BatchFileRecord] = {
    val partitionIndex = split.index
    reader.read(log, partitionIndex).map(s => {
      if(log.isSnapshot){
        BatchFileRecord(log.tableName, new Timestamp(0), s)
      }else {
        BatchFileRecord(log.tableName, log.cti, s)
      }})
  }

  override protected def getPartitions: Array[Partition] = {
    val partitionNumber = log.partitionNumber
    (0 until partitionNumber).map(i => new BatchFilePartition(i)).toArray
  }
}
