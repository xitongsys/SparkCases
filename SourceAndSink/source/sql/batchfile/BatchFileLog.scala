package org.apache.spark.sql.batchfile

import java.sql.Timestamp

/**
  * BatchFileLog class.
  * @param tableName table name.
  * @param cti cti of this log.
  * @param partitionNumber partition number.
  * @param partitionId partition id.
  * @param isSnapshot if is from snapshot.
  * @param isReady is this log ready.
  */
case class BatchFileLog(tableName: String, cti: Timestamp, partitionNumber: Int, partitionId: Int, isSnapshot: Boolean, isReady: Boolean);
