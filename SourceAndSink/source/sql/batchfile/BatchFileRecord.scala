package org.apache.spark.sql.batchfile

import java.sql.Timestamp

/**
  * Record of the BatchFile. One record is one row in the BatchFileRDD.
  *
  * @param tableName reference table name.
  * @param cti batch cti.
  * @param value one line(String) of the reference table.
  */
case class BatchFileRecord(tableName: String, cti: Timestamp, value: String)