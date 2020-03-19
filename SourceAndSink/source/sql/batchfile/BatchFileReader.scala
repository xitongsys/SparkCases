package org.apache.spark.sql.batchfile

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
  * Interface of the BatchFileReader.
  */
trait BatchFileReader
{
  /**
    * Read rows.
    * @param log batch file log.
    * @param partition partition index.
    * @return Iterator of rows.
    */
  def read(log: BatchFileLog, partition: Int): Iterator[String]

  /**
    * Get initial logs.
    * @return initial logs.
    */
  def getInitLogs(): Array[BatchFileLog]

  /**
    * Get the next log.
    * @param log the previous log.
    * @return the next log.
    */
  def next(log: BatchFileLog): BatchFileLog

  /**
    * Get the next logs.
    * @param logs the previous logs.
    * @return the next logs.
    */
  def next(logs: Array[BatchFileLog]): Array[BatchFileLog]

  /**
    * Commit the log.
    * @param log the log processed.
    */
  def commit(log: BatchFileLog): Unit

  /**
    * Return short name of the class.
    * @return short name of the class.
    */
  def shortName(): String

  /**
    * Set user defined parameters.
    * @param parameters user defined parameters.
    * @throws java.lang.Exception
    */
  @throws(classOf[java.lang.Exception])
  def setParameters(parameters: CaseInsensitiveMap[String])
}
