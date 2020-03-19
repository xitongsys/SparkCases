package org.apache.spark.sql.batchfile

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.sql.execution.streaming.HDFSMetadataLog
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Serialization
import org.json4s.NoTypeHints
import scala.io.{Source => IOSource}
import scala.reflect.ClassTag

/**
  * BatchFileSourceLog implements metalog for batch file source.
  * @param metadataLogVersion log version and default is 1.
  * @param sparkSession spark session.
  * @param path metadata path.
  * @tparam T log type.
  */
class BatchFileSourceLog[T <: AnyRef : ClassTag](metadataLogVersion: Int, sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[Array[T]](sparkSession, path){

  private implicit val formats = Serialization.formats(NoTypeHints)
  private implicit val manifest = Manifest.classType[T](implicitly[ClassTag[T]].runtimeClass)

  override def serialize(logData: Array[T], out: OutputStream): Unit = {
    out.write(("v" + metadataLogVersion).getBytes(UTF_8))
    logData.foreach{ data =>
      out.write('\n')
      out.write(Serialization.write[T](data).getBytes(UTF_8))
    }
  }

  override def deserialize(in: InputStream): Array[T] = {
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file")
    }

    val version = parseVersion(lines.next(), metadataLogVersion)
    lines.map(Serialization.read[T]).toArray
  }
}

object BatchFileSourceLog {
  var VERSION = 1
}
