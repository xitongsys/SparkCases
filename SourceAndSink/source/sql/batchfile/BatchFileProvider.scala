package org.apache.spark.sql.batchfile

import java.util.ServiceLoader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import scala.collection.JavaConverters._

/**
  * DataSource provider of batch file. Called by Spark.
  */
class BatchFileProvider extends StreamSourceProvider with DataSourceRegister with Logging
{
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String,String]):(String, StructType) =
  {

    ("batchFile", BatchFileSource.SCHEMA)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String,String]):Source =
  {

    val parametersMap = CaseInsensitiveMap[String](parameters);
    val readerClassName = parametersMap.get("reader").get

    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[BatchFileReader], loader)
    val reader = serviceLoader.asScala.filter(_.shortName().equalsIgnoreCase(readerClassName)).toList match
    {
      case header :: Nil => header
      case _ => throw new ClassNotFoundException(s"Failed to find $readerClassName")
    }
    reader.setParameters(parametersMap)
    new BatchFileSource(sqlContext, metadataPath, reader)
  }

  override def shortName(): String = "batchfile"
}
