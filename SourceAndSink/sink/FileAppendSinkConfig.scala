import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.NonFatal

/**
 * Serializable config class for FileAppendSink.
  *
  * @param sparkContext  spark context.
 * @param logPath        log file(checkpoint) output path.
 * @param fileSystemType output file system (adl/local).
 * @param expiryTime     expiry time of output files (only support adl).
 *  The pattern of string is like
 *  1.01:01:01  1 day 1 hour 1 minute 1 second
 *  1:01:01 1 hour 1 minutes 1 second
*/
class FileAppendSinkConfig(sparkContext: SparkContext,
  var logPath: String,
  var fileSystemType: String,
  var expiryTime: String) extends Serializable {
  private var hadoopConf :Configuration = null

  if(sparkContext != null){
    hadoopConf = sparkContext.hadoopConfiguration
  }

  /**
   * Get hadoopConf.
   * @return hadoopConf.
   */
  def getHadoopConf(): Configuration = {
    hadoopConf
  }

  /**
   * Get file system type.
   * @return file system type.
   */
  def getFileSystemType(): String = {
    fileSystemType
  }

  /**
   * Get sink log output path.
   * @return sink log output path.
   */
  def getLogPath(): String = {
    logPath
  }

  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    hadoopConf.write(out)
    out.writeObject(logPath)
    out.writeObject(fileSystemType)
    out.writeObject(expiryTime)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    hadoopConf = new Configuration(false)
    hadoopConf.readFields(in)
    logPath = in.readObject().asInstanceOf[String]
    fileSystemType = in.readObject().asInstanceOf[String]
    expiryTime = in.readObject().asInstanceOf[String]
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
