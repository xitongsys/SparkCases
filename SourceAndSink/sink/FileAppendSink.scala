package sink

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}

import ADLSFileSystem, FileSystem, LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.ForeachWriter
import scala.collection.JavaConverters._
import scala.collection.mutable

class FileAppendSink[T](
  conf: FileAppendSinkConfig,
  processor: IFileAppendSinkProcessor[T]) extends ForeachWriter[T]{

  private val DELTASUFFIX = "file-append-sink-delta"
  private var retry = false
  private var partitionId: Long = -1
  private var version: Long = -1
  private var sinkLog: FileAppendSinkLog = null
  private var fs: FileSystem = null

  private val buffer = new mutable.HashMap[String, OutputStream]()

  override def open(partitionId: Long, version: Long): Boolean = {
    this.partitionId = partitionId
    this.version = version
    conf.fileSystemType.toLowerCase() match {
      case "adl" => {
        val hadoopConf = conf.getHadoopConf()
        fs = new ADLSFileSystem("default",
          hadoopConf.get(UetConstants.DFS_ADLS_OAUTH2_REFRESH_URL),
          hadoopConf.get(UetConstants.DFS_ADLS_OAUTH2_CLIENT_ID),
          hadoopConf.get(UetConstants.DFS_ADLS_OAUTH2_CREDENTIAL))
      }
      case "local" => {
        fs = new LocalFileSystem
      }
      case _ => {
        throw new IllegalArgumentException(s"Unsupported file system type ${conf.fileSystemType}")
      }
    }

    this.sinkLog = new FileAppendSinkLog(conf.getHadoopConf(), partitionId, conf.logPath)
    val doneFlag = sinkLog.getDoneFlag(partitionId, version)
    val startFlag = sinkLog.getStartFlag(partitionId, version)
    retry = startFlag && !doneFlag
    if(doneFlag){
      false
    }else{
      sinkLog.addStartFlag(partitionId, version)
      true
    }
  }

  override def process(row: T): Unit = {
    val outputPath = processor.outputPath(row)
    val outputString = processor.outputString(row)

    if(outputPath == null || outputString == null){
      return
    }

    if(!buffer.contains(outputPath)){
      buffer.put(outputPath, fs.create(pathToDeltaPath(outputPath), true))
    }

    buffer.get(outputPath).get.write(outputString.getBytes())
  }

  override def close(errorOrNull: Throwable): Unit = {
    for(outputPath <- buffer.keySet){
      buffer.get(outputPath).get.close()
      if(!retry) {
        val deltaPath = pathToDeltaPath(outputPath)
        val inputStream = new BufferedInputStream(fs.open(deltaPath))
        val outputStream = new BufferedOutputStream(fs.append(outputPath))
        pipe(inputStream, outputStream)
        inputStream.close()
        outputStream.close()
        fs.setExpiryTime(deltaPath, conf.expiryTime)
        fs.setExpiryTime(outputPath, conf.expiryTime)

      }else{
        val outputStream = new BufferedOutputStream(fs.create(outputPath, true))
        val outputDirPath = new Path(outputPath).getParent.toUri.toString
        getDeltaFiles(outputDirPath)
          .foreach(f => {
            val inputStream = new BufferedInputStream(fs.open(f))
            pipe(inputStream, outputStream)
            inputStream.close()
            fs.setExpiryTime(f, conf.expiryTime)
        })

        outputStream.close()
        fs.setExpiryTime(outputPath, conf.expiryTime)
      }
    }

    sinkLog.addDoneFlag(partitionId, version)
  }

  private def deltaSuffix(): String = {
    s"_${version}_${partitionId}_${DELTASUFFIX}"
  }

  private def pathToDeltaPath(path: String): String = {
    path + deltaSuffix()
  }

  private def deltaPathToPath(deltaPath: String): String = {
    deltaPath.substring(0, deltaPath.size - deltaSuffix().length)
  }

  private def getDeltaFiles(path: String): Array[String] = {
    fs.list(path).asScala
      .filter(f => !f.isDir && f.path.endsWith(s"_${partitionId}_${DELTASUFFIX}"))
      .map(f=>f.path)
      .toArray
  }

  private def pipe(inputStream: InputStream, outputStream: OutputStream): Unit = {
    Iterator
      .continually(inputStream.read)
      .takeWhile(-1 !=)
      .foreach(outputStream.write)
  }
}
