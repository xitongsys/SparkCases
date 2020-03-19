import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CreateFlag, FSDataInputStream, FSDataOutputStream, FileAlreadyExistsException, FileContext, FileStatus, Path, PathFilter, UnsupportedFileSystemException}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.internal.SQLConf

/**
  * SinkLog record logs of every partition/batchid(version) processing.
  * @param hadoopConf hadoop configuration.
  * @param partitionId partition id.
  * @param metadataPath log file path.
  */
class FileAppendSinkLog(hadoopConf: Configuration, partitionId: Long, metadataPath: String) {
  private val minLogEntriesToMaintain: Int = SQLConf.MIN_BATCHES_TO_RETAIN.defaultValue.get
  require(minLogEntriesToMaintain > 0, "minBatchesToRetain has to be positive")

  private val fs = CheckpointFileManager.create(new Path(metadataPath), hadoopConf)
  val path = new Path(metadataPath)
  if(!fs.exists(path)){
    fs.mkdirs(path)
  }

  /**
    * Add Done log.
    * @param partitionId
    * @param version
    */
  def addDoneFlag(partitionId: Long, version: Long): Unit = {
    val path = logFileDoneFlagPath(partitionId, version)
    if(!fs.exists(path)) {
      fs.createAtomic(path, true)
    }

    purge(version - minLogEntriesToMaintain)
  }

  /**
    * Get Done log.
    * @param partitionId
    * @param version
    * @return true: Done log exists. false: Done log doesn't exists.
    */
  def getDoneFlag(partitionId: Long, version: Long): Boolean = {
    fs.exists(logFileDoneFlagPath(partitionId, version))
  }

  /**
    * Add Start log.
    * @param partitionId
    * @param version
    */
  def addStartFlag(partitionId: Long, version: Long): Unit = {
    val path = logFileStartFlagPath(partitionId, version)
    if(!fs.exists(path)) {
      fs.createAtomic(path, true)
    }
  }

  /**
    * Get Start log.
    * @param partitionId
    * @param version
    * @return true: Start log exists. false: Start log doesn't exists.
    */
  def getStartFlag(partitionId: Long, version: Long): Boolean = {
    fs.exists(logFileStartFlagPath(partitionId, version))
  }

  private def purge(thresholdBatchId: Long): Unit = {
    val batchIds = fs.list(new Path(metadataPath), logFilesFilter)
      .filter(f => !f.isDirectory)
      .map(f => pathToBatchId(f.getPath)).distinct

    for (batchId <- batchIds if batchId < thresholdBatchId) {
      val startPath = logFileStartFlagPath(partitionId, batchId)
      val donePath = logFileDoneFlagPath(partitionId, batchId)
      fs.delete(startPath)
      fs.delete(donePath)
    }
  }

  private def pathToBatchId(path: Path): Long = {
    (path.getName).split("_")(0).toLong
  }

  private val logFilesFilter = new PathFilter {
    override def accept(path: Path): Boolean = isLogFile(path)
  }

  private def isLogFile(path: Path) = {
    try {
      val ns = path.getName.split("_")
      ns(0).toLong
      (ns(2) == "start" || ns(2) == "done") && ns(1).toLong == partitionId
    } catch {
      case _: Throwable => false
    }
  }

  private def logFileStartFlagPath(partitionId: Long, version: Long): Path = {
    new Path(metadataPath, s"${version}_${partitionId}_start")
  }

  private def logFileDoneFlagPath(partitionId: Long, version: Long): Path = {
    new Path(metadataPath, s"${version}_${partitionId}_done")
  }
}