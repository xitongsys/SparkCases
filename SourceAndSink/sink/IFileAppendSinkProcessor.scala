/**
  * Processor class for FileAppendSink.
  * This class implements two functions:
  * 1. outputPath: give a row data from the DataFrame, return the output path of this row.
  * 2. outputString: give a row data from the DataFrame, return the output string to file of this row.
  * @tparam T row type.
  */
trait IFileAppendSinkProcessor[T] {
  /**
    * Get the output path of one row.
    * @param row data to process.
    * @return path to output.
    */
  def outputPath(row: T): String

  /**
    * Get the output string of one row.
    * @param row data to process.
    * @return output string to file.
    */
  def outputString(row: T): String
}
