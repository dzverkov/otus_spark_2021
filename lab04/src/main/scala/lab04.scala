import java.io._

import scala.io.Source._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

object lab04 extends App{

  val srcPath = "/stage"
  val destPath = "/ods"

  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")
  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)
  private val fileSystem = FileSystem.get(conf)

  var writer: BufferedWriter = null
  val srcFolder = fileSystem.listStatus(new Path(srcPath))

  srcFolder.foreach(f => {
    //println(f.getPath)

    // Create folder
    val destFolderPath = new Path(destPath + "/" + f.getPath.getName)
    if (!fileSystem.exists(destFolderPath))
      {
        fileSystem.mkdirs(destFolderPath)
      }

    writer = new BufferedWriter(
                                new OutputStreamWriter(fileSystem.create(
                                  new Path(destFolderPath + "/part-0000.csv")
                                ), "UTF-8"))

    val srcFiles = fileSystem.listStatus(f.getPath)
    srcFiles.foreach(fl => {
      if (fl.isFile && fl.getPath.getName.matches(".*csv") ) {
        val fVal = fromInputStream(fileSystem.open(fl.getPath)).mkString("")
        writer.write(fVal)
        //println(fl.getPath)
      }
    })
    writer.close()
  })
}
