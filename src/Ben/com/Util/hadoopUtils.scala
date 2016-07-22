package Ben.com.Util

import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
@transient
object hadoopUtils extends Serializable{

  def isFileExists(path: String, HDFSFileSytem: org.apache.hadoop.fs.FileSystem) = HDFSFileSytem.exists(new Path(path))

  def deleteFile(path: String, HDFSFileSytem: org.apache.hadoop.fs.FileSystem) = HDFSFileSytem.delete(new Path(path), true)

  def mkdirs(path: String, HDFSFileSytem: org.apache.hadoop.fs.FileSystem) = HDFSFileSytem.mkdirs(new Path(path))

  def create(path: String, HDFSFileSytem: org.apache.hadoop.fs.FileSystem) = HDFSFileSytem.create(new Path(path), false)

  def copyToLocal(hdfsPath: String, localPath: String, HDFSFileSytem: org.apache.hadoop.fs.FileSystem) = HDFSFileSytem.copyToLocalFile(new Path(hdfsPath), new Path(localPath))

  def merge(srcPath: String, dstPath: String, hadoop_conf: org.apache.hadoop.conf.Configuration): Unit = {
    val srcFileSystem = FileSystem.get(URI.create(srcPath), hadoop_conf)
    val dstFileSystem = FileSystem.get(URI.create(dstPath), hadoop_conf)
    dstFileSystem.delete(new Path(dstPath), true)
    FileUtil.copyMerge(srcFileSystem, new Path(srcPath), dstFileSystem, new Path(dstPath), false, hadoop_conf, null)
    dstFileSystem.copyToLocalFile(new Path(dstPath), new Path(dstPath))
    dstFileSystem.delete(new Path(dstPath), true)
  }

  def getFile2Array(fileName: String) = {
    val input = scala.io.Source.fromFile(fileName, "UTF-8")
    try input.getLines.toArray finally input.close()
  }
  

}