package com.poc.hbasetables.repository


import com.poc.hbasetables.utils.TableDescription
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputControllerFactory
import org.apache.hadoop.hbase.regionserver.compactions.NoLimitCompactionThroughputController
import org.apache.hadoop.hbase.util.RegionSplitter

class HBaseAdminRepository(conf: Configuration) {

  private val splitter = new RegionSplitter.HexStringSplit()

  def getConnection: Connection = {
    val conn: Connection = ConnectionFactory.createConnection(conf)
    conn
  }

  def getTableName(tableDesc: TableDescription): TableName = {
    TableName.valueOf(s"${tableDesc.namespace}:${tableDesc.name}")
  }

  def createTable(tableDescriptor: HTableDescriptor): Unit = {
    lazy val admin = this.getConnection.getAdmin
    admin.createTable(tableDescriptor)
  }

  def createTable(tableDescriptor: HTableDescriptor, splitSize: Int): Unit = {
    lazy val admin = this.getConnection.getAdmin
    admin.createTable(tableDescriptor, splitter.split(splitSize))
  }

  def modifyTable(tableDescriptor: HTableDescriptor): Unit = {
    lazy val admin = this.getConnection.getAdmin
    admin.modifyTable(tableDescriptor)
  }

  def isTableExists(tableDesc: TableDescription): Boolean = {
    lazy val admin = this.getConnection.getAdmin
    val tableExists = admin.tableExists(this.getTableName(tableDesc))
    tableExists
  }

}

object HBaseAdminRepository {

  def apply(@transient conf: Configuration): HBaseAdminRepository = {
    val config = HBaseConfiguration.create(conf)
    config.set("mapreduce.outputformat.class","org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
    config.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      classOf[NoLimitCompactionThroughputController].getName)
    new HBaseAdminRepository(config)
  }
}
