package com.poc.hbasetables.repository


import com.poc.hbasetables.utils.TableDescription
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState
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

  def modifyTable(tableName:TableName, tableDescriptor: HTableDescriptor): Unit = {
    lazy val admin = this.getConnection.getAdmin
    admin.modifyTable(tableName, tableDescriptor)
  }

  def isTableExists(tableDesc: TableDescription): Boolean = {
    lazy val admin = this.getConnection.getAdmin
    val tableExists = admin.tableExists(this.getTableName(tableDesc))
    tableExists
  }

  def doMajorCompact(tableNamespace: String, tableNm: String): Unit = {
    val conn = this.getConnection
    val admin: Admin = conn.getAdmin
    try{

      val tableName = TableName.valueOf(s"${tableNamespace}:${tableNm}")

      val currentCompactionState = admin.getCompactionState(tableName)

      val lastMajorCompactionTs = admin.getLastMajorCompactionTimestamp(tableName)
      admin.majorCompact(tableName)

      var state = admin.getCompactionState(tableName)

      while(state.equals(CompactionState.MAJOR)){
        Thread.sleep(5000)
        state = admin.getCompactionState(tableName)
      }
    }finally{
      admin.close()
      conn.close()
    }
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
