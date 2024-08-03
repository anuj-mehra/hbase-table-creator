package com.poc.hbasetables.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.RegionSplitter

class HBaseTableServiceImpl(hbaseConf: Configuration) {

  private val splitter = new RegionSplitter.HexStringSplit()

  def createOrUpdateTable(tableDesc: TableDescription): Unit = {
    lazy val connection = this.getConnection(hbaseConf)
    lazy val admin = connection.getAdmin

    val tableExists = admin.tableExists(getTableName(tableDesc))
    tableExists match {
      case true =>
        if(tableDesc.modifyTableIfExists.getOrElse(false)){
          val tableDescriptor = this.getTableDescriptor(tableDesc)
          admin.modifyTable(tableDescriptor)
        }
      case false =>
        val tableDescriptor = this.getTableDescriptor(tableDesc)
        if(Option(tableDesc.splitSize).isDefined){
          admin.createTable(tableDescriptor, splitter.split(tableDesc.splitSize.toInt))
        }else{
          admin.createTable(tableDescriptor)
        }
    }

  }


  private def getTableDescriptor(tableDesc: TableDescription):HTableDescriptor = {
    val tableDescriptor = new HTableDescriptor(getTableName(tableDesc))
    val colFamilyDescriptor = new HColumnDescriptor(tableDesc.columnFamily)
    if(tableDesc.enableBloomFilter){
      colFamilyDescriptor.setBloomFilterType(BloomType.ROW)
    }
    if(tableDesc.blockCacheEnabled){
      colFamilyDescriptor.setBlockCacheEnabled(tableDesc.blockCacheEnabled)
    }
    colFamilyDescriptor.setMaxVersions(tableDesc.maxColumnHistory)
    colFamilyDescriptor.setScope(tableDesc.replicationScope)

    tableDescriptor.addFamily(colFamilyDescriptor)
    tableDescriptor
  }

  private def getConnection(conf: Configuration): Connection = {
    val conn: Connection = ConnectionFactory.createConnection(conf)
    conn
  }

  private def getTableName(tableDesc: TableDescription): TableName = {
    TableName.valueOf(s"${tableDesc.namespace}:${tableDesc.name}")
  }

  private def persistStatus(conn: Connection, tableDesc: TableDescription, status: TableStatus): unit = {

  }


}
