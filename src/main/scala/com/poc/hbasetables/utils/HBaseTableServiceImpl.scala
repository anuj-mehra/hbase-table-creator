package com.poc.hbasetables.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.util.RegionSplitter

class HBaseTableServiceImpl(hbaseConf: Configuration) {

  private val splitter = new RegionSplitter.HexStringSplit()

  def createTable(tableDesc: TableDescription): Unit = {

    lazy val connection = this.getConnection(hbaseConf)

    lazy val tableName: String = tableDesc.name
    lazy val columnFamily: String = tableDesc.columnFamily
    lazy val splitSize: String = tableDesc.splitSize
    lazy val tableNamespace: String = tableDesc.namespace
    lazy val replicationScope: Int = tableDesc.replicationScope
    lazy val maxColumnHistory: Integer = tableDesc.maxColumnHistory
    lazy val modifyTableIfExists: Option[Boolean] = tableDesc.modifyTableIfExists
    lazy val enableBloomFilter: Boolean = tableDesc.enableBloomFilter


  }

  def modifyTable(): Unit = {

  }

  private def getConnection(conf: Configuration): Connection = {
    val conn: Connection = ConnectionFactory.createConnection(conf)
    conn
  }
}
