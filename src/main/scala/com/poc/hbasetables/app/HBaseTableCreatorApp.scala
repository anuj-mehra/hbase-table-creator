package com.poc.hbasetables.app

import com.poc.hbasetables.app.HBaseTableMajorCompactionApp.hbaseConf
import com.poc.hbasetables.repository.HBaseAdminRepository
import com.poc.hbasetables.utils.{HBaseTableServiceImpl, TableDescription}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object HBaseTableCreatorApp extends App {

  val hbaseConf: Configuration = HBaseConfiguration.create()
  val hbaseAdminRepository = new HBaseAdminRepository(hbaseConf)
  val obj = new HBaseTableCreatorApp(hbaseAdminRepository)

  obj.process
}

class HBaseTableCreatorApp(hbaseAdminRepository: HBaseAdminRepository) extends Serializable{

  def process: Unit ={

    val service = new HBaseTableServiceImpl(hbaseAdminRepository)

    val tableDesc: TableDescription = TableDescription("position", "cpbrd", "cf", "100",
      replicationScope=1, modifyTableIfExists = Option(true),
      maxColumnHistory=10, enableBloomFilter=true, blockCacheEnabled=true)

    service.createOrUpdateTable(tableDesc)
  }
}
