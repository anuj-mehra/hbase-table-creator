package com.poc.hbasetables.app

import com.poc.hbasetables.repository.HBaseAdminRepository
import com.poc.hbasetables.utils.HBaseTableServiceImpl
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object HBaseTableMajorCompactionApp extends App{

  val hbaseConf: Configuration = HBaseConfiguration.create()
  val hbaseAdminRepository = new HBaseAdminRepository(hbaseConf)
  val obj = new HBaseTableMajorCompactionApp(hbaseAdminRepository)
  obj.process("position","cpbqd")
}

class HBaseTableMajorCompactionApp(hbaseAdminRepository: HBaseAdminRepository) extends Serializable{

  def process(hbaseTableName: String, hbaseNamespace: String): Unit = {
    val service = new HBaseTableServiceImpl(hbaseAdminRepository)
    service.doMajorCompaction(hbaseTableName, hbaseNamespace)
  }
}
