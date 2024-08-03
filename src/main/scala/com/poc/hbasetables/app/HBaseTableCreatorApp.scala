package com.poc.hbasetables.app

import com.poc.hbasetables.utils.HBaseTableServiceImpl
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object HBaseTableCreatorApp extends App {


  val obj = new HBaseTableCreatorApp

  val hbaseConf: Configuration = HBaseConfiguration.create()
}

class HBaseTableCreatorApp(hbaseConf: Configuration) extends Serializable{

  def process(hbaseTableName: String, columnFamily: String, namespace: String): Unit ={

    val service = new HBaseTableServiceImpl(hbaseConf)

  }
}
