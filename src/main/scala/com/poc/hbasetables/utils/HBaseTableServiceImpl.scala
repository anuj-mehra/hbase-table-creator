package com.poc.hbasetables.utils

import com.poc.hbasetables.repository.HBaseAdminRepository
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.regionserver.BloomType

class HBaseTableServiceImpl(hbaseConf: Configuration) {

  private val hbaseAdminRepository = new HBaseAdminRepository(hbaseConf)

  def createOrUpdateTable(tableDesc: TableDescription): Unit = {

    val tableExists = hbaseAdminRepository.isTableExists(tableDesc)
    tableExists match {
      case true =>
        if(tableDesc.modifyTableIfExists.getOrElse(false)){
          val tableDescriptor: HTableDescriptor = this.getTableDescriptor(tableDesc)
          hbaseAdminRepository.modifyTable(tableDescriptor)
        }
      case false =>
        val tableDescriptor = this.getTableDescriptor(tableDesc)
        if(Option(tableDesc.splitSize).isDefined){
          hbaseAdminRepository.createTable(tableDescriptor, tableDesc.splitSize.toInt)
        }else{
          hbaseAdminRepository.createTable(tableDescriptor)
        }
    }

  }


  private def getTableDescriptor(tableDesc: TableDescription):HTableDescriptor = {
    val tableDescriptor = new HTableDescriptor(hbaseAdminRepository.getTableName(tableDesc))
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


}
