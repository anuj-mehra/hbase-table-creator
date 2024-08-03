package com.poc.hbasetables.utils

import com.poc.hbasetables.repository.HBaseAdminRepository
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.regionserver.BloomType

class HBaseTableServiceImpl(hbaseAdminRepository: HBaseAdminRepository) {

  //private

  def createOrUpdateTable(tableDesc: TableDescription): Unit = {

    try{
      val tableExists = hbaseAdminRepository.isTableExists(tableDesc)
      tableExists match {
        case true =>
          if(tableDesc.modifyTableIfExists.getOrElse(false)){
            val tableDescriptor: HTableDescriptor = this.getTableDescriptor(tableDesc)
            val tableName: TableName = hbaseAdminRepository.getTableName(tableDesc)
            hbaseAdminRepository.modifyTable(tableName, tableDescriptor)
          }
        case false =>
          val tableDescriptor = this.getTableDescriptor(tableDesc)
          if(Option(tableDesc.splitSize).isDefined){
            hbaseAdminRepository.createTable(tableDescriptor, tableDesc.splitSize.toInt)
          }else{
            hbaseAdminRepository.createTable(tableDescriptor)
          }
      }
    }finally{
      hbaseAdminRepository.getConnection.getAdmin.close()
      hbaseAdminRepository.getConnection.close()
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

  def doMajorCompaction(hbaseTableName: String, hbaseNamespace: String): Unit = {
    hbaseAdminRepository.doMajorCompact(hbaseNamespace, hbaseTableName)
  }

}
