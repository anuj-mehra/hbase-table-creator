package com.poc.hbasetables.utils

case class TableDescription(name: String, namespace: String, columnFamily: String, splitSize: String,
                            replicationScope: Int, modifyTableIfExists: Option[Boolean],
                            maxColumnHistory: Integer, enableBloomFilter: Boolean)
