package com.poc.hbasetables.utils

sealed trait TableStatus

object TableStatus {

  case object CREATED extends TableStatus
  case object ERROR extends TableStatus
  case object EXISTS extends TableStatus
  case object MODIFIED extends TableStatus
}
