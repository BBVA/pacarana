package com.bbvalabs.ai.runtime

import akka.actor.{Actor, ActorLogging, ActorRef}

/**
  * Created by e049627 on 5/6/17. This actor checks if all tasks have been executed
  */
case class AckBox(total: Int, n: Int, ref: ActorRef)

final class TaskSupervisor extends Actor with ActorLogging {
  var counter: Int = 0
  var _total: Int  = 0

  def receive = {
    case AckBox(totalTask, n, ref) => {
      counter = counter + n
      _total = totalTask
      log.debug(s"Total tasks: ${totalTask}")
      log.debug(s"N Bulk: ${n}")
      log.debug(s"Counter: ${counter}")
      log.debug(s"Total: ${_total}")
      if (counter == _total) {
        log.debug(s"Counter to 0")
        counter = 0
        ref ! "ack"
      }
    }
  }
}
