/*
   Copyright 2018 Banco Bilbao Vizcaya Argentaria, S.A.
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.bbva.pacarana.runtime

import akka.actor.{Actor, ActorLogging, ActorRef}

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
