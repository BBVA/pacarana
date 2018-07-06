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
import akka.event.LoggingAdapter
import com.bbva.pacarana.model.Model
import com.bbva.pacarana.model.SequencerTypes.{DataForRun, DataForTrain}
import com.bbva.pacarana.settings.Settings

import scalaz.effect.IO
import scalaz.{-\/, Scalaz, \/, \/-}
import scalaz.syntax.traverse.ToTraverseOps
import scalaz.std.list.listInstance

/**
  *  TODO: Try to include this partial function in actors body. There were errors with sender property
  */
object SinkFunctions {

  /** Sink actor initialization **/
  val ackMessage: String = "ack"
  val initMessage: String = "start"
  val healthCheck: String = "heartbeat"
  val notReady: String = "notready"
  val ready: String = "ready"
  val completeMessage = "complete"
  val completed = "completed"

  def getSinkCommonPartialFunction(s: ActorRef)(
      implicit log: LoggingAdapter): PartialFunction[Any, Unit] = {
    case `initMessage` => {
      log.info("Initializing sink actor")
      s ! ackMessage
    }

    case `healthCheck` => {
      log.info(s"Heartbeat received. Responding to ${s}")
      s ! ready
    }

    /** Error sent. Just log and ignore **/
    case a: AnyRef => {
      log.error(s"ERROR. Unhanded message ${a}")
      s ! ackMessage
    }
  }

  // TODO: this function should group by a composed key to prevent document collisions
  def group[A, K](list: List[A])(f: A => K): Map[K, List[A]] = {
    list.groupBy(f)
  }
}

import SinkFunctions._
import Scalaz._

final class SinkActor[A <: Model](
    settings: Settings,
    handler: List[SequenceHandler[A, _]],
    func: PartialFunction[Any, DataForTrain[A]],
    ackref: ActorRef,
    funcLabel: A => String,
    io: String => IO[Unit]
) extends Actor
    with ActorLogging {

  implicit val ec = context.dispatcher
  implicit val logAd = log

  var origin: Option[ActorRef] = None

  /** Process will start as soon as database is ready **/
  val partial = func andThen {
    case list => {
      origin = Some(sender())
      val e2 = list.map(_._2.size).sum

      // TODO: This code should be more expressive
      val totalTask = e2
      val result = list foreach {
        case (ngrps, list) =>
          handler
            .traverse(_.processBatchinOneModel(list, 0, Nil))
            .unsafePerformAsync { r =>
              r match {
                case -\/(err) => {
                  log.error(s"Error ${err}")
                }
                case \/-(list) => {
                  list match {
                    case ((n, l), f) :: t =>
                      log.debug(s"n tasks: ${n}")
                      log.debug(s"total tasks ${totalTask}")

                      if (!l.isEmpty) {
                        log.debug(s"L size ${l.size}")
                        l.foreach { l1 =>
                          /** l1 corresponds to th length of each Delta list length **/
                          if (l1.size == settings.entries) {
                            val strMaster = f(l1.right)
                            /* master model for label when window > 1 */
                            val masterModel = l1.head.model
                            val strTail = t.map {
                              case ((i, e), f2) => {
                                  e.map(r => f2(r.right))
                              }
                            }

                            /** to print to concatenate print from head and tail **/
                            val toPrint = {
                              val label = funcLabel(masterModel)
                              if (strTail.size == 0) {
                                if (!label.isEmpty)
                                  s"${strMaster},${funcLabel(masterModel)}"
                                else
                                  s"${strMaster}"
                              }
                              else {
                                val result = {
                                  if (!label.isEmpty) {
                                    s"${strMaster},${
                                      strTail.foldLeft("")((a, acc) =>
                                        s"${a}" ++ s"${acc.mkString(",")}")
                                    },${funcLabel(masterModel)}"
                                  }
                                  else {
                                    s"${strMaster},${
                                      strTail.foldLeft("")((a, acc) =>
                                        s"${a}" ++ s"${acc.mkString(",")}")
                                    }"
                                  }
                                }
                                result
                              }
                            }
                            io(toPrint).unsafePerformIO()
                          }

                          ackref ! AckBox(totalTask, 1, origin.get)
                        }
                      } else {
                        // TODO: In case of input error a valid message is received. Change it in shapeless decoder to throw one exception
                        log.warning(
                          "Invalid message. Requesting more elements")
                        origin.get ! "ack"
                      }
                    case _ => println("DO Nothing")
                  }
                }
              }
            }
      }
    }
  }

  def receive: Receive = partial orElse _receive

  def _receive: PartialFunction[Any, Unit] = {
    // TODO: There were problems to extract this partial function due to sender property. Fix IT!!!
    /** Sink actor initialization **/
    case `initMessage` => {
      log.info("Initializing sink actor")
      sender() ! ackMessage
    }

    case `healthCheck` => {
      log.info("Heartbeat received")
      sender ! ready
    }

    case `completeMessage` => {
      log.warning("EOF received. Completing stream and exiting...")
      sender ! completed
      //context.system.terminate().onComplete(_ => System.exit(0))
    }

    /** Error sent. Just log and ignore **/
    case a: AnyRef => {
      log.error(s"ERROR. Unhanded message ${a}")
      sender ! ackMessage
    }
  }
}

final class SinkActorRunner[A <: Model](
    settings: Settings,
    handler: List[SequenceHandler[A, _]],
    func: PartialFunction[Any, DataForRun[A]],
    ackref: ActorRef,
    io: String => IO[Unit]
) extends Actor
    with ActorLogging {

  implicit val ec = context.dispatcher
  implicit val logAd = log

  var origin: Option[ActorRef] = None
  var completed : Boolean = false

  /** Process will start as soon as database is ready **/
  val partial = func andThen {
    case list => {
      origin = Some(sender())
      val e2 = list.map(_._2.size).sum
      val totalTask = e2
      val result = list foreach {
        case (ngrps, list) =>
          handler
            .traverse(_.processBatchinOneModelTupled(list, 0, Nil))
            .unsafePerformAsync { r =>
              r match {
                case -\/(err) => {
                  log.error(s"Error ${err}")
                }
                case \/-(list) =>
                  list match {
                    case ((n, l), f) :: t => {
                      if (!l.isEmpty) {
                        l.foreach { l1 =>
                          if (l1.size == settings.entries) {
                            val strMaster = f(l1.left)
                            val masterModel = l1.head._2.model

                            val strTail = t.map {
                              case ((i, e), f2) => {
                               // if (e.size == settings.entries) {
                                // e.map(r => f2(r.left))
                                val w = e.map(r => r.map(e => e._2))
                                w.map(w0 => f2(w0.right))
                               // } else List()
                              }
                            }

                            val toPrint = {
                              if (strTail.isEmpty)
                                s"${strMaster}"
                              else {
                                s"${strMaster},${strTail.foldLeft("")(
                                  (a, acc) => s"${a}" ++ s"${acc.mkString(",")}")}"
                              }
                            }
                            io(toPrint).unsafePerformIO
                          }
                          ackref ! AckBox(totalTask, 1, origin.get)
                        }
                      } else {
                        log.warning(
                          "Invalid message. Requesting more elements")
                        origin.get ! "ack"
                      }
                    }
                    case _ => // Do nothing!!
                  }
              }
            }
      }
    }
  }

  def receive: Receive = partial orElse _receive

  def _receive: PartialFunction[Any, Unit] = {
    // TODO: There were problems to extract this partial function due to sender property. Fix IT!!!
    /** Sink actor initialization **/
    case `initMessage` => {
      log.info("Initializing sink actor")
      sender() ! ackMessage
    }

    case `healthCheck` => {
      log.info("Heartbeat received")
      sender ! ready
    }

    case `completeMessage` => {
      log.warning("EOF received. Completing stream and exiting...")
      sender ! completed
    }

    /** Error sent. Just log and ignore **/
    case a: AnyRef => {
      log.error(s"ERROR. Unhanded message ${a}")
      sender ! ackMessage
    }
  }
}
