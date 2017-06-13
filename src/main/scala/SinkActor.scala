package com.bbvalabs.ai.runtime

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.bbvalabs.ai._

import scalaz.concurrent.Task
import scalaz.{-\/, Monoid, \/, \/-}

/**
  * Created by e049627 on 5/6/17.
  */
final class SinkActor[A <: Model, B <: DeltaType](
    handler: SequenceHandler[A, B],
    func: PartialFunction[Any, (Int, List[DeltaModel2[A, B]])],
    ackref: ActorRef
)(implicit val monoid: Monoid[Sequence[A, B]]) extends Actor
    with ActorLogging {

  def group[A, K](list: List[A])(f: A => K): Map[K, List[A]] = {
    list.groupBy(f)
  }

  def checkTaskResultAndFlatten(
      result: Throwable \/ (Int, List[DeltaModel2[A, B]]),
      ref: ActorRef): Either[String, Int] = {
    val proc = result match {
      case -\/(err) => {
        log.error(s"Error ${err}")
        Left(err.getMessage)
      }
      case \/-((a, deltas)) => {
        log.info(s"delta number ${deltas.size} for id ${deltas.head.model.id}")
        Right(a)
      }
    }
    proc
  }

  implicit val ec = context.dispatcher

  val ackMessage: String = "ack"
  val initMessage: String = "start"
  val healthCheck: String = "heartbeat"
  val notReady: String = "notready"
  val ready: String = "ready"

  /** Process will start as soon as database is ready **/

  val partial = func andThen { case (ngrps, list)  =>
    var origin = sender()
    handler.processBatchinOneModel(list, 0, Nil)
    .unsafePerformAsync { result =>
      val _result = checkTaskResultAndFlatten(result, self)
      if (_result.isLeft) {
        log.error(s"Error executing task ${_result.left}")
      }
      ackref ! AckBox(ngrps, _result.getOrElse(0), origin)
    }
  }

  def receive: Receive = partial orElse _receive

  def _receive: PartialFunction[Any, Unit] = {

    /** Sink actor initialization **/
    case `initMessage` => {
      log.info("Initializing sink actor")
      sender() ! ackMessage
    }

    case `healthCheck` => {
      log.info("Heartbeat received")
      sender ! ready
    }

    /** Error sent. Just log and ignore **/
    case a: AnyRef => {
      log.error(s"ERROR. Unhanded message ${a}")
      sender ! ackMessage
    }
  }
}
