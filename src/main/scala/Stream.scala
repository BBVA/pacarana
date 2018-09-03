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

import akka.NotUsed
import akka.pattern.{after, ask}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.util.Timeout
import akka.actor.{ActorRef, ActorSystem}
import com.bbva.pacarana.model.Model
import com.bbva.pacarana.parser.CSVConverter
import com.bbva.pacarana.settings.Settings

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}

case class InputMsgs[A <: Model](list: List[A])
case class InputMsgsRunner[A <: Model](list: List[(String, A)])

object StreamOps {

  /** TODO: Extract to config **/

  final class StdinSourceStage extends GraphStage[SourceShape[String]] {
    val out: Outlet[String] = Outlet("Stdin.out")
    override val shape: SourceShape[String] = SourceShape(out)

    override def createLogic(
        inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) {
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            val str = StdIn.readLine()
            if (str != null)
              push(out, str)
            else {
              //complete(out)
            }
          }
        })
      }
    }
  }

  def checkIfSinkIsActive(ref: ActorRef)(
      implicit ac: ActorSystem): Future[String] = {

    implicit val timeout = Timeout(5 seconds)
    implicit val ec = ac.dispatcher

    val s1 = after[String](5 seconds, ac.scheduler) {
      (ref ? "heartbeat").mapTo[String]
    }
    s1 flatMap { res =>
      res match {
        case "notready" => checkIfSinkIsActive(ref)
        case "ready" => s1
      }
    }
  }

}

final class StreamTrainer[A <: Model, C](
    settings: Settings, sink: ActorRef, source: Source[String, NotUsed])(implicit as: ActorSystem, am: ActorMaterializer, cv: CSVConverter[A], ford: A => C, ord: Ordering[C]) {

  import StreamOps._

  implicit val ec = as.dispatcher

  val ackMessage      = "ack"
  val initMessage     = "start"
  val completeMessage = "complete"
  val healthCheck     = "heartbeat"

  val stream = source
    .watchTermination()((_, f) => {
      f.onComplete {
        case Success(e) =>  //TODO. add logger
        case Failure(err) => //TODO. add logger
      }
    })
    .groupedWithin(settings.grouped, settings.milliss milliseconds)
    .map(_.foldLeft(List[A]()) { (a, b) =>
      cv.from(b) match {
        case Success(line) => {
          line :: a
        }
        case Failure(err) => {
          Nil
        }
      }
    })
    .map(e => InputMsgs(e.sortBy(ford)))
    .watchTermination()((_, f) => {
      f.onComplete {
        case Success(e) => //TODO. add logger
        case Failure(err) => //TODO. add logger
      }
    })
    .to(Sink.actorRefWithAck(sink, initMessage, ackMessage, completeMessage))

  checkIfSinkIsActive(sink).onComplete(
    _ => stream.run()
  )
}

final class StreamRunner[A <: Model](settings: Settings, sink: ActorRef, source: Source[String, NotUsed])(implicit as: ActorSystem, am: ActorMaterializer, cv: CSVConverter[(String, A)]) {
  import StreamOps._

  implicit val ec = as.dispatcher

  val ackMessage      = "ack"
  val initMessage     = "start"
  val completeMessage = "complete"
  val healthCheck     = "heartbeat"

  // TODO: Common factor. The only diference is CSV converter
  val stream = source
    .watchTermination()((_, f) => {
      f.onComplete {
        case Success(e) => //TODO. add logger
        case Failure(err) => //TODO. add logger
      }
    })
    .groupedWithin(settings.grouped, settings.milliss milliseconds)
    .map(_.foldLeft(List[(String, A)]()) { (a, b) =>
      cv.from(b) match {
        case Success(line) => {
          line :: a
        }
        case Failure(err) => {
          Nil
        }
      }
    })
    .map(e => InputMsgsRunner(e) )
    .watchTermination()((_, f) => {
      f.onComplete {
        case Success(e) => //TODO. add logger
        case Failure(err) => //TODO. add logger
      }
    })
    .to(Sink.actorRefWithAck(sink, initMessage, ackMessage, completeMessage))

  checkIfSinkIsActive(sink).onComplete(
    _ => stream.run()
  )
}
