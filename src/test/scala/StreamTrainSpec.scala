package com.bbvalabs.ai.test

import akka.actor.Props
import akka.stream.scaladsl.Source
import com.bbvalabs.ai.Implicits.as
import com.bbvalabs.ai._
import com.bbvalabs.ai.runtime.{SinkActor, StreamTrainer, TaskSupervisor}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpecLike}
import shapeless.{Lens, lens}

import scala.concurrent.Await
import scala.concurrent.duration._
import scalaz.effect.IO


class FakeSettings(nentries: Int) extends Settings {
  override val entries = nentries
}


import impls._
import Implicits._

object streamCommons {
  def createSequenceHandler(settings: Settings, f: (ExampleModel, ExampleModel) => ExampleDelta)(implicit  lns: Lens[ExampleModel, String], col : String): (SequenceHandler[ExampleModel, ExampleDelta],
    Repository[ExampleModel, ExampleDelta]) = {

    implicit val _settings = settings

    implicit def append(_new: ExampleModel, last: ExampleModel): (ExampleModel, ExampleDelta) = (_new, f(_new, last))
    implicit val ec = as.dispatcher

    implicit def toMonoid = append _ lift

    implicit val repo = new Repository[ExampleModel, ExampleDelta]()
    val sh = Await.result(SequenceHandler[ExampleModel, ExampleDelta], 50 seconds)

    (sh, repo)

  }
}

@RunWith(classOf[JUnitRunner])
class StreamTrainSpec
    extends WordSpecLike
    with Matchers {

  import streamCommons._

  class FakeEntries1(netries: Int, groupednum: Int) extends Settings {
    override val entries: Int = netries
    override val grouped: Int = groupednum
  }

  "A Stream in training mode" must {

    "read events from the standard input and put in the ouptut stream the enriched events" in {

      val settings2 = new FakeSettings(1)

      val taskSupervisor = as.actorOf(Props[TaskSupervisor])

      def label: ExampleModel => String = n => n.label.toString

      var messages: List[String] = List()

      def io: String => IO[Unit] =
        str =>
          IO {
            messages = str :: messages
            println(str)
        }

      val stdinSource =
        Source(List("1,1,1,1,1", "1,2,2,2,0", "1,3,3,3,1", "1,4,4,4,0", "1,5,5,5,1"))

      implicit def sortBy(_new: ExampleModel): String = _new.id

      implicit val lns = lens[ExampleModel] >> 'id

      implicit val col : String = "test1"

      val sinkStream = as.actorOf(
        Props(new SinkActor[ExampleModel](
            settings2,
            List(createSequenceHandler(settings2, (d1,d2) => ExampleDelta(d2.field2 -  d1.field2))._1),
            Implicits.partialfunc,
            taskSupervisor,
            label,
            io)))

      new StreamTrainer[ExampleModel, String](settings2, sinkStream, stdinSource)

      Thread.sleep(30000)
      messages.size shouldBe 5
    }
  }
}
