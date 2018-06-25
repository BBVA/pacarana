package com.bbvalabs.ai.test

import akka.NotUsed
import akka.actor.Props
import akka.stream.scaladsl.Source
import com.bbvalabs.ai._
import com.bbvalabs.ai.runtime._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import scalaz.effect.IO
import shapeless.{Lens, lens}

@RunWith(classOf[JUnitRunner])
class StreamRunnerSpec
  extends WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  var mongoProcess0 : Option[MongodProps] = None

  class FakeSettings(nentries: Int) extends Settings {
    override val entries = nentries
  }

  import impls._
  import Implicits._

  def createSequenceHandler(
                             settings: Settings): (SequenceHandler[ExampleModel, ExampleDelta],
    Repository[ExampleModel, ExampleDelta]) = {

    implicit val _settings = settings

    implicit def append(_new: ExampleModel,
                        last: ExampleModel): (ExampleModel, ExampleDelta) =
      (_new, ExampleDelta(_new.field1 - last.field1))

    implicit val ec = as.dispatcher
    implicit val col = "test_collection"

    implicit def toMonoid = append _ lift

    implicit val _lens = lens[ExampleModel] >> 'id

    implicit val repo = new Repository[ExampleModel, ExampleDelta]()
    (Await.result(SequenceHandler[ExampleModel, ExampleDelta], 50 seconds),
      repo)

  }

  "A Stream in training mode" must {

    "read events from the standard input and put in the ouptut stream the enriched events" in {

      val settings2 = new FakeSettings(1)
      val seq = createSequenceHandler(new FakeSettings(1))
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
        Source(List("id1,1,1,1,1,1", "id2,1,2,2,2,0", "id3,1,3,3,3,1", "id4,1,4,4,4,0", "id5,1,5,5,5,1"))

      implicit def sortBy(_new: ExampleModel): String = _new.id

      val sinkStream = as.actorOf(
        Props(new SinkActorRunner[ExampleModel](
          settings2,
          List(createSequenceHandler(settings2)._1),
          Implicits.partialfuncRunner,
          taskSupervisor,
          io)))

      new StreamRunner[ExampleModel](settings2, sinkStream, stdinSource)

      Thread.sleep(20000)
      messages.size shouldBe 5
    }
  }

}