package com.bbvalabs.ai

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, WordSpecLike}
import shapeless._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocumentHandler, derived}

import scala.concurrent.Await
import scala.concurrent.duration._
import scalaz.concurrent.Task

@RunWith(classOf[JUnitRunner])
class MongoRepoSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll with MongoEmbedDatabase {

  case class ExampleModel(id: String, field1: Int, field2: Int) extends Model
  case class ExampleDelta(delta: Int) extends DeltaType

  object implicits {
    implicit val tran1 = CSVConverter[ExampleModel]
    implicit val codec1: BSONDocumentHandler[ExampleModel] =
      derived.codec[ExampleModel]
    implicit val codec2: BSONDocumentHandler[ExampleDelta] =
      derived.codec[ExampleDelta]
    implicit val _lens = lens[ExampleModel] >> 'id
    implicit val model: String = "example"
    implicit val init = ExampleDelta(0)
  }

  override def beforeAll(): Unit = {
    mongoStart()
  }

  private def createNewSequence(
      id: String): Task[Sequence[ExampleModel, ExampleDelta]] = {

    import Implicits._, implicits._
    val repo = new Repository[ExampleModel, ExampleDelta]()

    implicit val col =
      Await.result[BSONCollection](repo.collection, 3600 seconds)

    val seq = AnySequence(
      id,
      List(DeltaModel2(ExampleModel(id, 1, 1), ExampleDelta(1))))

    repo.save(seq)
  }

  "A Mongo implementation giving a Model and a Delta" should "add a new sequence" in {

    import Implicits._, implicits._

    val repo = new Repository[ExampleModel, ExampleDelta]()

    implicit val col =
      Await.result[BSONCollection](repo.collection, 3600 seconds)

    val result = createNewSequence("id").flatMap {
      case AnySequence(a, b) => {
        repo.find(ExampleModel("id", 1, 1))
      }
      case NoSequence => {
        Task {
          NoSequence
        }
      }
    }

    val sequence = result.unsafePerformSyncFor(5 seconds)
    // Result should be a AnySequence
    sequence shouldBe a[AnySequence[_, _]]
  }

  "A Mongo implementation giving a Model and a Delta" should "update an existing sequence" in {

    import Implicits._, implicits._
    val repo = new Repository[ExampleModel, ExampleDelta]()

    implicit val col =
      Await.result[BSONCollection](repo.collection, 3600 seconds)

    val result = createNewSequence("id2").flatMap {
      case s: AnySequence[ExampleModel, ExampleDelta] => {
        repo.update(
          ExampleModel("id2", 2, 2),
          s.copy(
            deltas = s.deltas ++ List(
              DeltaModel2(ExampleModel("id2", 2, 2), ExampleDelta(2)))))
      }
      case NoSequence => {
        Task {
          NoSequence
        }
      }
    }
    val sequence = result.unsafePerformSyncFor(5 seconds)
    sequence shouldBe a[AnySequence[_, _]]

    sequence
      .asInstanceOf[AnySequence[ExampleModel, ExampleDelta]]
      .deltas
      .length should be(2)
  }
}
