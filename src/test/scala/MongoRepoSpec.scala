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

package com.bbva.pacarana.tests

import com.bbva.pacarana.model._
import com.bbva.pacarana.parser.CSVConverter
import com.bbva.pacarana.repository.Repository
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, WordSpecLike}
import shapeless._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocumentHandler, derived}

import scala.concurrent.Await
import scala.concurrent.duration._
import scalaz.concurrent.Task

import com.bbva.pacarana.Implicits._

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

    import com.bbva.pacarana.Implicits._, implicits._
    val repo = new Repository[ExampleModel, ExampleDelta]()

    implicit val col =
      Await.result[BSONCollection](repo.collection, 3600 seconds)

    val seq = AnySequence(
      id,
      List(DeltaModel2(ExampleModel(id, 1, 1), ExampleDelta(1))))

    repo.save(seq)
  }

  "A Mongo implementation giving a Model and a Delta" should "add a new sequence" in {

    import implicits._

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

    import com.bbva.pacarana.Implicits._, implicits._
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
