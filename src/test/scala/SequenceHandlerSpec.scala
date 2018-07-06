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

import akka.actor.ActorSystem
import com.bbva.pacarana.model.{AnySequence, DeltaModel2, DeltaType, Model}
import com.bbva.pacarana.parser.CSVConverter
import com.bbva.pacarana.repository.Repository
import com.bbva.pacarana.runtime.SequenceHandler
import com.bbva.pacarana.settings.Settings

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import reactivemongo.bson.{BSONDocumentHandler, derived}
import shapeless.{Lens, lens}

import scala.concurrent.duration._
import scala.concurrent.Await

case class ExampleModel(id: String, id2: String, field1: Int, field2: Int, label: Int)
    extends Model
case class ExampleDelta(delta: Int) extends DeltaType

case class ExampleModel2(id: String, id2: String, field1: Int, field2: Int, label: Int)
  extends Model

object impls {
  implicit val tran1 = CSVConverter[ExampleModel]

  implicit val tran2 = CSVConverter[ExampleModel2]

  implicit val codec0: BSONDocumentHandler[ExampleModel] =
    derived.codec[ExampleModel]

  implicit val codec1: BSONDocumentHandler[ExampleModel2] =
    derived.codec[ExampleModel2]

  implicit val codec2: BSONDocumentHandler[ExampleDelta] =
    derived.codec[ExampleDelta]

  implicit val init = ExampleDelta(0)

  implicit def output(_new: (ExampleModel, ExampleDelta)): String =
    s"${_new._1.field1},${_new._1.field2},${_new._2.delta}"
}

class SequenceHandlerSpec
    extends FlatSpec
    with Matchers {

  def createSequenceHandler(lns: Lens[ExampleModel, String]): (SequenceHandler[ExampleModel, ExampleDelta],
    Repository[ExampleModel, ExampleDelta]) = {
    import impls._
    import com.bbva.pacarana.Implicits._

    implicit val settings = new Settings
    implicit def append(_new: ExampleModel,
                        last: ExampleModel): (ExampleModel, ExampleDelta) =
      (_new, ExampleDelta(_new.field1 - last.field1))

    implicit val as = ActorSystem("test")
    implicit val ec = as.dispatcher
    implicit val col = "test_collection"

    implicit val _lens = lns

    implicit def toMonoid = append _ lift

    implicit val repo = new Repository[ExampleModel, ExampleDelta]()
    (Await.result(SequenceHandler[ExampleModel, ExampleDelta], 50 seconds),
      repo)

  }


  "A SequenceHandler implementation giving a Model and a Delta" should "add a new sequence" in {

    val model = ExampleModel("id", "id2", 1, 2, 1)
    val delta = ExampleDelta(0)


    val (handler, repo) = createSequenceHandler(lens[ExampleModel] >> 'id)

    val r0 = handler.insertSequence(
      AnySequence("id", List(DeltaModel2(model, delta))))(repo)
    val r1 = r0.unsafePerformSyncFor(5 seconds)
    r1 shouldBe a[AnySequence[_, _]]

    val sequence = handler.get(model)(repo).unsafePerformSyncFor(5 seconds)
    sequence
      .asInstanceOf[AnySequence[ExampleModel, ExampleDelta]]
      .deltas
      .length should be(1)
  }


  "A SequenceHandler implementation giving a Model and a Delta" should "process a bunch of events and store their sequences by a field" in {

    val models = List(ExampleModel("id02", "id02", 1, 1, 1),
      ExampleModel("id02", "id02", 2, 2, 0),
      ExampleModel("id02", "id02", 3, 3, 1),
      ExampleModel("id02", "id02", 4, 4, 0),
      ExampleModel("id02", "id02", 5, 5, 1))

    val (handler, repo) = createSequenceHandler(lens[ExampleModel] >> 'id)
    val (result, f) = handler.processBatchinOneModel(models, 0, Nil).unsafePerformSyncFor(5 seconds)

    // Chek Sequence length
    result match {
      case (n, list) => {
        list.length shouldBe(5)
        for ((seq, count) <- list.zipWithIndex) {
          seq.size shouldBe (count + 1)
          if(seq.size == 1) seq(0).delta.delta shouldBe 0
          else
            seq(0).delta.delta shouldBe 1
        }
      }
    }
  }

  "A SequenceHandler implementation giving a Model and a Delta" should "process a bunch of tupled events and store their sequences by a field" in {

    val models = List(1,2,3,4,5).map(_.toString) zip List(ExampleModel("id003", "id003", 1, 1, 1),
      ExampleModel("id003", "id003", 2, 2, 0),
      ExampleModel("id003", "id003", 3, 3, 1),
      ExampleModel("id003", "id003", 4, 4, 0),
      ExampleModel("id003", "id003", 5, 5, 1))

    val (handler, repo) = createSequenceHandler(lens[ExampleModel] >> 'id)
    val (result, f) = handler.processBatchinOneModelTupled(models, 0, Nil).unsafePerformSyncFor(5 seconds)

    // Chek Sequence length
    result match {
      case (n, list) => {
        list.length shouldBe(5)
        for ((seq, count) <- list.zipWithIndex) {
          seq.size shouldBe (count + 1)
          seq(0)._1.toInt shouldBe (count + 1)
          if(seq.size == 1) seq(0)._2.delta.delta shouldBe 0
          else
            seq(0)._2.delta.delta shouldBe 1
        }
      }
    }
  }

}
