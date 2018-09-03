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

package com.bbva.pacarana.repository

import com.bbva.pacarana.model._
import com.bbva.pacarana.mongo.conf.MongoConf
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocument, BSONDocumentHandler}

import scalaz.concurrent.Task
import reactivemongo.bson._
import shapeless.Lens

import scala.concurrent.{ExecutionContext, Future}

class Repository[A <: Model, B <: DeltaType](
    implicit val lift: BSONDocumentHandler[AnySequence[A, B]],
    ec: ExecutionContext,
    model: String) {

  import delorean._

  val db = MongoConf.db
  val collection: Future[BSONCollection] = db.map(_.collection(model))

  /**
    * Finds a sequence in mongo by _id parameter
    * @param in model whose sequence is going to be changed.
    * @return the sequence if it exists otherwise NoSequence is returned.
    */
  def find(in: A)(implicit col: BSONCollection, lens: Lens[A, String]): Task[Sequence[A, B]] =
    col
      //.find(document("_id" -> in.id))
      .find(document("_id" -> lens.get(in)))
      .one[BSONDocument]
      .map(_ match {
        case Some(data) =>
          lift.read(data)
        case _ => NoSequence
      })
      .toTask

  /**
    * Saves a new sequence in DDBB. At the moment it returns the input sequence.
    * TODO: Add error handling if necessary.
    * @param in New sequence to be inserted.
    * @return the new sequence.
    */
  def save(in: AnySequence[A, B])(
      implicit col: BSONCollection): Task[Sequence[A, B]] = {
    col
      .insert[BSONDocument](lift.write(in))
      .recover {
        case e: Throwable => { NoSequence }
        case _ => NoSequence
      }
  }.toTask.map(_ => in)

  /**
    * Updates an existing sequence with the new Delta
    * @param in model whose sequence is going to be changed.
    * @param seq the new updated sequence
    * @return the updated sequence
    */
  def update(in: A, seq: AnySequence[A, B])(
      implicit col: BSONCollection, lens: Lens[A, String]): Task[Sequence[A, B]] = {
    val selector = document("_id" -> lens.get(in))
    val upd = col
      .update[BSONDocument, BSONDocument](selector, lift.write(seq))
      .recover {
        case e: Throwable => NoSequence
        case _ => NoSequence
      }
    upd
  }.toTask.map(_ => seq)

}
