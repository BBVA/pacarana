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

package com.bbva.pacarana.model

object SequencerTypes {
  type DataForTrain[A] = List[(Int, List[A])]
  type DataForRun[A] = List[(Int, List[(String, A)])]
}

sealed trait Sequence[+A, +B]

case class AnySequence[A, B](_id: String, deltas: List[DeltaModel2[A, B]])
    extends Sequence[A, B]

case object NoSequence extends Sequence[Nothing, Nothing]

object Sequence {
  def unit[A, B] = AnySequence("", Nil)
}

case class DeltaModel2[+A, +B](model: A, delta: B)

object DeltaModel2 {
  def apply[A](m: A) : DeltaModel2[A, _ <: DeltaType] = DeltaModel2(m, DeltaType.unit)
}

trait DeltaType

object DeltaType {
  def unit : DeltaType = DeltaType.unit
}

trait Model { self =>
  val id: String
}
