package com.bbvalabs.ai

import reactivemongo.bson.derived
import scala.util.Try

object Sequencer {
  type OpReturn = Try[Boolean]
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

trait DeltaType {
  def unit : DeltaType
}

object DeltaType {
  def unit : DeltaType = DeltaType.unit
}

trait Model { self =>
  val id: String
}

object Model {
  def apply(implicit instance: Model) : Model = instance
  def init_delta[A <: Model, B <: DeltaType](in: A) (f: A => B) : B = f(in)
  def ~>>>[A <: Model, B <: DeltaType] = (model: A) => (delta: DeltaType) =>  {
    DeltaModel2(model, delta)
  }
}