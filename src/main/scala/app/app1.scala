package com.bbvalabs.ai.sequence.app

import com.bbvalabs.ai._

import scala.util.Success

/** Steps to create one sequence model:
  *
  * 1:-> Create the data structures
  *      Model1: Data type that represents the transaction, movement or whatever
  *      Delta1: Represents the delta, aggregation or whatever you want between movements
  *
  * 2:-> Create one val with the model name. In the code is obtained from settings tha point to the MODEL_NAME env variable
  * 3:-> Create append function. This function uses the two existing types of sequence: AnySequence and NoSequence. Use pattern matching.
  *
  *  @example {{{
  *
  *       case class Model1(id: String, attrb1: String, opTime: String)
  *       case class Delta1(interval: Double) extends DeltaType
  *
  *
  *       val instance = new SequenceHandler[TransactionInterval] {
  *          override val repo: Repository[TransactionInterval] = [ ... provide a repo implementation]
  *       }
  *
  *       val monoid = new Monoid[Sequence[TransactionInterval]] {
  *        ...
  *       }
  *
  *       instance.process(DeltaModel2(parsed.get, TransactionInterval(0)))([..provide a monoid implementation])
  *
  *
  *  @note All operations are compose with the scalaz Task type. It should be a high kinded type,
  *        however it could be more complex to understand. This api will be codified in a full functional structure
  *        in the future.
  *
  *  @author  Emiliano Martinez
  *  @date  02/06/2017
  */

case class Model1(id: String,
                  field1: Option[String],
                  field2: Int,
                  tag: String,
                  timestamp: Double)
  extends Model

case class Delta1(time: Double) extends DeltaType {
  override def unit = Delta1(0)
}

object Functions {
  import Model._
  import Implicits._

  def append(_new: Model1, last: Model1): (Model1, Delta1) =
    (_new, Delta1(_new.timestamp - last.timestamp))

  def ignore(ev: Model1) : Model1 =
    ev.copy(field1 = None)

  def label(ev: Model1) : String = {
    ev.field2.toString
  }
}

object app1 {

  def main(args: Array[String]): Unit = {
    import implicits._
    import com.bbvalabs.ai.Implicits._

    import Functions._

    implicit val _modelName: String = "seq1"
    implicit def f = append _ lift
    implicit def la = label _
    //implicit def lb = ignore _
    implicit val l: Delta1 = Delta1(0)

    val seq = SequenceHandler[Model1, Delta1]

    seq.onComplete {
      case Success(handler) => {
        SequenceHandlerStreamRunner[Model1, Delta1](handler)
      }
    }
  }

}