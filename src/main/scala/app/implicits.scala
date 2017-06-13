package com.bbvalabs.ai.sequence.app

import akka.actor.ActorSystem
import com.bbvalabs.ai._
import reactivemongo.bson.derived

/** DO NOT CHANGE THIS!! unless you know what you are doing.
  *
  *  @author  Emiliano Martinez
  *  @date  02/06/2017
  */
object implicits {

  import Implicits._

  implicit val modelConverter = CSVConverter[Model1]

  implicit val c1 = derived.codec[Delta1]
  implicit val c2 = derived.codec[Model1]
  implicit val c3 =
    derived.codec[DeltaModel2[Model1, Delta1]]
  implicit val c4 = derived.codec[AnySequence[Model1, Delta1]]

}
