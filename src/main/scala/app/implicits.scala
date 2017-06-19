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

  implicit val modelConverter1 = CSVConverter[Model1]

  implicit val c1 = derived.codec[Delta1]
  implicit val c2 = derived.codec[Model1]

}
