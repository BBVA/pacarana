package com.bbvalabs.ai.test

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{Graph, SourceShape}
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import com.bbvalabs.ai.Implicits.{SimpleDelta, WithOrdering}
import com.bbvalabs.ai.{Model, MongoEmbedDatabase}

/**
  * Created by e049627 on 21/6/17.
  */

/*
case class Model1(id: String, a: Int, b: Int, c: Int, timestamp: Long, label: Option[Int])
  extends Model
case class Delta1(diff1: Long)

class RunnerStreamSuite(_system: ActorSystem)
    extends TestKit(_system)
    with MongoEmbedDatabase
      with CommonFilterAndLabel[Model1]
      with SimpleDelta[Model1, Delta1] {

  import com.bbvalabs.ai.runtime.StreamOps._

  /**
    * This function drops the fields from the output. These fields must be Option
    *
    * @param _new input object
    * @return one copy of the input object with the fields to None to drop fields
    */
  override def ignore(_new: Model1): Option[Model1] = None

  /**
    * Create the label from the new object. Result must be a string to append to the output sequence
    *
    * @param _new input object
    * @return the string which will be used as label
    */
  override def label(_new: Model1): String = _new.label.toString

  /**
    * Function to create one delta from the last record in the sequence
    *
    * @param _new new document
    * @param last last stored document
    * @return a Tuple2 with the new object and the new delta
    */
  override def append(_new: Model1, last: Model1): (Model1, Delta1) = (_new, Delta1(_new.timestamp - last.timestamp))


  val sourceGraph: Graph[SourceShape[String], NotUsed] = new StdinSourceStage

  val stdinSource: Source[String, NotUsed] =
    Source.fromGraph(sourceGraph).async

  val sourceRunner = getSource[(String, Model1)](stdinSource)


}*/
