/*
 * Copyright (c) 2014 Mario Pastorelli (pastorelli.mario@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shapeless.examples

import java.text.SimpleDateFormat
import java.util.Date

import com.bbva.pacarana.model.Model
import shapeless._

import scala.collection.immutable.{:: => Cons}
import scala.util.{Failure, Success, Try}

/** Exception to throw if something goes wrong during CSV parsing */
class CSVException(s: String) extends RuntimeException

/** Trait for types that can be serialized to/deserialized from CSV */
trait CSVConverter[T] {
  def from(s: String): Try[T]
  def to(t: T): String
}

object CSVConverter {
  def apply[T](implicit st: Lazy[CSVConverter[T]]): CSVConverter[T] = st.value

  def fail(s: String) = Failure(new CSVException(s))

  implicit def stringCSVConverter: CSVConverter[String] =
    new CSVConverter[String] {
      def from(s: String): Try[String] = Success(s)

      def to(s: String): String = s
    }

  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  implicit def dateCSVConverter : CSVConverter[Date] =
    new CSVConverter[Date] {
      def from(s: String): Try[Date] = Success(sdf.parse(s))
      def to(s: Date): String = s.toString
    }

  implicit def intCsvConverter: CSVConverter[Int] = new CSVConverter[Int] {
    def from(s: String): Try[Int] = { Try(s.toInt) }

    def to(i: Int): String = i.toString
  }

  implicit def optionConverter[A](
      implicit ec: CSVConverter[A]): CSVConverter[Option[A]] = {
    new CSVConverter[Option[A]] {
      def from(s: String): Try[Option[A]] = {
        if (s == null || s.isEmpty) Success(None)
        else {
          ec.from(s) match {
            case Success(e) => { Success(Some(e)) }
            case _ => { Success(None) }
          }
        }
      }
      def to(l: Option[A]): String = {
        l match {
          case Some(a) => l.map(ec.to).mkString("\n")
          case None => ""
        }
      }
    }
  }

  implicit def doubleCsvConverter: CSVConverter[Double] =
    new CSVConverter[Double] {
      def from(s: String): Try[Double] = Try(s.toDouble)

      def to(i: Double): String = i.toString
    }

  // Add support for Long type
  implicit def longCsvConverter: CSVConverter[Long] =
    new CSVConverter[Long] {
      def from(s: String): Try[Long] = Try(s.toLong)

      def to(i: Long): String = i.toString
    }

  def listCsvLinesConverter[A](l: List[String])(
      implicit ec: CSVConverter[A]): Try[List[A]] = l match {
    case Nil => Success(Nil)
    case Cons(s, ss) =>
      for {
        x <- ec.from(s)
        xs <- listCsvLinesConverter(ss)(ec)
      } yield Cons(x, xs)
  }

  implicit def listCsvConverter[A](
      implicit ec: CSVConverter[A]): CSVConverter[List[A]] =
    new CSVConverter[List[A]] {
      def from(s: String): Try[List[A]] =
        listCsvLinesConverter(s.split("\n").toList)(ec)

      def to(l: List[A]): String = l.map(ec.to).mkString("\n")
    }

  /* Runner mode to extract first field as an hypothetical event preceded by an id*/
  implicit def runnerModelConverter[A, B <: Model](
      implicit c1: CSVConverter[A],
      c2: CSVConverter[B]): CSVConverter[(A, B)] = {
    new CSVConverter[(A, B)] {
      override def from(s: String): Try[(A, B)] = {
        val result = s.split(",")
        val head = c1.from(result(0))
        val tail = result.drop(1)
        val tailStr = tail.foldLeft("")(_ ++ "," ++_).drop(1)
        Try(head.get, c2.from(tailStr).get)
      }

      override def to(t: (A, B)): String = {
        s"${c2.to(t._2)}${c1.to(t._1)}"
      }
    }
  }

  implicit def deriveHNil: CSVConverter[HNil] =
    new CSVConverter[HNil] {
      def from(s: String): Try[HNil] = s match {
        case "" => Success(HNil)
        case s => fail("Cannot convert '" ++ s ++ "' to HNil")
      }

      def to(n: HNil) = ""
    }

  implicit def deriveHList[V, T <: HList](
      implicit scv: Lazy[CSVConverter[V]],
      sct: Lazy[CSVConverter[T]]): CSVConverter[V :: T] =
    new CSVConverter[V :: T] {

      def from(s: String): Try[V :: T] = s.span(_ != ',') match {
        case (before, after) =>
          for {
            front <- scv.value.from(before)
            back <- sct.value.from(if (after.isEmpty) after else after.tail)
          } yield front :: back

        case _ => fail("Cannot convert '" ++ s ++ "' to HList")
      }

      def to(ft: V :: T): String = {
        scv.value.to(ft.head) ++ "," ++ sct.value.to(ft.tail)
      }
    }

  implicit def deriveHListOption[V, T <: HList](
      implicit scv: Lazy[CSVConverter[V]],
      sct: Lazy[CSVConverter[T]]): CSVConverter[Option[V] :: T] =
    new CSVConverter[Option[V] :: T] {

      def from(s: String): Try[Option[V] :: T] = s.span(_ != ',') match {
        case (before, after) =>
          (for {
            front <- scv.value.from(before)
            back <- sct.value.from(if (after.isEmpty) after else after.tail)
          } yield Some(front) :: back).orElse {
            sct.value.from(s).map(None :: _)
          }

        case _ => fail("Cannot convert '" ++ s ++ "' to HList")
      }

      def to(ft: Option[V] :: T): String = {
        ft.head.map(scv.value.to(_) ++ ",").getOrElse("") ++ sct.value.to(
          ft.tail)
      }
    }

  implicit def deriveClass[A, R](implicit gen: Generic.Aux[A, R],
                                 conv: CSVConverter[R]): CSVConverter[A] =
    new CSVConverter[A] {

      def from(s: String): Try[A] = conv.from(s).map(gen.from)

      def to(a: A): String = conv.to(gen.to(a))
    }
}
