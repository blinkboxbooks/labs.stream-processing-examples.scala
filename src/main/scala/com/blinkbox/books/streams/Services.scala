package com.blinkbox.books.streams

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random
import scala.util.{ Try, Success, Failure }

/**
 * Shared services and utilities for the message processing examples.
 */
object Services {

  // Values used in randomising behaviour.
  val random = new Random
  val maxWaitTime = 5000
  val failurePercentage = 2

  /** An input message. */
  case class Data(id: Long, value: String)

  /** Some intermediate type */
  case class Widget(name: String)

  /** Enriched input data. */
  case class EnrichedData(input: Data, extra1: String, extra2: String, extra3: Widget)

  /**
   *  Common interface for services that return results for requests as Futures that complete after a random delay,
   */
  trait Transformer {
    def transform(value: String): Future[String] =
      sometimesInFuture(s"$tag transforming value $value")(doTransform(value))

    protected def doTransform(value: String): String
    private def tag = getClass.getSimpleName
  }

  class Reverser extends Transformer {
    override def doTransform(value: String) = value.reverse
  }

  class UpperCaser extends Transformer {
    override def doTransform(value: String) = value.toUpperCase
  }

  /** A different type of transformer that doesn't return the same type as the others. */
  class Sorter {
    def transform(value: String): Future[Widget] =
      sometimesInFuture(s"Sorter transforming value $value")(Widget(value.sorted))
  }

  /**
   * A thing that nominally produces the data, to which we need to ack or nack messages.
   */
  class Input {
    def ack(id: Long) = { println(s"Acked message $id") }
    def nack(id: Long) = { println(s"Nacked message $id") }
  }

  /**
   * A thing we can write results to, emulating something like a database, a search index
   * or other data store. I.e. a side-effecting operation that runs synchronously.
   *
   * This sometimes fails too. A failure here should be interpreted as invalid input data.
   * Failures to write to the output such as a DB should cause retries,
   * hence wouldn't come out of this class as an error.
   */
  class Output {
    def save(data: EnrichedData): Try[EnrichedData] = sometimes("output") {
      println(s"Saved $data")
      data
    }
  }

  /**
   * A service that records messages that couldn't be processed.
   */
  class InvalidMessageHandler {
    def invalid(msg: Data) = { println(s"Handled error for message: $msg") }
  }

  /**
   * Make operations intermittently fail.
   */
  def sometimes[T](tag: String)(func: => T): Try[T] =
    if (random.nextInt(100) > failurePercentage)
      Success(func)
    else
      Failure(new Exception(s"Random failure from '$tag'!"))

  /**
   * Make operations take a random amount of time and intermittently fail.
   */
  def sometimesInFuture[T](tag: String)(func: => T): Future[T] = Future {
    println(s"Starting job for $tag")
    Thread.sleep(random.nextInt(maxWaitTime))
    println(s"Completed job for $tag")
    sometimes(tag)(func).get
  }

}
