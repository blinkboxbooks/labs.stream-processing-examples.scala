package com.blinkbox.books.streams

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random
import scala.util.{ Try, Success, Failure }
import pl.project13.scala.rainbow.Rainbow._
import java.io.IOException

/**
 * Shared services and utilities for the message processing examples.
 */
object Services {

  // Values used in randomising behaviour.
  val random = new Random
  val maxWaitTime = 5000

  // Percentage of requests that fail.
  val failurePercentage = 20

  // Percentage of requests that are temporary failures (throw IOException).
  val temporaryFailurePercentage = 50

  /** An input message. */
  case class Data(id: Long, value: String)

  /** Some intermediate type */
  case class Widget(name: String)

  /** Enriched input data. */
  case class EnrichedData(input: Data, extra1: String, extra2: String, extra3: Widget)

  /** Yet another intermediate type. */
  case class OutputData(data: EnrichedData, additional: String)

  /**
   *  Common interface for services that return results for requests as Futures that complete after a random delay,
   */
  trait Transformer {
    def transform(value: String): Future[String] =
      sometimesInFuture(s"${getClass.getSimpleName} transforming value $value")(doTransform(value))

    protected def doTransform(value: String): String
  }

  class Reverser extends Transformer {
    override def doTransform(value: String) = value.reverse
  }

  class UpperCaser extends Transformer {
    override def doTransform(value: String) = value.toUpperCase
  }

  /** A type of enricher that doesn't return the same type as the others. */
  class Sorter {
    def transform(value: String): Future[Widget] =
      sometimesInFuture(s"Sorter transforming value $value")(Widget(value.sorted))
  }

  /** A type of transformer that takes a more complex input type. */
  class DataTransformer {
    def transform(data: EnrichedData): Future[OutputData] =
      sometimesInFuture(getClass.getSimpleName)(OutputData(data, s"transformed ${data.input.id}"))
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
    def save(data: OutputData): Try[OutputData] = sometimes("output") {
      println(s"Saved $data".green)
      data
    }
  }

  /**
   * A service that records messages that couldn't be processed.
   */
  class InvalidMessageHandler {
    def invalid(msg: Data) = { println(s"Handled error for message: $msg".red) }
  }

  /**
   * Make operations intermittently fail.
   */
  def sometimes[T](tag: String)(func: => T): Try[T] =
    if (random.nextInt(100) > failurePercentage)
      Success(func)
    else {
      Failure(exception(s"Random failure from '$tag'!".yellow))
    }

  private def exception(msg: String) =
    if (random.nextInt(100) < temporaryFailurePercentage)
      new IOException(msg)
    else
      new Exception(msg)

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
