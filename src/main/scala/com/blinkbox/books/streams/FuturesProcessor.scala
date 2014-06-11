package com.blinkbox.books.streams

import scala.concurrent.Future
import scala.concurrent.duration._
import Services._
import akka.actor.{ Actor, ActorRef, ActorLogging, ActorSystem, Props }
import pl.project13.scala.rainbow.Rainbow._
import scala.util.{ Success, Failure }
import java.util.concurrent.TimeoutException
import java.io.IOException

object FuturesProcessor extends App with MessageProcessor {

  implicit val system = ActorSystem("messsage-pipeline")
  implicit val ec = system.dispatcher

  // Create the input actor.
  val messageHandler = system.actorOf(Props(
    new MessageHandler(input, output, enricher1, enricher2, enricher3, outputTransformer, invalidMsgHandler)),
    name = "message-handler")

  // Generate input messages.
  val source = system.actorOf(Props(new Source(messageHandler)))
  system.scheduler.schedule(0.seconds, 2.second, source, Ping)
  case object Ping

  /** An actor that generates input messages. In a real system, these would come from a queue instead. */
  class Source(receiver: ActorRef) extends Actor with ActorLogging {

    var tick = 0L

    def receive = {
      case Ping =>
        tick += 1
        receiver ! Data(tick, s"Input Data: ${tick.toString}")
        log.debug(s"Sent input message number $tick")
    }
  }

  // Wait around.
  Console.readLine()

  class MessageHandler(input: Input, output: Output,
    reverser: Reverser, upperCaser: UpperCaser, sorter: Sorter,
    transformer: DataTransformer, errorHandler: InvalidMessageHandler)
    extends Actor with ActorLogging {

    val retryInterval = 5.seconds //.minutes

    def receive = {
      case data: Data =>
        val futureReversed = reverser.transform(data.value)
        val futureUppercased = upperCaser.transform(data.value)
        val futureSorted = sorter.transform(data.value)
        val result = for (
          reversed <- futureReversed;
          upperCased <- futureUppercased;
          sorted <- futureSorted;
          enriched = EnrichedData(data, reversed, upperCased, sorted);
          outputData <- transformer.transform(enriched);
          result <- Future(output.save(outputData).get)
        ) yield result

        result.onComplete {
          case Success(_) => input.ack(data.id)
          case Failure(e) if isTemporaryFailure(e) => retry(data)
          case Failure(e) => errorHandler.invalid(data)
        }
    }

    /** Example classification of errors into temporary vs. */
    def isTemporaryFailure(e: Throwable) = e.isInstanceOf[IOException] || e.isInstanceOf[TimeoutException]

    /** Try again in a while. */
    def retry(input: Data) = context.system.scheduler.scheduleOnce(retryInterval, self, input)
  }

}
