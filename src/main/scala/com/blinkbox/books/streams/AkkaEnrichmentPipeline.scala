package com.blinkbox.books.streams

import akka.actor.{ Actor, ActorRef, ActorLogging, ActorSystem, Props }
import akka.actor.Status.{ Success, Failure }
import akka.event.LoggingReceive
import akka.util.Timeout
import pl.project13.scala.rainbow.Rainbow._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import Services._

/**
 * A pipeline of message processing, implemented using Akka Actors.
 * 
 * This initial version works, but has lots of duplicated code as there's been 
 * no attempt to factor out common parts - yet!
 */
object AkkaEnrichmentPipeline extends App with MessageProcessor {

  // Kick things off.
  startActorSystem()

  def startActorSystem() {

    implicit val system = ActorSystem("messsage-pipeline")

    // Create and wire up the various actors.
    val errorHandler = system.actorOf(Props(new ErrorHandler(invalidMsgHandler)), name = "error-handler")
    val outputWriter = system.actorOf(Props(new OutputWriter(output)), name = "output-writer")
    val transformer = system.actorOf(Props(new Transformer(outputTransformer, outputWriter)), name = "output-transformer")
    val reverser = system.actorOf(Props(new ReverserActor(enricher1, transformer)), name = "reverser")
    val upperCaser = system.actorOf(Props(new UpperCaserActor(enricher2, transformer)), name = "upper-caser")
    val sorter = system.actorOf(Props(new SorterActor(enricher3, transformer)), name = "sorter")
    val enricher = system.actorOf(Props(new Enricher(reverser, upperCaser, sorter, transformer)), name = "enricher")
    val source = system.actorOf(Props(new Source(input, enricher, errorHandler)), name = "source")
  }

  // Wait around.
  Console.readLine()

  // Global for now.
  val retryTime = 5.seconds

  // Messages for helper services.
  case class TransformRequest(input: String)
  case class Reversed(result: String)
  case class Sorted(result: Widget)
  case class UpperCased(result: String)

  /**
   * An actor that just sends a new message every second,
   * and acknowledges completions to some external entity, its input (i.e. as one would with RabbitMQ).
   */
  class Source(input: Input, output: ActorRef, errorHandler: ActorRef)
    extends Actor with ActorLogging {

    import Source._
    import context.dispatcher
    context.system.scheduler.schedule(0.seconds, 1.second, self, Ping)

    var tick = 0L

    def receive = {
      case Ping =>
        tick += 1
        val message = Data(tick, s"Input Data: ${tick.toString}")
        val responseHandler = context.actorOf(responseHandlerProps(input, message, errorHandler))
        output.tell(message, responseHandler)
        log.debug(s"Sent input message number $tick")
    }
  }

  object Source {

    case object Ping

    // Create a helper actor that ACKs a single message when successful,
    // or passes on the input to an error handler actor when failing.
    def responseHandlerProps(input: Input, message: Data, errorHandler: ActorRef) =
      Props(new ResponseHandler(input, message, errorHandler))

    private class ResponseHandler(input: Input, message: Data, errorHandler: ActorRef) extends Actor with ActorLogging {
      def receive = {
        case _: Success => input.ack(message.id)
        case Failure(e) => errorHandler ! message // Signifies unrecoverable error.
      }
      // TODO: Could have a postStop() that calls "nack" on the message if it wasn't acked?
    }
  }

  /**
   * Actor that performs storage of messages that failed with an unrecoverable error, for handling
   * outside of this process.
   */
  class ErrorHandler(handler: InvalidMessageHandler) extends Actor with ActorLogging {
    def receive = {
      case message: Data => handler.invalid(message)
    }
  }

  /**
   * Actor that takes input messages, enriches them via helper services, and forwards the result if successful.
   * In case of an input that can't be handled, it will respond with a Failure to the sender.
   */
  class Enricher(reverser: ActorRef, upperCaser: ActorRef, sorter: ActorRef, output: ActorRef)
    extends Actor with ActorLogging {

    import Enricher._

    // TODO: Think about timeouts.
    implicit val timeout: Timeout = 24.hours

    def receive = LoggingReceive {
      case input: Data =>
        // Delegate the handling of results for this message to a short-lived actor.
        val handler = context.actorOf(enrichmentHandlerProps(input, sender, output))
        reverser.tell(TransformRequest(input.value), handler)
        upperCaser.tell(TransformRequest(input.value), handler)
        sorter.tell(TransformRequest(input.value), handler)
    }

  }

  object Enricher {

    def enrichmentHandlerProps(input: Data, originator: ActorRef, output: ActorRef) =
      Props(new EnrichmentHandler(input, originator, output))

    // Simple actor that merges enrichment data then forwards result when complete.
    private class EnrichmentHandler(input: Data, originator: ActorRef, output: ActorRef) extends Actor with ActorLogging {

      var reversed: Option[String] = None
      var upperCased: Option[String] = None
      var sorted: Option[Widget] = None

      // TODO: Consider a timeout message on this actor.
      def receive = LoggingReceive {
        case Reversed(result) =>
          reversed = Some(result)
          checkIsDone()
        case Sorted(result) =>
          sorted = Some(result)
          checkIsDone()
        case UpperCased(result) =>
          upperCased = Some(result)
          checkIsDone()
        case r: Failure =>
          originator ! r
          log.debug("Stopping enrichment handler after failure")
          context.stop(self)
      }

      private def checkIsDone() {
        (reversed, upperCased, sorted) match {
          case (Some(r), Some(u), Some(s)) =>
            output.tell(EnrichedData(input, r, u, s), originator)
            log.debug("Stopping enrichment handler after success")
            context.stop(self)
          case _ =>
            log.debug("Waiting for more results")
        }
      }

    }
  }

  class ReverserActor(reverser: Reverser, output: ActorRef) extends Actor with ActorLogging {

    import ReverserActor._
    import context.dispatcher

    def receive = LoggingReceive {
      // TODO: Should do async and retry etc.
      case TransformRequest(value) =>
        val originalSender = sender
        reverser.transform(value).onComplete({
          case scala.util.Success(result) => originalSender ! Reversed(result)
          case scala.util.Failure(e) => originalSender ! Failure(e)
        })
    }
  }

  object ReverserActor {
    // TODO!
  }

  class SorterActor(sorter: Sorter, output: ActorRef) extends Actor with ActorLogging {

    import SorterActor._
    import context.dispatcher

    def receive = {
      // TODO: Should do async and retry etc.
      case TransformRequest(value) =>
        val originalSender = sender
        sorter.transform(value).onComplete({
          case scala.util.Success(result) => originalSender ! Sorted(result)
          case scala.util.Failure(e) => originalSender ! Failure(e)
        })

    }
  }

  object SorterActor {
    // TODO!
  }

  class UpperCaserActor(upperCaser: UpperCaser, output: ActorRef) extends Actor with ActorLogging {

    import UpperCaserActor._
    import context.dispatcher

    def receive = {
      // TODO: Should do async and retry etc.
      case TransformRequest(value) =>
        val originalSender = sender
        upperCaser.transform(value).onComplete({
          case scala.util.Success(result) => originalSender ! UpperCased(result)
          case scala.util.Failure(e) => originalSender ! Failure(e)
        })

    }
  }

  object UpperCaserActor {
    // TODO!
  }

  /**
   * A simple actor that implements another transformation step of data.
   */
  class Transformer(transformerService: DataTransformer, output: ActorRef) extends Actor with ActorLogging {

    import Transformer._

    def receive = {
      case data: EnrichedData =>
//        log.warning(s"Transformer got message from $sender")
        val handler = context.actorOf(transformHandlerProps(transformerService, output, sender))
        handler.forward(data)
    }
  }

  object Transformer {

    def transformHandlerProps(transformerService: DataTransformer, output: ActorRef, originator: ActorRef) =
      Props(new TransformHandler(transformerService, output, originator))

    private class TransformHandler(transformerService: DataTransformer, output: ActorRef, originator: ActorRef)
      extends Actor with ActorLogging {

      import context.dispatcher

      var data: Option[EnrichedData] = None

      def receive = {
        case data: EnrichedData => transformerService.transform(data).onComplete({
          case scala.util.Success(outputData) =>
            output.tell(outputData, originator)
            context.stop(self)
          case scala.util.Failure(e) if isTemporaryFailure(e) => retry(data)
          case scala.util.Failure(e) =>
            originator ! Failure(e)
            context.stop(self)
        })
      }

      // Make some errors temporary (i.e. external service unavailable) and some permanent (i.e. bad input data).
      private def isTemporaryFailure(e: Throwable): Boolean = random.nextInt(100) > 20

      private def retry(data: EnrichedData) = {
        log.info(s"Retrying write for $data")
        context.system.scheduler.scheduleOnce(3.seconds, self, data)
      }
    }
  }

  /**
   * Actor that writes input messages to an output.
   * If the input is not valid, reports a failure to the sender.
   * Will cope with output being temporarily unavailable.
   */
  class OutputWriter(output: Output) extends Actor with ActorLogging {

    import OutputWriter._

    def receive = {
      // Delegate each request to an actor that does the blocking write.
      case data: OutputData =>
        val outputHandler = context.actorOf(outputHandlerProps(data, output, sender),
          name = s"output-writing-handler-${data.data.input.id}")
        outputHandler.forward(data)
    }
  }

  object OutputWriter {

    def outputHandlerProps(data: OutputData, output: Output, originator: ActorRef) =
      Props(new OutputHandler(data, output, originator))

    /**
     * Actor that handles the (blocking) job of writing to the output.
     * Will retry on temporary errors, and report Failure for non-temporary errors.
     */
    private class OutputHandler(data: OutputData, output: Output, originator: ActorRef) extends Actor with ActorLogging {

      import context.dispatcher

      def receive = {
        case _ => output.save(data) match {
          case scala.util.Success(_) =>
//            log.warning(s"Trying to send message to actor ref '$originator'")
            originator ! Success
            context.stop(self)
          case scala.util.Failure(e) if isTemporaryFailure(e) => retry()
          case scala.util.Failure(e) =>
            originator ! Failure(e)
            context.stop(self)
        }
      }

      // Make some errors temporary (i.e. external service unavailable) and some permanent (i.e. bad input data).
      private def isTemporaryFailure(e: Throwable): Boolean = random.nextInt(100) > 20

      private def retry() = {
        log.info(s"Retrying write for $data")
        context.system.scheduler.scheduleOnce(3.seconds, self, data)
      }

    }

  }
}
