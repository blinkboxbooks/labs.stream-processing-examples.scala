package com.blinkbox.books.streams

import akka.actor.{ Actor, ActorRef, ActorLogging, ActorSystem, Props }
import akka.actor.Status.{ Success, Failure }
import akka.event.LoggingReceive
import akka.util.Timeout
import java.util.concurrent.TimeoutException
import pl.project13.scala.rainbow.Rainbow._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import PipelineActor._
import Services._
import java.io.IOException
import scala.concurrent.Await

/**
 * A pipeline of message processing, implemented using Akka Actors.
 *
 * This initial version works, but has lots of duplicated code as there's been
 * no attempt to factor out common parts - yet!
 */
object AkkaEnrichmentPipeline extends App with MessageProcessor {

  // TODO: Think about timeouts at the various levels.

  implicit val system = ActorSystem("messsage-pipeline")
  implicit val ec = system.dispatcher

  // Create and wire up the various actors.
  val errorHandler = system.actorOf(Props(new ErrorHandler(invalidMsgHandler)), name = "error-handler")
  val outputWriter = system.actorOf(Props(new OutputWriter(output)), name = "output-writer")
  val transformer = system.actorOf(Props(new TransformerActor(outputTransformer, outputWriter)), name = "output-transformer")
  val reverser = system.actorOf(Props(new ReverserActor(enricher1, transformer)), name = "reverser")
  val upperCaser = system.actorOf(Props(new UpperCaserActor(enricher2, transformer)), name = "upper-caser")
  val sorter = system.actorOf(Props(new SorterActor(enricher3, transformer)), name = "sorter")
  val enricher = system.actorOf(Props(new Enricher(reverser, upperCaser, sorter, transformer)), name = "enricher")
  val source = system.actorOf(Props(new Source(input, enricher, errorHandler)), name = "source")

  // Start generating input messages.
  system.scheduler.schedule(0.seconds, 2.second, source, Ping)

  // Wait around.
  Console.readLine()

  // Messages.
  case object Ping
  case class Reversed(result: String)
  case class Sorted(result: Widget)
  case class UpperCased(result: String)

  /**
   * An actor that just sends a new request every time it's triggered by a message,
   * and acknowledges completions to some external entity, its input (i.e. as one would with RabbitMQ).
   *
   * In this example, this is the head of the pipeline.
   */
  class Source(input: Input, output: ActorRef, errorHandler: ActorRef) extends Actor with ActorLogging {

    import Source._

    var tick = 0L

    def receive = {
      case Ping =>
        tick += 1
        val data = Data(tick, s"Input Data: ${tick.toString}")
        val responseHandler = context.actorOf(responseHandlerProps(input, data, errorHandler))
        output.tell(Process(data), responseHandler)
        log.debug(s"Sent input message number $tick")
    }
  }

  object Source {

    def responseHandlerProps(input: Input, data: Data, errorHandler: ActorRef) =
      Props(new ResponseHandler(input, data, errorHandler))

    private class ResponseHandler(input: Input, data: Data, errorHandler: ActorRef) extends BaseResponseHandler {
      override def onSuccess() = input.ack(data.id)
      override def onError() = errorHandler ! data
      override def onNotProcessed() = input.nack(data.id)
    }

  }

  /**
   * Simple example actor that performs storage of messages that failed with an unrecoverable error, for handling
   * outside of this process. In real life this might be a RabbitMQ DLQ, an error log, or similar.
   */
  class ErrorHandler(handler: InvalidMessageHandler) extends Actor with ActorLogging {
    def receive = {
      case message: Data =>
        log.error(s"Invalid message handled: $message")
        handler.invalid(message)
    }
  }

  /**
   * Actor that takes input messages, enriches them via helper services, and forwards the result if successful.
   * In case of an input that can't be handled, it will respond with a Failure to the sender.
   *
   * See "Cameo Pattern" and "Capturing Context" sections in Effective Akka book:
   * http://my.safaribooksonline.com/book/programming/scala/9781449360061/2dot-patterns-of-actor-usage/cameo_pattern_html
   */
  class Enricher(reverser: ActorRef, upperCaser: ActorRef, sorter: ActorRef, output: ActorRef)
    extends Actor with ActorLogging {

    import Enricher._

    def receive = LoggingReceive {
      case Process(input: Data) =>
        // Delegate the handling of results for this message to a short-lived actor.
        val handler = context.actorOf(enrichmentHandlerProps(input, sender, output))
        reverser.tell(Process(input.value), handler)
        upperCaser.tell(Process(input.value), handler)
        sorter.tell(Process(input.value), handler)
      case msg => log.warning(s"Unexpected message: $msg")
    }

  }

  object Enricher {

    def enrichmentHandlerProps(input: Data, originator: ActorRef, output: ActorRef) =
      Props(new EnrichmentHandler(input, originator, output))

    /**
     *  Simple actor that merges enrichment data then forwards result when complete.
     *  On failure, it sends this back to the sender of the original message that triggered it,
     *  i.e. the originator.
     */
    private class EnrichmentHandler(input: Data, originator: ActorRef, output: ActorRef) extends Actor with ActorLogging {

      var reversed: Option[String] = None
      var upperCased: Option[String] = None
      var sorted: Option[Widget] = None

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
        case msg => log.warning(s"Unexpected message: $msg")
      }

      private def checkIsDone() {
        (reversed, upperCased, sorted) match {
          case (Some(r), Some(u), Some(s)) =>
            output.tell(Process(EnrichedData(input, r, u, s)), originator)
            log.debug("Stopping enrichment handler after success")
            context.stop(self)
          case _ =>
            log.debug("Waiting for more results")
        }
      }

    }
  }

  /** Common settings for all pipeline actors in this example. */
  trait ExamplePipelineActor extends PipelineActor {
    def isTemporaryFailure(e: Throwable): Boolean = e.isInstanceOf[IOException] || e.isInstanceOf[TimeoutException]
    def retryInterval = 10.seconds
    def timeout = 1.minute
  }

  class ReverserActor(reverser: Reverser, output: ActorRef) extends ExamplePipelineActor with Requester {
    override def process = {
      case Process(input: String) =>
        Reversed(Await.result(reverser.transform(input), retryInterval))
    }
  }

  class SorterActor(sorter: Sorter, output: ActorRef) extends ExamplePipelineActor with Requester {
    override def process = {
      case Process(input: String) =>
        Sorted(Await.result(sorter.transform(input), retryInterval))
    }
  }

  class UpperCaserActor(upperCaser: UpperCaser, output: ActorRef) extends ExamplePipelineActor with Requester {
    override def process = {
      case Process(input: String) =>
        UpperCased(Await.result(upperCaser.transform(input), retryInterval))
    }
  }

  class TransformerActor(transformerService: DataTransformer, override val output: ActorRef)
    extends ExamplePipelineActor with Transformer {
    override def process = {
      case Process(input: EnrichedData) =>
        Process(Await.result(transformerService.transform(input), retryInterval))
    }
  }

  class OutputWriter(out: Output) extends ExamplePipelineActor with Sink {

    override def preStart() = println("Let's pretend I'm opening my database connection here")
    override def postStop() = println("Let's pretend I'm closing my database connection here")

    override def process = {
      case Process(input: OutputData) => out.save(input)
    }
  }

}

