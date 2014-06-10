package com.blinkbox.books.streams

import akka.actor.{ Actor, ActorLogging, ActorRef, Props, OneForOneStrategy, SupervisorStrategy }
import akka.actor.Status.{ Success, Failure }
import akka.actor.SupervisorStrategy._
import akka.util.Timeout
import scala.util.control.NonFatal
import scala.concurrent.duration._

/**
 * A collection of common actor classes that are useful in pipeline processing.
 */

/**
 * Common trait for pipeline actors, that is:
 * actors that get an input, compute some result based on it, and either pass on
 * or return the result.
 *
 * The action used to compute the result will be retried in the case of a temporary failure,
 * i.e. one that should go away over time, such as an external service or database being unavailable.
 * In the case of an unrecoverable error, typically due to bad input data, a failure will be sent back to the
 * sender of the input data.
 */
trait PipelineActor extends Actor with ActorLogging {

  import context.dispatcher
  import PipelineActor._

  implicit def timeout: Timeout
  def retryInterval: FiniteDuration

  /** Partial function to be called when processing messages, override in concrete implementations. */
  def process: PartialFunction[Any, Any]

  /** Override to define where the result will be sent on successful result. */
  def respondTo: ActorRef

  /** Override to define which errors should be considered recoverable hence would be retried. */
  def isTemporaryFailure(e: Throwable): Boolean

  // Restart children in case of temporary glitches, stop them and report failure for other errors.
  override def supervisorStrategy = OneForOneStrategy()(customDecider.orElse(defaultDecider))
  private val customDecider: SupervisorStrategy.Decider = {
    case TemporaryFailure(originator, e) => Restart
    case UnrecoverableFailure(originator, e) =>
      originator ! Failure(e)
      Stop
  }

  final def receive = {
    // Forward work requests to a dedicated child actor.
    case request: Process =>
      val outputHandler = context.actorOf(handlerProps(request, respondTo, sender))
      outputHandler.forward(request)
    case msg => log.error("Unexpected message: " + msg)
  }

  def handlerProps(request: Process, responseTo: ActorRef, originator: ActorRef) =
    Props(new PipelineRequestHandler(request, responseTo, originator))

  /**
   * Short-lived actor that retries to process a single input message, until it succeeds, then stops.
   * It makes a blocking call - not much point in responding to this off the actor thread.
   */
  private class PipelineRequestHandler(request: Process, responseTo: ActorRef, originator: ActorRef)
    extends Actor with ActorLogging {

    val requestTimeout = 5.seconds // The timeout for a single request attempt.

    override def preRestart(reason: Throwable, message: Option[Any]) = {
      super.preRestart(reason, message)
      // Send the message to ourselves so we can have another go, after a suitable interval.
      log.debug("Rescheduling message for retry")
      message.foreach { msg => context.system.scheduler.scheduleOnce(retryInterval, self, msg) }
    }

    def receive = {
      case msg: Process => try {
        if (!process.isDefinedAt(msg)) {
          throw new IllegalStateException("Internal error: unexpected message: " + msg.data)
        }
        val result = process(msg)
        sendResponseAndShutdown(result)
      } catch {
        // Wrap exception including all details.
        case NonFatal(e) if isTemporaryFailure(e) => throw TemporaryFailure(originator, e)
        case NonFatal(e) => throw UnrecoverableFailure(originator, e)
      }
    }

    private def sendResponseAndShutdown(outputData: Any) {
      println(s"Telling $responseTo about data $outputData from $originator")
      responseTo.tell(outputData, originator)
      context.stop(self)
    }

  }

}

object PipelineActor {

  /** Request to an actor in a pipeline to process a piece of input data. */
  case class Process(val data: Any)

  // Exception classes used to signal failure in worker actor.
  case class UnrecoverableFailure(originator: ActorRef, e: Throwable) extends Exception(e)
  case class TemporaryFailure(originator: ActorRef, e: Throwable) extends Exception(e)

  /** Properties for an actor that will silently swallow all messages. The /dev/null of actors. */
  def nullActorProps: Props = Props(new NullActor())

}

/** Actor that responds with a computed result. */
trait Requester extends PipelineActor {
  def respondTo = sender
}

/** Actor that forwards computed result. */
trait Transformer extends PipelineActor {
  final def respondTo = output
  val output: ActorRef
}

/** Actor that only performs side-effects, i.e. doesn't forward or respond with any results. */
trait Sink extends PipelineActor {
  private val out = context.actorOf(PipelineActor.nullActorProps)
  def respondTo = out
}

private class NullActor extends Actor {
  def receive() = {
    case _ => // Do nothing.
  }
}

/**
 * Base class for actors that handles the response of a single message processed in a pipeline.
 * An instance of this should be created for each message fired into the pipeline, and this actor
 * should be used as the sender of the message, so that it receives the final Success/Failure notificaiton
 * and can take appropriate action.
 */
abstract class BaseResponseHandler extends Actor with ActorLogging {

  var succeeded: Option[Boolean] = None

  def receive = {
    case Success =>
      onSuccess()
      succeeded = Some(true)
    case Failure(e) =>
      log.info(s"Message failed: " + e.getMessage)
      onError()
      succeeded = Some(false)
  }

  override def postStop() =
    if (!succeeded.isDefined) {
      onNotProcessed()
      log.warning(s"Failed to process message")
    }

  // Methods to override in concrete implementations.
  def onSuccess(): Unit
  def onError(): Unit
  def onNotProcessed(): Unit
}

