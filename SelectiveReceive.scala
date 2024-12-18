package protocols

import akka.actor.typed.{Behavior, BehaviorInterceptor}
import akka.actor.typed.scaladsl.{Behaviors, StashBuffer}

import scala.reflect.ClassTag

object SelectiveReceive {

  /**
   * @return A behavior that stashes incoming messages unless they are handled
   *         by the underlying `initialBehavior`
   * @param bufferCapacity Maximum number of messages to stash before throwing a `StashOverflowException`
   *                       Note that 0 is a valid size and means no buffering at all (ie all messages should
   *                       always be handled by the underlying behavior)
   * @param initialBehavior Behavior to decorate
   * @tparam T Type of messages
   */
  def apply[T: ClassTag](bufferCapacity: Int, initialBehavior: Behavior[T]): Behavior[T] = {
    require(bufferCapacity > 0, "Capacity must be greater than 0")
    Behaviors.setup { ctx =>
      Behaviors.withStash(bufferCapacity) { buffer =>
        intercept(bufferCapacity, buffer, Behavior.validateAsInitial(initialBehavior))
      }
    }
  }

  /**
   * @return A behavior that interprets the incoming messages with the supplied `started`
   *         behavior to compute the next behavior. If the message has been unhandled, it
   *         is stashed in the `buffer`. If the message has been handled, the previously
   *         stashed messages are also sent to the next behavior.
   *
   * @param bufferSize Capacity of the StashBuffer
   * @param buffer     Buffer to stash unhandled messages to
   * @param started    Behavior to decorate. Must be a valid “initial” behavior.
   * @tparam T         Type of messages
   */
  private def intercept[T: ClassTag](bufferSize: Int, buffer: StashBuffer[T], started: Behavior[T]): Behavior[T] =
    Behaviors.receive { (ctx, message) =>
      val nextBehavior = Behavior.interpretMessage(started, ctx, message)
      if (Behavior.isUnhandled(nextBehavior)) {
        buffer.stash(message)
        Behaviors.same
      } else {
        val canonicalNextBehavior = Behavior.canonicalize(nextBehavior, started, ctx)
        val unstashedBehavior = buffer.unstashAll(canonicalNextBehavior)
        SelectiveReceive(bufferSize, unstashedBehavior)
      }
    }
}
