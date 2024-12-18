package protocols

import akka.actor.typed._
import akka.actor.typed.scaladsl._

import scala.concurrent.duration._

object Transactor {

  sealed trait PrivateCommand[T] extends Product with Serializable
  final case class Committed[T](session: ActorRef[Session[T]], value: T) extends PrivateCommand[T]
  final case class RolledBack[T](session: ActorRef[Session[T]]) extends PrivateCommand[T]

  sealed trait Command[T] extends PrivateCommand[T]
  final case class Begin[T](replyTo: ActorRef[ActorRef[Session[T]]]) extends Command[T]

  sealed trait Session[T] extends Product with Serializable
  final case class Extract[T, U](f: T => U, replyTo: ActorRef[U]) extends Session[T]
  final case class Modify[T, U](f: T => T, id: Long, reply: U, replyTo: ActorRef[U]) extends Session[T]
  final case class Commit[T, U](reply: U, replyTo: ActorRef[U]) extends Session[T]
  final case class Rollback[T]() extends Session[T]

  def apply[T](value: T, sessionTimeout: FiniteDuration): Behavior[Command[T]] =
    SelectiveReceive[Command[T]](30, idle(value, sessionTimeout).narrow)

  private def idle[T](value: T, sessionTimeout: FiniteDuration): Behavior[PrivateCommand[T]] =
    Behaviors.receive { (ctx, message) =>
      message match {
        case Begin(replyTo) =>
          val sessionActor = ctx.spawnAnonymous(sessionHandler(value, ctx.self, Set()))
          ctx.watch(sessionActor)
          ctx.scheduleOnce(sessionTimeout, ctx.self, RolledBack(sessionActor))
          replyTo ! sessionActor
          inSession(value, sessionTimeout, sessionActor)
        case _ =>
          Behaviors.same
      }
    }

  private def inSession[T](rollbackValue: T, sessionTimeout: FiniteDuration, sessionRef: ActorRef[Session[T]]): Behavior[PrivateCommand[T]] =
    Behaviors.receive { (ctx, message) =>
      message match {
        case RolledBack(session) if session == sessionRef =>
          ctx.stop(sessionRef)
          idle(rollbackValue, sessionTimeout)

        case Committed(session, value) if session == sessionRef =>
          idle(value, sessionTimeout)

        case Terminated(ref) if ref == sessionRef =>
          ctx.self ! RolledBack(sessionRef)
          Behaviors.same

        case _ =>
          Behaviors.unhandled
      }
    }


  private def sessionHandler[T](currentValue: T, parent: ActorRef[PrivateCommand[T]], done: Set[Long]): Behavior[Session[T]] =
    Behaviors.receive { (ctx, message) =>
      message match {
        case Extract(f, replyTo) =>
          try {
            replyTo ! f(currentValue)
            Behaviors.same
          } catch {
            case _: Exception =>
              parent ! RolledBack(ctx.self)
              Behaviors.stopped
          }

        case Modify(f, id, reply, replyTo) =>
          if (done.contains(id)) {
            replyTo ! reply
            Behaviors.same
          } else {
            try {
              val newValue = f(currentValue)
              replyTo ! reply
              sessionHandler(newValue, parent, done + id)
            } catch {
              case _: Exception =>
                parent ! RolledBack(ctx.self)
                Behaviors.stopped
            }
          }

        case Commit(reply, replyTo) =>
          parent ! Committed(ctx.self, currentValue)
          replyTo ! reply
          Behaviors.stopped

        case Rollback() =>
          parent ! RolledBack(ctx.self)
          Behaviors.stopped
      }
    }
}
