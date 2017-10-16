package io.gabriel.akka.singlenode

import scala.concurrent.duration._

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Terminated }
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._

// Needed for actor ? Message
import akka.pattern.ask
import akka.util.Timeout

import ServiceImpl._
import Worker._



//
// Worker Messages
//
object Worker {

  sealed trait BMsg
  case object CommandOne_leaf extends BMsg
  case class  CommandTwo_leaf(key: String) extends BMsg

  case object StopException    extends Exception
  case object ResumeException  extends Exception
  case object RestartException extends Exception

  // used to create Worker, e.g., by ServiceImpl
  def props = Props[Worker]

}

//
// Worker class
//
class Worker extends Actor {

  import Worker._ 

  //override def preStart() = {
  //  println(s"  ! ${self.path} preStart hook")
  //}

  // Restarting on failure
  override def preRestart(reason: Throwable, message: Option[Any] ) = {
    println(s"  ! ${self.path} preRestart hook")
    super.preRestart(reason, message)
  }
  override def postRestart(reason: Throwable ) = {
    println(s"  ! ${self.path} postRestart hook")
    super.postRestart(reason)
  }



  //
  // Process incoming messages
  //
  def receive = {

    case Worker.CommandOne_leaf =>
      println(s"  ${self.path} performing CommandOne_leaf")
      sender() ! ServiceImpl.CommandOneDone

    case Worker.CommandTwo_leaf(key) =>
      println(s"  ${self.path} performing CommandTwo_leaf(${key})")
      val res: String = key.reverse
      sender() ! ServiceImpl.CommandTwoDone(res)

    case default => 
      println(s"  ${self.path} ERROR: Illegal message '${default}' received")
      throw new IllegalArgumentException

  }

} // Worker


//
// ServiceImpl Messages
//
object ServiceImpl {
  sealed trait AMsg

  // received from app
  case object CommandOne extends AMsg
  case class  CommandTwo(key: String) extends AMsg

  // received from Worker
  case object CommandOneDone extends AMsg
  case class  CommandTwoDone(res: String) extends AMsg

  // Used to create an ServiceImpl instance
  def props = Props[ServiceImpl]
}


//
// ServiceImpl class
//

//  Receives messages from the app and delegates 
//  message processing to the child actor Worker
//
class ServiceImpl extends Actor {

  import Worker._

  //
  // Supervised events
  //
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 10 second) {
      case _: IllegalArgumentException  => Restart
      case _: Exception                 => Escalate
  }


  //val worker_ref = context.actorOf(Worker.props, "Worker")
  var worker_ref: ActorRef = _

  // Needed for actor_ref ? Message
  implicit val timeout = Timeout(9 seconds)

  // Needed for map { ... }
  import scala.concurrent.ExecutionContext.Implicits.global

  // Create Worker
  override def preStart() = {
    println(s"  ! ${self.path} preStart() hook: create Worker")
    worker_ref = context.actorOf(Worker.props, "Worker")
  }

  //
  // Receive messages from the App and delegate work to actor B.
  //
  def receive = {

    case ServiceImpl.CommandOne =>
      // Delegate CommandOne to actor B and get result
      println(s"  ${self.path} received CommandOne; delegate to Worker as CommandOne_leaf")
      worker_ref ? Worker.CommandOne_leaf  map {
        case ServiceImpl.CommandOneDone =>
          println(s"  ${self.path} finished CommandOne")
      }

    case ServiceImpl.CommandTwo(key) =>
      // Delegate CommandTwo to actor B and get result
      println(s"  ${self.path} received CommandTwo(${key}; delegate to Worker as CommandTwo_leaf")
      worker_ref ? Worker.CommandTwo_leaf(key) map {
        case ServiceImpl.CommandTwoDone(res) =>
          println(s"  ${self.path} finished CommandTwo result is ${res}")
      }

    case default =>
      // default case
      println(s"  ${self.path} WARNING: Unknown message '${default}' received")
      worker_ref ? default

  }

} // ServiceImpl



//
// Monitoring Actor
// 

object MonitorActor {

  // For replacing
  //   system.actorOf(Props(classOf[MonitorActor], service_impl_ref), "monitoring")
  // with 
  //   system.actorOf(MonitorActor.props(service_impl_ref), "monitoring")
  def props(service_impl_ref: ActorRef) = Props( new MonitorActor(service_impl_ref))

}


class MonitorActor(service_impl_ref: ActorRef) extends Actor {

  override def preStart() = {
    context.watch(service_impl_ref)
  }

  override def postStop() = {
    println(s"  ! ${self.path} Monitoring postStop...")
  }

  def receive = {
    case Terminated(_) => 
      println(s"  ${self.path} received Terminated")
      context.stop(self)
  }
}




//
// The Akka App
// 
object ServiceDriver extends App {


  // Debug flag
  val debug : Boolean = false


  //
  // 1. Set up actor system, actor_a and monitoring
  //

  // Name of the app
  val app_name = "delegation"

  // Create the "delegation" actor system
  val system = ActorSystem(app_name)

  // Create an ServiceImpl instance
  val service_impl_ref = system.actorOf(ServiceImpl.props, name="service_impl")

  // Create the monitoring actor for ServiceImpl
  val monitoring_ref = system.actorOf(MonitorActor.props(service_impl_ref), name="monitoring")


  //
  // 2. Generate messages -- both valid and invalid -- for service_impl
  //

  // 2.1 Send CommandOne to service_impl
  println(s"${app_name}:main Delegating CommandOne to ServiceImpl")
  service_impl_ref ! ServiceImpl.CommandOne 

  // 2.2 Send CommandTwo to service_impl
  println(s"${app_name}:main Delegating CommandTwo('hello') to ServiceImpl")
  service_impl_ref ! ServiceImpl.CommandTwo("hello") 

  if ( debug ) {
    // 2.3 Send illegal command and check that supervision resumes the actor
    println(s"${app_name}:main Delegating illegal command'ERROR' to ServiceImpl")
    service_impl_ref ! "PHONY_BALONEY"

    // 2.4 Send another CommandTwo to service_impl
    println(s"${app_name}:main Delegating CommandTwo('hello') to ServiceImpl")
    service_impl_ref ! ServiceImpl.CommandTwo("farewell") 
  }


  //
  // 3. Hang around until OK to terminate
  //
  try {
    system.awaitTermination(Duration(5, SECONDS));
  } 
  catch {
     case e: Exception => system.terminate() 
  }
  finally {
    system.terminate()
  }


} // ServiceDriver
