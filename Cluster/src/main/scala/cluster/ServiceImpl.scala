package io.gabriel.akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ Actor, ActorRef, ActorSystem, Props, RootActorPath, Terminated }
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._

// Needed for actor ? Message
import akka.pattern.ask
import akka.util.Timeout

import akka.cluster._

// events such as MemberUp, MemberRemoved, ...
import akka.cluster.ClusterEvent._

import io.gabriel.akka.commons._
import ServiceDriver._
import Worker._



//
// Worker Messages
//
object Worker {

  sealed trait BMsg
  case object CommandOne_leaf              extends BMsg
  case class  CommandTwo_leaf(key: String) extends BMsg

  case object StopException    extends Exception
  case object ResumeException  extends Exception
  case object RestartException extends Exception

  // used to create worker by ServiceImpl
  def props = Props[Worker]

}


//
// The worker class
//
class Worker extends Actor {


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
// Service implem
//
object ServiceImpl {

  sealed trait AMsg

  // received from ServiceDriver
  case object CommandOne extends AMsg
  case class  CommandTwo(key: String) extends AMsg

  // received from Worker
  case object CommandOneDone extends AMsg
  case class  CommandTwoDone(res: String) extends AMsg


  private var service_impl_ref: ActorRef = _ 

  //
  // Create the ServiceImpl instance
  //
  def initiate(port: Int) : ActorSystem = {

    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.load().getConfig("ServiceImpl"))

    //val system = ActorSystem("ClusterSystem", config)
    val system = ActorSystem(AppName.name, config)

    // Server ServiceImpl actor
    service_impl_ref = system.actorOf(Props[ServiceImpl], name = "service_impl")

    // Create actor for monitoring ServiceImpl
    val monitoring_ref = system.actorOf(MonitorService.props(service_impl_ref), "monitoring")


    return system

  } // initiate




   def getServiceImplRef = service_impl_ref



} // object ServiceImpl




//
// ServiceImpl class
//
class ServiceImpl extends Actor {


  //
  // Supervised events
  //
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 10 second) {
      case _: IllegalArgumentException  => Restart
      case _: Exception                 => Escalate
  }


  //
  // This cluster
  //
  val cluster = Cluster(context.system)


  //
  // Reference to worker actor
  //
  var worker_ref: ActorRef = _


  //
  // Used for ask type messaging
  //

  // Needed for service_driver_ref ? Message
  implicit val timeout = Timeout(9 seconds)

  // Needed for map { ... }
  import scala.concurrent.ExecutionContext.Implicits.global




  //
  // preStart()
  //
  // Create worker and subscribe to cluster changes, MemberUp
  // re-subscribe when restarted
  override def preStart(): Unit = {

    println(s"  ! ${self.path} preStart() hook")
    cluster.subscribe(self, classOf[MemberUp])

    println(s"  ! ${self.path} preStart(): create Worker")
    worker_ref = context.actorOf(Worker.props, "Worker")

  }


  //
  // postStop()
  //
  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }


  //
  // Function to register this actor with the service_driver
  //
  def register(member: Member): Unit = {
    val from_service_driver : Boolean = member.hasRole("service_driver")
    if ( from_service_driver ) {
      val service_driver_ref = 
        context.actorSelection(RootActorPath(member.address) / "user" / "service_driver")
      service_driver_ref ! ServiceImplRegistration
      println(s"  ${self.path} # Sent ServiceImplRegistration msg to service_driver") 
    }

  }



  //
  // Receive messages
  //
  def receive = {


    //
    // CommandOne
    //
    case ServiceImpl.CommandOne =>
      // Delegate CommandOne to actor B and get result
      println(s"  ${self.path} received CommandOne; delegate to Worker as CommandOne_leaf")
      worker_ref ? Worker.CommandOne_leaf  map {
        case ServiceImpl.CommandOneDone =>
          println(s"  ${self.path} finished CommandOne")
      }


    //
    // CommandTwo(key)
    //
    case ServiceImpl.CommandTwo(key) =>
      // Delegate CommandTwo to actor B and get result
      println(s"  ${self.path} received CommandTwo(${key}; delegate to Worker as CommandTwo_leaf")
      worker_ref ? Worker.CommandTwo_leaf(key) map {
        case ServiceImpl.CommandTwoDone(res) =>
          println(s"  ${self.path} finished CommandTwo result is ${res}")
      }


    // Register the service_impl with the service_driver, 
    // by sending it a ServiceImplRegistration message, 
    // when the service_driver is up, so that the service 
    // driver can communicate with the service impl,
    case MemberUp(member) =>

      val m_addr = member.address
      var member_role : String = "none"
      if( member.hasRole("service_driver") ) { 
        member_role = "service_driver"
      }
      else if( member.hasRole("service_impl") ) { 
        member_role = "service_impl"
      }
      println(s"  ${self.path} # Got MemberUp msg from ${member_role} with addr ${m_addr}") 

      // If message came from service_driver, register this actor with service_driver.
      register(member)


    //
    // Member removed
    //
    case MemberRemoved(member, previousStatus) =>
      println(s"  ${self.path} Member ${member.address} is Removed after ${previousStatus}")

    // 
    // CurrentClysterState
    //
    case state: CurrentClusterState =>
      println(s"  ${self.path} # Got message CurrentClusterState; not handled at this time")
      // TODO
      // state.members.filter(_.status == MemberStatus.Up) foreach register


    //
    // MemberEvent
    //
    case _: MemberEvent => // ignore


    //
    // Test messages from the ClusterApp and ServiceDriver
    //
    case "app_2_impl" => 
      println(s"  ${self.path} # Got message 'app_2_impl' from app")
    case "driver_2_impl" => 
      println(s"  ${self.path} # Got message 'driver_2_impl' from service_driver")


    // 
    // Default is for unknown messages: we can discard or forward them
    //
    case default =>
      // default case
      println(s"  ${self.path} WARNING: Unknown message '${default}' received")
      worker_ref ? default


  } // receive 


} // class ServiceImpl






//
// Monitor the service impl
//

object MonitorService {

  def props(service_impl_ref: ActorRef) = Props( new MonitorService(service_impl_ref))

}

class MonitorService(service_impl_ref: ActorRef) extends Actor {

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

