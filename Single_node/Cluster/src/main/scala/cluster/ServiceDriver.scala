package io.gabriel.akka.cluster

import scala.util.Random

import com.typesafe.config.ConfigFactory

import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Terminated }

import akka.cluster._

// events such as MemberUp, MemberRemoved, ...
import akka.cluster.ClusterEvent._

import io.gabriel.akka.commons._

import ServiceImpl._


//
// ServiceDriver object creates a ServiceDriver instance 
// in the initiate function which is invoked by the app
//
object ServiceDriver {

  private var _sd: ActorRef = _ 

  def initiate() : ActorSystem = {

    val config = ConfigFactory.load().getConfig("ServiceDriver")

    //val system = ActorSystem("ClusterSystem", config)
    val system = ActorSystem(AppName.name, config)

    _sd = system.actorOf(Props[ServiceDriver], name = "service_driver")

    return system

  }


  //
  // Allow the app to send messages to ServiceDriver
  //
  def getServiceDriverRef = _sd

} // object ServiceDriver



//
// ServiceDriver class sends commands 
// to ServiceImpl
//
class ServiceDriver extends Actor {

  //
  // Debug flag
  //
  val debug: Boolean = false


  //
  // Pool of available service implem actors
  //
  var service_impl_pool = IndexedSeq.empty[ActorRef]


  //
  // This cluster
  //
  val cluster = Cluster(context.system)

  //override def preStart(): Unit = {
  //  println(s"  ! ${self.path} preStart() hook")
  //  cluster.subscribe(self, classOf[MemberUp])
  //}



  //
  // receive function
  //
  def receive = {

    //
    // Message to debug communication App -> ServiceDriver
    //
    case "app_2_driver" => 
      println(s"  ${self.path} # Got message 'app_2_driver' from app")



    //
    // 1. Start message from app:
    // 
    // Wait for a service_impl to register with the driver 
    // (which happens when receing the ServiceImplRegistration message), 
    // and then send commands to the service_impl
    //
    case "start" => 

      // 
      // 1.1 Find a service_impl actor to which to send messages
      //

      println(s"  ${self.path} # Got message 'start' from app")

      //
      //  Wait for the pool to be non-empty (this happens when 
      //  the ServiceImplRegistration message is received below)
      //
      println(s"  ${self.path} # Waiting for a service implementation actor")
      while ( service_impl_pool.isEmpty ) { 
        Thread.sleep(60) 
      }
      println(s"  ${self.path} # Got a service implementation actor")


      //
      // 1.2 Pick (from the pool) a service_impl actor to which to send messages
      //
      val service_impl_ref: ActorRef = service_impl_pool(Random.nextInt(service_impl_pool.size))


      //
      // 1.3 Generate messages -- both valid and invalid -- for ServiceImpl
      //

      // 1.3.0 TEST Send test message 'driver_2_impl' to service_impl
      if ( debug ) {
        println(s"  ${self.path} # Sending 'driver_2_impl' msg to service impl ${service_impl_ref}")
        service_impl_ref ! "driver_2_impl"
      }

      // 1.3.1 Send CommandOne
      println(s"${self.path} Delegating CommandOne to ServiceImpl")
      service_impl_ref ! CommandOne 

      // 1.3.2 Send CommandTwo to ServiceImpl
      println(s"${self.path} Delegating CommandTwo('hello') to ServiceImpl")
      service_impl_ref ! CommandTwo("hello") 

      if ( debug ) {
        // 1.3.3 Send illegal command and check that supervision resumes the actor
        println(s"${self.path} Delegating illegal command'ERROR' to ServiceImpl")
        service_impl_ref ! "PHONY_BALONEY"

        // 1.3.4 Send another CommandTwo to ServiceImpl
        println(s"${self.path} Delegating CommandTwo('hello') to ActorA")
        service_impl_ref ! CommandTwo("farewell") 
      }



    //
    // 2. Add a new service_impl when such a service_impl actor registers
    //
    case ServiceImplRegistration if !(service_impl_pool.contains(sender())) =>
      println(s"  ${self.path} # Received registration from service_impl")
      service_impl_pool = service_impl_pool :+ sender()
      context watch(sender())


    //
    // 3. Remove a terminated service_impl from the pool
    //
    case Terminated(a) =>
      service_impl_pool = service_impl_pool.filterNot(_ == a)


    //
    // 4. MemberUp message: show who sent it
    //
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

    //
    // 5. MemberRemoved
    //
    case MemberRemoved(member, previousStatus) =>
      println(s"  ${self.path} Member ${member.address} is Removed after ${previousStatus}")

    //
    // 6. CurrentClusterState
    //
    case state: CurrentClusterState =>
      println(s"  ${self.path} WARNING: Discarding message type CurrentClusterState")
      // state.members.filter(_.status == MemberStatus.Up) foreach register


    //
    // 7. MemberEvent
    //
    case _: MemberEvent => // ignore


    //
    // 8. Default case
    //
    case default =>

      // Can forward or discard
      println(s"  ${self.path} WARNING: Unknown message '${default}'")

      // Forward to service_impl
      if ( ! service_impl_pool.isEmpty ) { 
        println(s"  ${self.path} WARNING: Forward unknown message '${default}; to service_impl")
        val service_impl_ref: ActorRef = service_impl_pool(Random.nextInt(service_impl_pool.size))
        service_impl_ref ! default
      }


  } // receive


} // class ServiceDriver

