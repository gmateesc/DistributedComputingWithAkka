package io.gabriel.akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ Actor, ActorRef, ActorSystem, Props, Terminated }

import akka.cluster._

import io.gabriel.akka.commons._

import ServiceDriver._
import ServiceImpl._


object ClusterApp extends App {

  //
  // Debug flag
  //
  val debug: Boolean = false


  //
  // 1. Create ServiceDriver and ServiceImpl
  //
  val system_driver = ServiceDriver.initiate()
  val system_impl   = ServiceImpl.initiate(2552)
  Thread.sleep(1000)


  //
  // 2. Send msg to service_driver and service_impl
  //

  val driverRef = ServiceDriver.getServiceDriverRef
  val implRef = ServiceImpl.getServiceImplRef

  // 2.0 Send messages for testing
  if ( debug ) {
    // Send test msg to service_driver and service_impl
    driverRef ! "app_2_driver"
    implRef ! "app_2_impl"
  }

  // 2.1 Send start message to service_driver
  driverRef ! "start"



  //
  // 3. Hang around until OK to terminate
  //
  try {
    system_impl.awaitTermination(Duration(15, SECONDS));
  } 
  catch {
     case e: Exception => system_impl.terminate() 
  }
  finally {
    system_impl.terminate()
  }

  try {
    system_driver.awaitTermination(Duration(5, SECONDS));
  } 
  catch {
     case e: Exception => system_driver.terminate() 
  }
  finally {
    system_driver.terminate()
  }


} // object ClusterApp
