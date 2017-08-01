package com.openjaw.contentload

import akka.actor.ActorRef
import com.openjaw.contentload.algorithms._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.Node

trait Algorithm {
  def run(): Node
}

object Algorithm  {
  val logger = Logger(LoggerFactory.getLogger("Algorithm"))

  def apply(locHelper: DefaultHierarchyHelper,config: Config, mainActor: Option[ActorRef]): Algorithm = {

    val algorithmType = config.algorithm
    logger.info(s"Provided algorithm type: $algorithmType")

    val algorithm = {
      algorithmType  match {
        case "TEMPLATE" => MappingTemplate(locHelper, config)
        case "COUNTRY" => Countries(locHelper, config)
        case "CITY" => Cities (locHelper, config, mainActor)
        case "DEPOT" => Depots(locHelper, config, mainActor)
        case other => {
          logger.info(s"Provided algorithm type: $other not handled")
          throw new Exception(s"Provided algorithm type: $other not handled")
        }
      }
    }
    algorithm
  }
}

case class Config(xml: Node) {
  val defaultHierarchy =  (xml \\ "default_hierarchy").text
  val airportCoordinates = (xml \\ "airport_coordinates").text

  val inDir = (xml \\ "data_dir").text + "in/"
  val outDir = (xml \\ "data_dir").text + "out/"
  val rcEndpoint = (xml \\ "rc_endpoint").text
  val rcUser = (xml \\ "rc_user").text
  val rcPassword = (xml \\ "rc_password").text
  val rcRemoteIp = (xml \\ "rc_remote_ip").text
  val distributorWorkers = (xml \\ "number_of_workers").text
  val distributorEndpoint = (xml \\ "xdist_endpoint").text
  val distributorTimeout = (xml \\ "xdist_timeout").text
  var countyIsoCode = (xml \\ "iso_country").text
  var carSupplierCode = (xml \\ "car_supplier_code").text
  var carBookingOfficeCode = (xml \\ "car_booking_office_code").text

  var algorithm = ""
  private val modeStr = (xml \\ "mode").text
  var mode = modeStr match {
    case "FILE_ONLY" => Mode.SaveToFile
    case "SEND_RQ" => Mode.SendRequest
    case _ => Mode.None
  }
  val slash = System.getProperty("file.separator")
}

object Mode extends Enumeration {
  type Mode = Value
  val SaveToFile, SendRequest, None = Value
}

case class LocationInfo(generalInfo: LocationGeneralInfo, codesInfo: LocationCodesInfo)

case class RentalCarsLocationInfo(locationId: String,
                                  locationType: String,
                                  locationName: String,
                                  isoCountryCode: String,
                                  stateCodeInUSA: String,
                                  keyType: String,
                                  node: Node)

case class LocationGeneralInfo(
                             locationId: String,
                             locationType: String,
                             locationNameEN: String,
                             locationNameRU: String,
                             locationAjaxNameEN: String,
                             locationAjaxNameRU: String,
                             node: Node)

case class LocationCodesInfo(
                isoCountryCode: String,
                stateCodeInUSA: String,
                rcCountryCode: String,
                rcCityCode: String,
                utsCode: Seq[String])


