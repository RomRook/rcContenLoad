package com.openjaw.connectors

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.openjaw.contentload.{Progress, SenderProgress}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{Node, NodeSeq}
import scalaj.http.{Http, HttpRequest}

object DistributorConnector{

  val logger = Logger(LoggerFactory.getLogger("DistributorConnector"))

  val REQUEST_TYPE_LOCATION = "OJ_LocationHierarchyRQ"
  val REQUEST_TYPE_DEPOT = "OTA_VehLocDetailsNotifRQ"

  class Supervisor(name: String, requestType: String, nodesToBeSent: NodeSeq, xDistEndpoint: String, xDistTimeout: Int = 180000, numberOfWorkers: Int) extends Actor {
    logger.info(s"Supervisor '$name' about to create $numberOfWorkers sendActors ...")
    val workers = createWorkers
    val workersCount = workers.size
    val workerProgress = collection.mutable.Map.empty[AnyRef, SenderProgress]
    //statistics based on all workers
    val totalSentCount = nodesToBeSent.size
    var totalProcessedCount = 0
    var totalSuccessCount = 0
    var totalErrorCount = 0

    def receive = {
      case "DistributeRequests" => {
        distributeRequests
      }
      //message received from SenderActor
      case SenderProgress(processed: Int, success: Int, err: Int) => {
        logger.debug(s"Progress from ${sender.toString()}:  processed: $processed, successCount: $success, errorCount: $err")
        workerProgress.put(sender,SenderProgress(processed,success,err))
        totalProcessedCount = workerProgress.map(pair => pair._2.processedCount).sum
        totalSuccessCount = workerProgress.map(pair => pair._2.successCount).sum
        totalErrorCount = workerProgress.map(pair => pair._2.errorCount).sum

        context.parent ! Progress(totalSentCount, totalProcessedCount, totalSuccessCount, totalErrorCount)
      }
      case other => logger.error(s"Supervisor got not handled message: '$other'")
    }

    def createWorkers() = {
      val requestCount = nodesToBeSent.size
      val actorCount = if (requestCount < numberOfWorkers) requestCount else numberOfWorkers
      var listOfActors: List[ActorRef] = List.empty
      for (i <- 1 to actorCount) {
        val actorName = "sendActor-" + i
        val sendActor = context.actorOf(Props(new SendActor(xDistEndpoint,xDistTimeout)), actorName)
        listOfActors = sendActor :: listOfActors
      }
      listOfActors
    }

    def distributeRequests = {
      var index: Int = 0
      nodesToBeSent.foreach(n  => {
        index += 1
        val senderIndex = index % workers.size
        workers(senderIndex) ! (requestType, n)
      })
    }

    def genReport = {
      var index: Int = 0
      workers.foreach(n  => {
        index += 1
        val senderIndex = index % workers.size
        workers(senderIndex) ! "Report"
      })
    }

  }

  class SendActor(xDistEndpoint: String, xDistTimeout: Int) extends Actor {

    val log = Logging(context.system, this)
    var successCount = 0
    var processedCount = 0

    def receive = {
      case (reqType: String, reqBody: Node) if (reqType == DistributorConnector.REQUEST_TYPE_LOCATION) => {
        processedCount += 1
        val locationHierarchyRQ = DistributorConnector.createLocationHierarchyRQ(reqBody)
        val soapResponse = DistributorConnector.sendSAOP(locationHierarchyRQ, xDistEndpoint, xDistTimeout)
        val (locationHierarchyRS, isSuccess) = DistributorConnector.getLocationHierarchyRS(soapResponse)
        if (isSuccess) {
          successCount += 1
          logger.debug("Successful response 'LocationHierarchyRS':" + locationHierarchyRS)
        } else {
          val supervisorName = sender.toString
          logger.error(s"Error in response 'LocationHierarchyRS' [supervisor: $supervisorName]\nxDistRequest: ${locationHierarchyRQ}\nxDistResponse: ${locationHierarchyRS}")
        }
        sender ! SenderProgress(processedCount, successCount, processedCount-successCount)
      }
      case (reqType: String, vehLocDetailsNotifRQ: Node) if (reqType == DistributorConnector.REQUEST_TYPE_DEPOT) => {
        processedCount += 1
        val soapResponse = DistributorConnector.sendSAOP(vehLocDetailsNotifRQ, xDistEndpoint, xDistTimeout)
        val (vehLocDetailsNotifRS, isSuccess) = DistributorConnector.getVehLocDetailsNotifRS(soapResponse)
        if (isSuccess) {
          successCount += 1
          logger.debug("Successful response 'VehLocDetailsNotifRS':" + vehLocDetailsNotifRS)
        } else {
          val supervisorName = sender.toString
          logger.debug(s"Error in response 'VehLocDetailsNotifRS' [supervisor: $supervisorName]\nxDistRequest: ${vehLocDetailsNotifRQ}\nxDistResponse: ${vehLocDetailsNotifRS}")
        }

        sender ! SenderProgress(processedCount, successCount, processedCount-successCount)
      }
      case other => log.error(s"SendActor: message type '$other' is not handled")
    }

  }

  case class VehLocDetails(locationId: String,
                           depotCode: String,
                           depotName: String,
                           atAirport: Boolean,
                           cityName: String,
                           countryCode: String,
                           bookingOffice: String,
                           supplierCode: String)

  def createLocationHierarchyRQ(location: Node) = {
    <OJ_LocationHierarchyRQ CheckVersionOnly=" " xmlns="http://www.opentravel.org/OTA/2003/05">
      <POS>
        <Source ISOCountry="IE" ISOCurrency="EUR" PseudoCityCode="XLC">
          <RequestorID ID="xRezServer01" URL="http://www.openjawtech.com" UserID="admin"/>
        </Source>
      </POS>
      <LocationHierarchy Action="set" IgnoreVersions="true" Name="DefaultHierarchy">
        <Locations>{location}</Locations>
      </LocationHierarchy>
    </OJ_LocationHierarchyRQ>
  }

  def createDefaultHierarchyRQ() = {
    <ns1:OJ_LocationHierarchyRQ CheckVersionOnly="" xmlns:ns1="http://www.opentravel.org/OTA/2003/05">
      <ns1:LocationHierarchy Action="get" Name="DefaultHierarchy" Page="1" PageSize="1000000" Version=""/>
      <ns1:POS>
        <ns1:Source ISOCountry="IE" ISOCurrency="EUR" PseudoCityCode="XLC">
          <ns1:RequestorID ID="xRezServer01" URL="http://www.openjawtech.com" UserID="admin"/>
        </ns1:Source>
        <ns1:Audits/>
      </ns1:POS>
    </ns1:OJ_LocationHierarchyRQ>
  }

  def getLocationHierarchyRS(soapResponse: Node) = {
    val locationHierarchyRS = soapResponse \\ "OJ_LocationHierarchyRS"
    if (locationHierarchyRS.size > 0) {
      val isSuccess = ((locationHierarchyRS \ "Success").size > 0)
      (locationHierarchyRS, isSuccess)
    } else {
      (soapResponse, false)
    }
  }

  def getVehLocDetailsNotifRS(soapResponse: Node): (NodeSeq, Boolean) = {
    val vehLocDetailsNotifRS = soapResponse \\ "OTA_VehLocDetailsNotifRS"
    if (vehLocDetailsNotifRS.size > 0) {
      logger.debug("VehLocDetailsNotifRS response from xDist:" + vehLocDetailsNotifRS)
      val isSuccess = ((vehLocDetailsNotifRS \ "Success").size > 0)
      (vehLocDetailsNotifRS, isSuccess)
    } else {
      logger.debug("VehLocDetailsNotifRS response from xDist:" + soapResponse)
      (soapResponse, false)
    }
  }

  def getVehLocSearchRS(soapResponse: Node) = {
    val result = soapResponse \\ "OTA_VehLocSearchRS"
    logger.debug("VehLocSearchRS response from xDist:" + result)
    result
  }

  def isSuccess(xDistRequest: Node, xDistResponse: NodeSeq) = {
    val result = ((xDistResponse \ "Success").size > 0)
    result
  }

  def createVehLocDetailsNotifRQ(vehDetails: VehLocDetails) = {
      <OTA_VehLocDetailsNotifRQ PrimaryLang="en" TransactionIdentifier="rcContentLoad" xmlns="http://www.opentravel.org/OTA/2003/05">
        <POS>
          <Source ISOCurrency="EUR" PseudoCityCode="XCARC">
            <RequestorID ID="xRezServer01" URL="http://www.openjawtech.com" UserID="admin" accessAllCars="true"/>
          </Source>
        </POS>
        <LocationDetails>
          <LocationDetail AtAirport={vehDetails.atAirport.toString}  Code={vehDetails.locationId} DepotRegion="false" Enabled="true" ExtendedLocationCode={vehDetails.depotCode} Name={vehDetails.depotName} OJVehDepotCode="" PrimaryAirportCode="" Status="" StatusComment="">
            <Address Type="" UseType="">
              <BldgRoom/>
              <StreetNmbr/>
              <CityName>{vehDetails.cityName}</CityName>
              <PostalCode/>
              <CountryName Code={vehDetails.countryCode}/>
              <Position Latitude="" Longitude=""/>
            </Address>
            <Telephone AreaCityCode="" CountryAccessCode="" PhoneNumber="" PhoneTechType="1"/>
            <Telephone AreaCityCode="" CountryAccessCode="" PhoneNumber="" PhoneTechType="3"/>
 						<OperationSchedules>
                <OperationSchedule>
                    <OperationTimes>
                        <OperationTime Close="23:00" Mon="true" Open="00:00"/>
                        <OperationTime Close="23:00" Open="00:00" Tue="true"/>
                        <OperationTime Close="23:00" Open="00:00" Weds="true"/>
                        <OperationTime Close="23:00" Open="00:00" Thur="true"/>
                        <OperationTime Close="23:00" Fri="true" Open="00:00"/>
                        <OperationTime Close="23:00" Open="00:00" Sat="true"/>
                        <OperationTime Close="23:00" Open="00:00" Sun="true"/>
                    </OperationTimes>
                </OperationSchedule>
            </OperationSchedules>
            <AgeRequirements>
              <Age MaximumAge="" MinimumAge=""/>
            </AgeRequirements>
            <VehicleVendorSupplier BookingOfficeChannel={vehDetails.bookingOffice} Code={vehDetails.supplierCode} TravelSector=""/>
          </LocationDetail>
        </LocationDetails>
        <Audit Action="CreatNewCar" resource="Common" type="Cars"/>
      </OTA_VehLocDetailsNotifRQ>
  }


  def createVehLocSearchRQ(isoCountryCode: String) = {
      <OTA_VehLocSearchRQ MaxResponses="500" PageNumber="1" Target="Editor" TransactionStatusCode="true" Version="1.0" accessAllCars="true" xmlns="http://www.opentravel.org/OTA/2003/05">
        <POS>
          <Source ISOCurrency="EUR" PseudoCityCode="XCARC">
            <RequestorID ID="xRezServer01" URL="http://www.openjawtech.com" UserID="admin" accessAllCars="true"/>
          </Source>
        </POS>
        <VehLocSearchCriterion DepotRegion=" " Enabled=" ">
          <Address>
            <CountryName Code={isoCountryCode}/>
          </Address>
        </VehLocSearchCriterion>
        <Vendor Code="RC"/>
      </OTA_VehLocSearchRQ>
  }

  def createSOAP(requestBody: Node) = {
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
      <soapenv:Body>
        {requestBody}
      </soapenv:Body>
    </soapenv:Envelope>
  }


  def sendSAOP(xDistRequest: Node, xDistEndpoint: String, xDistTimeout: Int) = {
    val soapRequest = createSOAP(xDistRequest)
    val soapResponse = postHttpRequest(soapRequest, xDistEndpoint, xDistTimeout)
    soapResponse
  }


  def postHttpRequest(requestSOAP: Node, xDistEndpoint: String, xDistTimeout: Int) = {
    val request: HttpRequest = Http(xDistEndpoint)
      .postData(requestSOAP.toString())
      .header("SOAPAction","")
      .header("Content-type","application/xml")
      .timeout(connTimeoutMs = 1000, readTimeoutMs = xDistTimeout)

    logger.debug("About to send SOAP request to xDist: " + requestSOAP.toString())

    try {
      val responseSOAP = request.execute(parser = {inputStream => scala.xml.XML.load(inputStream)})
      logger.debug("SOAP response from xDist: " + responseSOAP)

      if (responseSOAP.isSuccess)
        responseSOAP.body
      else
        //error during communication over Http
        <Error code={responseSOAP.code.toString}>{responseSOAP.body}</Error>

    } catch {
      case socketTimeout: java.net.SocketTimeoutException =>
        logger.error(s"SocketTimeoutException: ${socketTimeout.getMessage} \nrequestSOAP: $requestSOAP")
        <Error code="SOCKET_TIMEOUT" desc={socketTimeout.getMessage}>{socketTimeout.getStackTrace}</Error>
      case e: Exception =>
        logger.error(s"Exception: ${e.getMessage}")
        <Error code="HTTP_ERROR" desc={e.getMessage}>{e.getStackTrace}</Error>
    }
  }

  def getDepotCodes(response: NodeSeq) = {
    val locationDetailNodes = (response \\ "LocationDetail")
    val result = locationDetailNodes.map(n => {
      val depotCode = (n \ "@ExtendedLocationCode").text
      depotCode
    })
    logger.debug(result.toString())
    result
  }

  def getDefaultHierarchy(xDistEndpoint: String, xDistTimeout: Int) = {
    val locationHierarchyRQ = DistributorConnector.createDefaultHierarchyRQ()
    val soapResponse = DistributorConnector.sendSAOP(locationHierarchyRQ, xDistEndpoint, xDistTimeout)
    val (locationHierarchyRS, isSuccess) = DistributorConnector.getDefaultHierarchyRS(soapResponse)
    if (isSuccess) {
      logger.debug("Successful response 'DefaultHierarchyRS':" + locationHierarchyRS)
    } else {
      logger.error(s"Error in response 'DefaultHierarchyRS' [\nxDistRequest: ${locationHierarchyRQ}\nxDistResponse: ${locationHierarchyRS}")
    }
    (locationHierarchyRS, isSuccess)
  }

  def getDefaultHierarchyRS(soapResponse: Node) = {
    val locationHierarchyRS = soapResponse \\ "OJ_LocationHierarchyRS"
    if (locationHierarchyRS.size > 0) {
      val isSuccess = ((locationHierarchyRS \ "Success").size > 0)
      (locationHierarchyRS, isSuccess)
    } else {
      (soapResponse, false)
    }
  }


}
