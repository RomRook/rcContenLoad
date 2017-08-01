package com.openjaw.contentload.algorithms

import akka.actor._
import com.openjaw.connectors.DistributorConnector.VehLocDetails
import com.openjaw.connectors.{DistributorConnector, RentalCarsConnector}
import com.openjaw.contentload._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{Node, NodeSeq, XML}


case class Depots(locHelper: DefaultHierarchyHelper, config: Config, mainActor: Option[ActorRef]) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("Depots"))

  //external systems
  val rcConnector = new RentalCarsConnector(config.rcEndpoint, config.rcUser, config.rcPassword, config.rcRemoteIp)

  override def run() = {

    val isoCountryCode = config.countyIsoCode
    val isoCountryCodeList = Main.getList(config.countyIsoCode)

    // based on data provided by user we create structure to hold detailed info about countries being processed
    // important attributes:
    // ISOCountryCode - used to find matching city/airport already defined (it is possible to provide multiple codes, e.g. "PL|DE|RU")
    // LocationId - used as a parent location for cities being added into DefaultHierarchy

    val fileWithMapping = config.inDir + Countries.COUNTRY_MAPPING_FILENAME
    val countriesMappings = Countries.userDefinedMapping(skipIgnored = true, fileWithMapping)

    val report =
      <Report>{countriesMappings
        .filter( pair =>{val countryInfo = pair._2
                         val currentIsoCode = countryInfo.isoCountryCode
                      (isoCountryCodeList.contains(currentIsoCode) || isoCountryCode.isEmpty)
          })
        .map( pair => {
          val supplierCountryName = pair._1
          val countryInfo = pair._2
          val isoCountryCode = countryInfo.isoCountryCode
          val stateCodeInUSA = countryInfo.stateCodeInUSA
          val countryId = countryInfo.locationId
          val defaultHierarchyCountryName = countryInfo.locationName //comes from attribute @CurrentName

          logger.info(s"==============================================================================================")
          logger.info(s"Processing depots from RentalCars for country '$supplierCountryName' started")
          logger.info(s"Name of the country defined in DefaultHierarchy '$defaultHierarchyCountryName'")

          //cities from RentalCars for current country
          val rcCities = rcConnector.getCities(supplierCountryName)

          //depots from Car Manager
          val carManagerDepots = Depots.getListFromCarManager(config, isoCountryCode)

          //function to find new depotes for each processed city
          val cityAndItsNewDepotsMap = getVehLocDetailsList(supplierCountryName,isoCountryCode, rcCities, carManagerDepots)

          val toBeAddedRQ = Depots.generateRequests(cityAndItsNewDepotsMap)
          val suffix = isoCountryCode + "_" + supplierCountryName.replaceAll("\\W","")

          config.mode match   {
            case Mode.SaveToFile => {
              Depots.saveToFiles(config, suffix, toBeAddedRQ)
            }
            case Mode.SendRequest => {
              //val actor = Main.system.actorOf(Props(new Main(config)), name = {"actor" + suffix})
              Depots.saveToFiles(config, suffix, toBeAddedRQ)
              Depots.sendRequests(config, mainActor.get, supplierCountryName, toBeAddedRQ)
            }
          }

          logger.info(s"Processing country '$supplierCountryName' finished")

          <Country SupplierName={supplierCountryName} LocationName={defaultHierarchyCountryName} ISOCountryCode={isoCountryCode} LocationId={countryId} StateCode={stateCodeInUSA}>
            {cityAndItsNewDepotsMap.map( pair => {
              val city = pair._1
              val depots = pair._2
              <City SupplierName={city}>
              <Depots>
                <ToBeAdded>{depots
                  .sortBy(n => n.depotCode)
                  .map(n => {
                    val depotCode = n.depotCode
                    val depotName = n.depotName
                    val locationId = n.locationId
                    <Depot locationId={locationId} depotName={depotName} depotCode={depotCode}></Depot>
                  })
                  }
                </ToBeAdded>
              </Depots>
              </City>
            })}
          </Country>
        })}
        <Config> {config.xml}</Config>
      </Report>

    report
  }

  def getVehLocDetailsList(supplierCountryName: String, isoCountryCode: String, supplierCities: NodeSeq, carManagerDepots: Seq[String]) = {
    var cityAndItsDepotsMap = collection.immutable.ListMap.empty[String, List[VehLocDetails]]

    supplierCities.sortBy(_.text).map(n => {
      val supplierCityName = n.text.trim
      //Thread.sleep(1000) //0.5s
      val allToBeProcessed = rcConnector.getDepots(supplierCountryName, supplierCityName)

      val (toBeUpdated, toBeAdded) = allToBeProcessed.partition(n => {
        val supplierCode = (n \ "@id").text

        val result = carManagerDepots.contains(supplierCode)
        if (result)
          logger.debug(s"$supplierCode already defined for city '$supplierCityName'")
        else
          logger.debug(s"$supplierCode needs to be added for city '$supplierCityName'")

        result
        //if (supplierCode == "1984678") false else carManagerDepots.contains(supplierCode)
      })
      logger.info(s"depots for '$supplierCountryName' in city '$supplierCityName' [toBeAdded: ${toBeAdded.size}, toBeUpdated: ${toBeUpdated.size}]")

      val locationId = supplierCityName + "|" + supplierCountryName
      val bookingOffice = config.carBookingOfficeCode
      val supplierCode = config.carSupplierCode

      val toBeAddedVehLocDetails = toBeAdded.map(n => {
        val depotCode = (n \ "@id").text
        //DB constraint for depotName's length
        val depotName = n.text.trim.take(50)

        VehLocDetails(
          locationId,
          depotCode,
          depotName,
          atAirport = Depots.isAirport(depotName),
          cityName = supplierCityName,
          countryCode = isoCountryCode,
          bookingOffice, supplierCode)
      }).toList

      if (cityAndItsDepotsMap.isDefinedAt(supplierCityName)) {
        val message = s"supplierCityName '$supplierCityName' is duplicated for country '$supplierCountryName'"
        logger.error(message)
        throw new RuntimeException(message)
      }
      cityAndItsDepotsMap +=(supplierCityName -> toBeAddedVehLocDetails)
    })
    cityAndItsDepotsMap
  }
}

object Depots {

  val logger = Logger(LoggerFactory.getLogger("Depots"))

  private val AIRPORT_EN = "Airport"

  //contex for codes being added into DefaultHierarchy
  private val RENTAL_CARS_COUNTRY_CODE = "RentalCarsCountry"
  private val RENTAL_CARS_CITY_CODE = "RentalCarsCity"

  private def newDefinition(vehLocDetails: VehLocDetails) = {
    DistributorConnector.createVehLocDetailsNotifRQ(vehLocDetails)
  }

  def sendRequests(config: Config, mainActor: ActorRef, supplierCountryName: String, toBeAddedRQ: NodeSeq) = {
    val countryName = supplierCountryName.replaceAll("\\W","")

    if (toBeAddedRQ.size > 0) {
      logger.info(s"About to send toBeAddedRQ requests to xDist for '$supplierCountryName' and its size is ${toBeAddedRQ.size}")
      mainActor ! Supervisor("toBeAdded", countryName, requestType = DistributorConnector.REQUEST_TYPE_DEPOT, toBeAddedRQ)
    } else {
      logger.info(s"No messages to be sent to xDist for '$supplierCountryName'")
    }
  }

  def saveToFiles(config: Config, sufix: String, toBeAddedRQ: NodeSeq) = {
    logger.info("About to save files which contain xDist request")
    val outDir = config.outDir
    val filename  = outDir + "toBeAddedRQ_" + sufix + ".xml"
    XML.save(filename, <Depots>{toBeAddedRQ}</Depots>, "UTF-8")
    logger.info(s"File $filename saved in directory $outDir")
  }

  def isAirport(depotName: String): Boolean = {
    depotName.toUpperCase.contains(AIRPORT_EN.toUpperCase)
  }

  def getListFromCarManager(config: Config, isoCountryCode: String) = {
    val depotRequest = DistributorConnector.createVehLocSearchRQ(isoCountryCode)
    val depotSoapResponse = DistributorConnector.sendSAOP(depotRequest, config.distributorEndpoint, config.distributorTimeout.toInt)
    val depotResponse = DistributorConnector.getVehLocSearchRS(depotSoapResponse)

    logger.info(depotRequest.toString())
    logger.info(depotResponse.toString())

    val carManagerDepots = DistributorConnector.getDepotCodes(depotResponse)
    logger.info(carManagerDepots.toString())

    carManagerDepots
  }

  def generateRequests(cityAndItsDepotsMap: Map[String, List[VehLocDetails]])  = {
    val toBeAddedDepotsXML = cityAndItsDepotsMap.map(pair => {
      val city = pair._1
      val toBeAddedDepots = pair._2
      val newDepotsXML = toBeAddedDepots.map(Depots.newDefinition)
      (city, newDepotsXML)
    })
    //flat structure which contains XML request used to create new depot
    toBeAddedDepotsXML.map(pair => pair._2).flatten[Node].toSeq
  }

}
