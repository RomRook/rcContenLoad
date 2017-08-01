package com.openjaw.contentload.algorithms

import akka.actor.ActorRef
import com.openjaw.connectors.{DistributorConnector, GoogleConnector, RentalCarsConnector}
import com.openjaw.contentload._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{Node, NodeSeq, XML}


case class Cities(locHelper: DefaultHierarchyHelper, config: Config, mainActor: Option[ActorRef]) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("Cities"))

  val locations = locHelper.locationNodes
  val countries = locHelper.countries
  val cities = locHelper.cities
  val airports = locHelper.airports

  //helper maps
  val locationLookup = locHelper.locationLookup
  lazy val statesInUSALookup =  locHelper.getStatesInUSALookup

  //external systems
  val rcConnector = new RentalCarsConnector(config.rcEndpoint, config.rcUser, config.rcPassword, config.rcRemoteIp)

  def extract(n: Node) = locHelper.extractLocation(n)

  override def run() = {

    val isoCountryCode = config.countyIsoCode
    val isoCountryCodeList = Cities.getList(config.countyIsoCode)

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
          val keyType = countryInfo.keyType
          val defaultHierarchyCountryName = countryInfo.locationName //comes from attribute @CurrentName

          logger.info(s"==============================================================================================")
          logger.info(s"Processing cities from RentalCars for country '$supplierCountryName' started")
          logger.info(s"Name of the country defined in DefaultHierarchy '$defaultHierarchyCountryName'")

          //cities from RentalCars for current country
          val allToBeProcessedAll = rcConnector.getCities(supplierCountryName)

          // spilt cities from supplier into two groups:
          // 1) airports - in fact it is city that contains in its name string 'airport' (it will be merged with locationType='airport')
          // 2) cities - remaining locations (it will be merged with locationType='city')
          val (airportsToBeProcessed, citiesToBeProcessed) = allToBeProcessedAll.partition(n => Cities.isAirport(supplierCityName = n.text, supplierCountryName))
          logger.info("allToBeProcessedAll: " + allToBeProcessedAll.size)
          logger.info("airportsToBeProcessed: " + airportsToBeProcessed.size)
          logger.info("citiesToBeProcessed: " + citiesToBeProcessed.size)

          //populate map with cities from DefaultHierarchy (used during merge process)
          val defaultHierarchyLookup = Cities.getDefaultHierarchyKeyLookup(cities, locHelper.extractLocation, isoCountryCode, stateCodeInUSA, Cities.LOCATION_TYPE_CITY, keyType)
          val defaultHierarchyLookupRU = Cities.getDefaultHierarchyKeyLookup(cities, locHelper.extractLocation, isoCountryCode, stateCodeInUSA, Cities.LOCATION_TYPE_CITY, keyType, Cities.LANGUAGE_RU)

          val (citiesToBeAddedXML, citiesToBeUpdatedXML, parentsToBeUpdatedXML) = processCities(citiesToBeProcessed,
                                                                                                supplierCountryName,
                                                                                                defaultHierarchyCountryName,
                                                                                                countryId,
                                                                                                isoCountryCode,
                                                                                                stateCodeInUSA,
                                                                                                defaultHierarchyLookup,
                                                                                                defaultHierarchyLookupRU,
                                                                                                keyType)

          val (airportsToBeUpdatedXML, noneMatchesAirportNames, multiMatchesAirportNames) = processAirports(airportsToBeProcessed,supplierCountryName, countryId,isoCountryCode,stateCodeInUSA)

          //generate request which will be sent to xDist
          val (toBeAddedRQ, toBeUpdatedRQ, parentsToBeUpdatedRQ, airportsToBeUpdatedRQ) = Cities.generateRequests(citiesToBeAddedXML,citiesToBeUpdatedXML, parentsToBeUpdatedXML, airportsToBeUpdatedXML)
          val sufix = isoCountryCode + "_" + supplierCountryName.replaceAll("\\W","")
          config.mode match   {
              case Mode.SaveToFile => {
                Cities.saveToFiles(config, sufix , toBeAddedRQ, toBeUpdatedRQ, parentsToBeUpdatedRQ, airportsToBeUpdatedRQ )
              }
              case Mode.SendRequest => {
                Cities.saveToFiles(config, sufix, toBeAddedRQ, toBeUpdatedRQ, parentsToBeUpdatedRQ, airportsToBeUpdatedRQ)
                Cities.sendRequests(config, mainActor.get, supplierCountryName, citiesToBeAddedXML,citiesToBeUpdatedXML, parentsToBeUpdatedXML, airportsToBeUpdatedXML)
            }
          }
          logger.info(s"Processing country '$supplierCountryName' finished")

          <Country SupplierName={supplierCountryName} LocationName={defaultHierarchyCountryName} ISOCountryCode={isoCountryCode} LocationId={countryId} StateCode={stateCodeInUSA}>
            <Cities currentCount={defaultHierarchyLookup.size.toString} toBeProcessedCount={citiesToBeProcessed.size.toString} toBeAddedCount={citiesToBeAddedXML.size.toString} toBeUpdatedCount={citiesToBeUpdatedXML.size.toString}>
              <ToBeAdded>{citiesToBeAddedXML
                .sortBy(n => extract(n).generalInfo.locationNameEN)
                .map( n => {
                val locationInfo = locHelper.extractLocation(n)
                val generalInfo = locationInfo.generalInfo
                val codesInfo = locationInfo.codesInfo
                <City locationId={generalInfo.locationId} locationType={generalInfo.locationType} nameRU={generalInfo.locationNameRU} ustCode={codesInfo.utsCode.mkString} rentalCarsCountry={codesInfo.rcCountryCode} rentalCarsCity={codesInfo.rcCityCode}>{generalInfo.locationNameEN}</City>
              })
                }</ToBeAdded>
              <ToBeUpdated>{citiesToBeUpdatedXML
                .sortBy(n => extract(n).generalInfo.locationNameEN)
                .map( n => {
                val locationInfo = locHelper.extractLocation(n)
                val generalInfo = locationInfo.generalInfo
                val codesInfo = locationInfo.codesInfo
                <City locationId={generalInfo.locationId} locationType={generalInfo.locationType} nameRU={generalInfo.locationNameRU} ustCode={codesInfo.utsCode.mkString} rentalCarsCountry={codesInfo.rcCountryCode} rentalCarsCity={codesInfo.rcCityCode}>{generalInfo.locationNameEN}</City>
              })}
              </ToBeUpdated>
            </Cities>
            <Parents>
              <ToBeUpdated>{parentsToBeUpdatedXML
                .map( n => {
                val locationInfo = locHelper.extractLocation(n)
                val generalInfo = locationInfo.generalInfo
                val codesInfo = locationInfo.codesInfo
                <Location locationId={generalInfo.locationId} locationType={generalInfo.locationType} nameRU={generalInfo.locationNameRU} ustCode={codesInfo.utsCode.mkString}>{generalInfo.locationNameEN}</Location>
              })}
              </ToBeUpdated>
            </Parents>
            {if (airportsToBeProcessed.size > 0)
            {<Airports currentCount={defaultHierarchyLookup.size.toString} toBeProcessedCount={airportsToBeProcessed.size.toString} toBeAddedCount="0" toBeUpdatedCount={airportsToBeUpdatedXML.size.toString}>
              <ToBeUpdated>{airportsToBeUpdatedXML
                .sortBy(n => extract(n).generalInfo.locationNameEN)
                .map( n => {
                  //val(locationId, locationType, locationNameEN, locationNameRU, ustCode, _, _, _, _) = extract(n)
                  val locationInfo = locHelper.extractLocation(n)
                  val generalInfo = locationInfo.generalInfo
                  val codesInfo = locationInfo.codesInfo
                  <Airport locationId={generalInfo.locationId} locationType={generalInfo.locationType} nameRU={generalInfo.locationNameRU} ustCode={codesInfo.utsCode.mkString} rentalCarsCountry={codesInfo.rcCountryCode} rentalCarsCity={codesInfo.rcCityCode}>{generalInfo.locationNameEN}</Airport>
                })}
              </ToBeUpdated>
              {if (multiMatchesAirportNames.size > 0)
              {<MultipleMatches count={multiMatchesAirportNames.size.toString} desc="List of location with multiple matches. Look at logs for more detailed info">{multiMatchesAirportNames
                .map(name => <City>{name}</City>)}
              </MultipleMatches>}}
              <ZeroMatches count={noneMatchesAirportNames.size.toString} desc="">{noneMatchesAirportNames
                .map(name => <City>{name}</City>)}
              </ZeroMatches>
              <SupplierAirports count={airportsToBeProcessed.size.toString} desc="Cities from RentalCars that are in fact airports">{airportsToBeProcessed
                .sortBy(n => n.text)
                .map(node => node)}
              </SupplierAirports>
            </Airports>}}
          </Country>
        })}
        <Config> {config.xml}</Config>
      </Report>

    report
  }

  def getMatchingAirport(supplierCityName: String, isoCountryCode: String): (Option[Node], Int, String) = {

    val airport = findMatchingAirport(supplierCityName, isoCountryCode )
    val count = airport.size
    if (count == 1)
      (Some(airport.head),count, supplierCityName)
    else
      (None, count, supplierCityName) //none or multiple matches
  }

  def findMatchingAirport(supplierNameEN: String, isoCountryCode: String) = {
    //e.g London Luton Airport
    val patternEN = supplierNameEN.trim
      .replaceAll("\\([A-Z]{2}\\)","")
      .replaceAll("-"," ")
      .toLowerCase
      .replaceAll(Cities.AIRPORT_EN,"")

    val tokenListEN = patternEN.split(" ")

    val matchedAirports  = airports
      .filter(n => {
        val result = (locHelper.extractLocation(n).codesInfo.isoCountryCode == isoCountryCode)
        result
        })
      .filter(n => {
        val locationInfo = locHelper.extractLocation(n)
        val locationNameEN = locationInfo.generalInfo.locationAjaxNameEN
        val locationAjaxNameEN = locationInfo.generalInfo.locationAjaxNameEN

        val locationAjaxEN = locationAjaxNameEN.replaceAll(" ","").replaceAll(",","").toLowerCase.trim
        val locationEN = locationNameEN.replaceAll(" ","").replaceAll(",","").toLowerCase.trim

        (tokenListEN.forall(e => locationEN.contains(e)) || tokenListEN.forall(e => locationAjaxEN.contains(e)))

    })

    matchedAirports.size match {
      case 0 => {
        logger.info(s"No matching airport for '$supplierNameEN' [patternEN: ${tokenListEN.mkString(" ")}, isoCountryCode: $isoCountryCode ]")}
      case 1 => {
        val locationInfo = locHelper.extractLocation(matchedAirports.head)
        val locationId = locationInfo.generalInfo.locationId
        val locationType = locationInfo.generalInfo.locationType
        val locationNameEN = locationInfo.generalInfo.locationAjaxNameEN
        val locationNameRU = locationInfo.generalInfo.locationAjaxNameRU
        val locationAjaxNameEN = locationInfo.generalInfo.locationAjaxNameEN
        val locationAjaxNameRU = locationInfo.generalInfo.locationAjaxNameRU

        logger.info(s"Found match for '$supplierNameEN' which is '$locationId' of type '$locationType' " +
          s"[enName: '$locationNameEN', ruName: '$locationNameRU', enAjaxName: '$locationAjaxNameEN', ruAjaxName: '$locationAjaxNameRU']")
      }
      case count: Int => {
        logger.info(s"Multiple matches ($count) for '$supplierNameEN'")
        matchedAirports.foreach(n => {
          //val (locationId, locationType, _, _, _, _, _, locationAjaxNameEN, locationAjaxNameRU) = locHelper.extractLocationInfo(n)

          val locationInfo = locHelper.extractLocation(n)
          val locationId = locationInfo.generalInfo.locationId
          val locationType = locationInfo.generalInfo.locationType
          val locationNameEN = locationInfo.generalInfo.locationAjaxNameEN
          val locationAjaxNameEN = locationInfo.generalInfo.locationAjaxNameEN
          val locationAjaxNameRU = locationInfo.generalInfo.locationAjaxNameRU

          logger.info(s"'$locationId' of type '$locationType' [locationNameEN: " +
            s"'$locationAjaxNameEN', locationNameRU: " +
            s"'$locationAjaxNameRU', patternEN: $patternEN]")
        })
      }
    }

    matchedAirports

  }

  def processCities(supplierCities: NodeSeq,
                    supplierCountryName: String,
                    defaultHierarchyCountryName: String,
                    parentId: String,
                    isoCountryCode: String,
                    stateCodeInUSA: String,
                    defaultHierarchyLookupEN: Map[String, RentalCarsLocationInfo],
                    defaultHierarchyLookupRU: Map[String, RentalCarsLocationInfo],
                    keyType: String) = {
    def getState(supplierCityName: String, stateCodeInUSA: String ) = {
      if (stateCodeInUSA != "") stateCodeInUSA
      else Cities.getCityNameAndState(supplierCityName)._2
    }
    def getTranslation(supplierCities: NodeSeq, subQuery: String) = {
        supplierCities
          .map(n => {
            val supplierCityEN = n.text.trim
            val query = supplierCityEN.replaceAll(" ","%20") + " " + subQuery
            val (supplierCityRU,_,_,_) = GoogleConnector.getRussianNameAndGeoCodes(query)
            supplierCityEN -> supplierCityRU
        }).toMap
    }

    //first attempt using english names
    val (matchedCitiesEN, newCitiesEN) = supplierCities.partition(n => Cities.isAlreadyDefined(n.text.trim, defaultHierarchyLookupEN, keyType))

    //get russian names for cities that were not merged
    val subQuery = if (stateCodeInUSA != "") (stateCodeInUSA + " " + isoCountryCode) else isoCountryCode
    val dictionary = getTranslation(newCitiesEN, subQuery)

    //second attempt to merge, this time based on russian names
    val (matchedCitiesRU, newCitiesRU) = newCitiesEN.partition(n => {
      val supplierCityEN = n.text.trim
      val supplierCityRU = dictionary(supplierCityEN)
      if (supplierCityRU != "")
        Cities.isAlreadyDefined(supplierCityRU,defaultHierarchyLookupRU, keyType, Cities.LANGUAGE_RU)
      else
        false
    })

    val newCities = newCitiesEN.diff(matchedCitiesRU)
    val matchedCities = matchedCitiesEN ++ matchedCitiesRU

    logger.info("toBeProcessedCity: " + supplierCities.size)
    logger.info(s"matchedCitiesEN: ${matchedCitiesEN.size}")
    logger.info(s"matchedCitiesRU: ${matchedCitiesRU.size}")
    logger.info(s"matchedCities: ${matchedCities.size}")
    logger.info(s"newCities: ${newCities.size}")

    val toBeUpdatedXML_EN = matchedCitiesEN.map(n => {
      val supplierCity = n.text.trim
      val (city, state) = Cities.getCityNameAndState(supplierCity)
      val key = Cities.getKey(keyType, city, state)

      val matchedCity = defaultHierarchyLookupEN(key).node
      val changed_1 = locHelper.addCarCapability(matchedCity)
      val changed_2 = locHelper.addRentalCarsCode(changed_1, value=supplierCountryName, context=Cities.RENTAL_CARS_COUNTRY_CODE)
      val changed_3 = locHelper.addRentalCarsCode(changed_2, value=supplierCity, context=Cities.RENTAL_CARS_CITY_CODE)
      changed_3
    })

    val toBeUpdatedXML_RU = matchedCitiesRU.map(n => {
      val supplierCityEN = n.text.trim
      val supplierCityRU = dictionary(supplierCityEN)
      val (city, state) = Cities.getCityNameAndState(supplierCityRU)
      val key = Cities.getKey(keyType, city, state)

      val matchedCity = defaultHierarchyLookupRU(key).node
      val changed_1 = locHelper.addCarCapability(matchedCity)
      val changed_2 = locHelper.addRentalCarsCode(changed_1, value=supplierCountryName, context=Cities.RENTAL_CARS_COUNTRY_CODE)
      val changed_3 = locHelper.addRentalCarsCode(changed_2, value=supplierCityEN, context=Cities.RENTAL_CARS_CITY_CODE)
      changed_3
    })

    val toBeAddedXML = newCities.map(n => {
      val supplierCityName = n.text.trim
      val stateCode = getState(supplierCityName, stateCodeInUSA)
      val stateId = statesInUSALookup.getOrElse(stateCode, "")
      Cities.newDefinition(supplierCityName, supplierCountryName, defaultHierarchyCountryName, parentId, stateId, isoCountryCode, stateCode)
    })

    val parentsToBeUpdatedXML = getParentsToBeUpdated(toBeAddedXML)

    (toBeAddedXML, toBeUpdatedXML_EN ++ toBeUpdatedXML_RU, parentsToBeUpdatedXML)
  }

  def processAirports(supplierAirport: NodeSeq, supplierCountryName: String, countryId: String, isoCountryCode: String, stateCodeInUSA: String) = {

    val (matchedAirports, otherAirports) = supplierAirport
        .map(n => getMatchingAirport(supplierCityName = n.text, isoCountryCode))
        .partition(_._2 == 1) //number of matches is one
    logger.info("toBeProcessedAirport: " + supplierAirport.size)
    logger.info(s"Number of location matched with airports: ${matchedAirports.size}")
    logger.info(s"Number of location not matched with airports (none or multiple matches): ${otherAirports.size}")

    val toBeUpdatedXML = matchedAirports.map(n => {
      //modify airport (add RenatalCarCountry, RenatalCarCity)
      val supplierCityName = n._3
      val matchedNode = n._1.get
      val changed_1 = locHelper.addCarCapability(matchedNode)
      val changed_2 = locHelper.addRentalCarsCode(changed_1, value = supplierCountryName, context = Cities.RENTAL_CARS_COUNTRY_CODE)
      val changed_3 = locHelper.addRentalCarsCode(changed_2, value = supplierCityName, context = Cities.RENTAL_CARS_CITY_CODE)
      changed_3
    })

    val (noneMatch, multipleMatches) = otherAirports.partition(_._2 == 0) //none
    logger.info(s"Number of locations with no match: ${noneMatch.size}")
    logger.info(s"Number of locations with multiple matches: ${multipleMatches.size}")

    val noneMatchesAirportNames = noneMatch.map(_._3)
    val multiMatchesAirportNames = multipleMatches.map(_._3)

    (toBeUpdatedXML, noneMatchesAirportNames, multiMatchesAirportNames)

  }


  def getParentsToBeUpdated(citiesToBeAdded: NodeSeq) = {
    var parentsToBeUpdatedMap: Map[String, List[String]] = Map()

    //populate parents (countries, regions) to be updated
    //map contains locationId as a key and list of cities as value
    citiesToBeAdded.foreach(n => {
      val parentId = (n \\ "ParentLocations" \ "LocationReference" \ "@Id").text
      val cityId = (n \ "@Id").text

      if (parentsToBeUpdatedMap.isDefinedAt(parentId)) {
        val currentList = parentsToBeUpdatedMap(parentId)
        val listUpdated = currentList.::(cityId)
        parentsToBeUpdatedMap += (parentId -> listUpdated)
      } else {
        parentsToBeUpdatedMap += (parentId -> List(cityId))
      }
    })
    logger.info("parentsToBeUpdatedMap size: " + parentsToBeUpdatedMap.size)

    val parentsToBeUpdatedXML = parentsToBeUpdatedMap.map(pair => {
     val parentId = pair._1
     val parentNode: Node = locationLookup(parentId).generalInfo.node
     val childList = pair._2
     val changedNode: Node = locHelper.addMultipleChildLocation(parentNode, childList)
      changedNode
    })

    logger.info("parentsToBeUpdatedXML size: " + parentsToBeUpdatedXML.size)

    parentsToBeUpdatedXML.toSeq
  }

}

object Cities {

  val logger = Logger(LoggerFactory.getLogger("Cities"))

  private val KEY_TYPE_CITY_AND_STATE = "city#state"
  private val KEY_TYPE_CITY_ONLY = "city"

  private val LOCATION_TYPE_COUNTRY = "country"
  private val LOCATION_TYPE_CITY = "city"
  private val LOCATION_TYPE_AIRPORT = "airport"

  private val AIRPORT_EN = " airport"
  private val AIRPORT_RU = " аэропорт"

  private val LANGUAGE_EN = "en"
  private val LANGUAGE_RU = "ru"

  //contex for codes being added into DefaultHierarchy
  private val RENTAL_CARS_COUNTRY_CODE = "RentalCarsCountry"
  private val RENTAL_CARS_CITY_CODE = "RentalCarsCity"

  private val ISO_COUNTRY_CODE_USA = "US"

  def getKey(cityName: String) = {
    //locationNameEN.toUpperCase.trim
    cityName.trim
  }

  def getKey(keyType: String, city: String, state: String ) = {
    val key = if (keyType == KEY_TYPE_CITY_AND_STATE) {
              s"${city.trim}#${state.trim}"
              } else {
                city.trim
              }
    key
  }

  def getCityNameAndState(supplierName: String) = {
    // for country: 'USA - Other' there is also stateCode provided in city name, e.g.
    //    <City>Augusta, GA</City>
    //    <City>Augusta, KS</City>
    //    <City>Augusta, ME</City>
    //    <City>Augusta, NJ</City>
    val list = supplierName.split(",")
    val city = if (list.isDefinedAt(0)) list(0).trim else ""
    val state = if (list.isDefinedAt(1)) list(1).trim else ""

    (city, state)

  }

  def validateCity(supplierNode: Node, defaultHierarchyNode: Node,  extractInfoFunction: (Node) => LocationInfo) = {
      val supplierCityName = supplierNode.text.trim

      val generalInfo = extractInfoFunction(defaultHierarchyNode).generalInfo
      val codesInfo = extractInfoFunction(defaultHierarchyNode).codesInfo
      val rcCityCode = codesInfo.rcCityCode
      //compare info RentalCarsCity already defined with name coming from supplier
      val result = rcCityCode == supplierCityName

      if (result == false)
        logger.error(s"Error: Matched city has different info for code '$RENTAL_CARS_CITY_CODE'. " +
          s"City from supplier '$supplierCityName' was matched with '${generalInfo.locationNameEN}locationName' [locationId: ${generalInfo.locationId}locationId]" +
          s"Current $RENTAL_CARS_CITY_CODE '$rcCityCode'")

      result
  }

  def validateCountry(supplierCountryName: String, supplierCityName: String, defaultHierarchyNode: Node,  extractInfoFunction: (Node) => LocationInfo) = {

    val generalInfo = extractInfoFunction(defaultHierarchyNode).generalInfo
    val codesInfo = extractInfoFunction(defaultHierarchyNode).codesInfo
    val rcCountryCode = codesInfo.rcCountryCode
    //compare info RentalCarsCity already defined with name coming from supplier
    val result = rcCountryCode == supplierCountryName

    if (result == false)
      logger.error(s"Error: Matched city has different info for code '$RENTAL_CARS_COUNTRY_CODE'. " +
        s"City from supplier '$supplierCityName' from country '$supplierCountryName' was matched with '${generalInfo.locationNameEN}locationName' [locationId: ${generalInfo.locationId}locationId]" +
        s"Current $RENTAL_CARS_COUNTRY_CODE '$rcCountryCode'")

    result
  }

  def isAlreadyDefined(supplierCityName: String, defaultHierarchyLookup: Map[String, RentalCarsLocationInfo], keyType: String, language: String = LANGUAGE_EN) = {
    val (city, state) = getCityNameAndState(supplierCityName)
    val key = getKey(keyType, city, state)
    defaultHierarchyLookup.isDefinedAt(key)
  }


  private def newDefinition(supplierCityName: String, supplierCountryName: String, defaultHierarchyCountryName: String, parentId: String, stateId: String, isoCountryCode: String, stateCodeInUSA: String ) = {
    def getNewCityId(isoCountryCode: String, supplierName: String, stateCodeInUSA: String) = {
      val name = supplierName.replaceAll( "\\W","").trim.take(10).toLowerCase.capitalize
      val iso = isoCountryCode.toLowerCase.capitalize
      val state = stateCodeInUSA.toLowerCase.capitalize
      val locationId =
        if (isoCountryCode == ISO_COUNTRY_CODE_USA) {
          "Cit" + iso + state + name
        } else {
          "Cit" + iso + name
        }
      locationId.take(16)
    }
    def getParentId(isoCountryCode: String, parentId: String, stateId: String) = {
      if (isoCountryCode == ISO_COUNTRY_CODE_USA) {
        if (stateId != "") stateId else parentId
      } else parentId
    }
    def getAjaxString(isoCountryCode: String, countryName: String, stateName: String, cityName: String) = {
      if (isoCountryCode == ISO_COUNTRY_CODE_USA) {
        s"$cityName, $stateName, $countryName"
      }
      else {
          s"$cityName, $countryName"
      }
    }

    //city without info about state is used as name in DefaultHierarchy (state is defined as separate info)
    val city = getCityNameAndState(supplierCityName)._1
    val defaultHierarchyCountryNameEN = defaultHierarchyCountryName
    val ajaxStringEN = getAjaxString(isoCountryCode, defaultHierarchyCountryNameEN, stateCodeInUSA, city)
    val locationId = getNewCityId(isoCountryCode, city, stateCodeInUSA)
    val finalParentId = getParentId(isoCountryCode, parentId, stateId)

    <Location OnSale="false" ignoreDiff="true" TZ="" HideName="false" LocationType="city" Version="1" Searchable="true" IndexNumber="5" Id={locationId}>
      <ParentLocations>
        <LocationReference Index="2" Id={finalParentId}>
        </LocationReference>
      </ParentLocations>
      <Names>
        <Name AjaxString={ajaxStringEN} Value={city} Language="en" AlternativeSpelling="FALSE"></Name>
        <Name AjaxString={ajaxStringEN} Value={city} Language="ru" AlternativeSpelling="FALSE"></Name>
      </Names>
      <Codes>
        <Code Value={isoCountryCode} Context="ISOCountryCode"></Code>
        <Code Value={supplierCountryName} Context={RENTAL_CARS_COUNTRY_CODE}></Code>
        <Code Value={supplierCityName} Context={RENTAL_CARS_CITY_CODE}></Code>{if (isoCountryCode == ISO_COUNTRY_CODE_USA && stateCodeInUSA != "") <Code Value={stateCodeInUSA} Context="StateCode"></Code>}
      </Codes>
      <Capabilities>
        <Capability Type="car" Value=""/>
      </Capabilities>
    </Location>
  }

  def getDefaultHierarchyKeyLookup(locations: NodeSeq,
                                   extractInfoFunction: (Node) => LocationInfo,
                                   isoCountryCode: String,
                                   stateCodeInUSA: String,
                                   locationType: String,
                                   keyType: String,
                                   language: String = LANGUAGE_EN) = {
        logger.info(s"About to populate map for isoCountry='$isoCountryCode' that contains [key: $locationType, value: RentalCarsLocationInfo]")
        val keyLookup = collection.mutable.Map.empty[String, RentalCarsLocationInfo]
        locations
          .filter(n => {
                val currentLocationType = extractInfoFunction(n).generalInfo.locationType
                val currentIsoCode = extractInfoFunction(n).codesInfo.isoCountryCode
                val currentStateCodeInUSA = extractInfoFunction(n).codesInfo.stateCodeInUSA
                ((currentLocationType == locationType)
                  && (currentIsoCode == isoCountryCode)
                  && (stateCodeInUSA == "" || currentStateCodeInUSA == stateCodeInUSA)
                )
            })
          .foreach(n => {

            val generalInfo = extractInfoFunction(n).generalInfo
            val codesInfo = extractInfoFunction(n).codesInfo

            val (city, state) = if (language == LANGUAGE_EN)
                                    (generalInfo.locationNameEN, codesInfo.stateCodeInUSA)
                                else
                                    (generalInfo.locationNameRU, codesInfo.stateCodeInUSA)

            val key = getKey(keyType, city, state)
            if (key != "") {
              val locationInfo = RentalCarsLocationInfo(generalInfo.locationId, generalInfo.locationType, generalInfo.locationNameEN, codesInfo.isoCountryCode, codesInfo.stateCodeInUSA, keyType, n)
              if (keyLookup.isDefinedAt(key)) {
                logger.error(s"Duplicated location for key '$key' " +
                  s"current:[locationId: ${generalInfo.locationId}, locationName: ${generalInfo.locationNameEN}, locationType: ${generalInfo.locationType}, isoCountryCode: ${codesInfo.isoCountryCode}] " +
                  s"alreadyInMap:[${keyLookup(key).locationId}, ${keyLookup(key).locationName}, ${keyLookup(key).locationType}, ${keyLookup(key).isoCountryCode}]")
              } else   {
                keyLookup.put(key, locationInfo)
              }
            }
        })
        logger.info(s"Map was populated and its size: ${keyLookup.size}")
        keyLookup.toMap
      }

  def isAirport(supplierCityName: String, supplierCountryName: String) = {
    if (supplierCityName.toLowerCase.trim.contains(AIRPORT_EN)) {
      logger.debug(s"City '$supplierCityName' in country '$supplierCountryName' from RentalCars will be treated as 'Airport' type")
      true
    } else {
      false
    }
  }

  //param might be passed as 'PL|DE|RU' (pipe separates each iso code)
  def getList(inputParam: String) = {
    val isoCountryCodeList = {
      if (inputParam.isEmpty) {
        logger.warn(s"all countries will be processed")
        List.empty[String]
      }
      else {
        val list = inputParam.split("\\|").toList
        logger.info(s"Number of countries to be processed: ${list.size}")
        list
      }
    }
    logger.info(s"isoCountryCodeList: $isoCountryCodeList")
    isoCountryCodeList
  }

  def generateRequests(toBeAdded: NodeSeq, toBeUpdated: NodeSeq, parentsToBeUpdated: NodeSeq, airportsToBeUpdated: NodeSeq) = {
    logger.info("Preparing requests to be sent to xDist ")
    val toBeAddedRQ = if (toBeAdded.size > 0) Some(DistributorConnector.createLocationHierarchyRQ(<Locations>{toBeAdded}</Locations>))
                      else None
    val toBeUpdatedRQ = if (toBeUpdated.size > 0) Some(DistributorConnector.createLocationHierarchyRQ(<Locations>{toBeUpdated}</Locations>))
                        else None
    val parentsToBeUpdatedRQ = if (parentsToBeUpdated.size > 0) Some(DistributorConnector.createLocationHierarchyRQ(<Locations>{parentsToBeUpdated}</Locations>))
                                else None
    val airportsToBeUpdatedRQ = if (airportsToBeUpdated.size > 0) Some(DistributorConnector.createLocationHierarchyRQ(<Locations>{airportsToBeUpdated}</Locations>))
                                else None

    (toBeAddedRQ,toBeUpdatedRQ,parentsToBeUpdatedRQ, airportsToBeUpdatedRQ)
  }

  def sendRequests(config: Config, mainActor: ActorRef, supplierCountryName: String, toBeAddedRQ: NodeSeq, toBeUpdatedRQ: NodeSeq, parentsToBeUpdatedRQ: NodeSeq, airportsToBeUpdatedRQ: NodeSeq ) = {

    val countryName = supplierCountryName.replaceAll("\\W","")

    if (toBeAddedRQ.size > 0) {
      logger.info(s"About to send toBeAddedRQ requests to xDist for '$supplierCountryName' (size: ${toBeAddedRQ.size})")
      mainActor ! Supervisor("toBeAdded", countryName, requestType = DistributorConnector.REQUEST_TYPE_LOCATION, toBeAddedRQ)
    }

    if (toBeUpdatedRQ.size > 0) {
      logger.info(s"About to send toBeUpdatedRQ requests to xDist for '$supplierCountryName' (size: ${toBeUpdatedRQ.size})")
      mainActor ! Supervisor("toBeUpdated", countryName, requestType = DistributorConnector.REQUEST_TYPE_LOCATION, toBeUpdatedRQ)
    }

    if (parentsToBeUpdatedRQ.size > 0) {
      logger.info(s"About to send parentsToBeUpdatedRQ requests to xDist for '$supplierCountryName' (size: ${parentsToBeUpdatedRQ.size})")
      mainActor ! Supervisor("parentsToBeUpdated", countryName, requestType = DistributorConnector.REQUEST_TYPE_LOCATION, parentsToBeUpdatedRQ)
    }

    if (airportsToBeUpdatedRQ.size > 0) {
      logger.info(s"About to send airportsToBeUpdatedRQ requests to xDist for '$supplierCountryName' (size: ${airportsToBeUpdatedRQ.size})")
      mainActor ! Supervisor("airportsToBeUpdated", countryName, requestType = DistributorConnector.REQUEST_TYPE_LOCATION, airportsToBeUpdatedRQ)
    }

  }

  def saveToFiles(config: Config, sufix: String, toBeAddedRQ: Option[Node],toBeUpdatedRQ: Option[Node], parentsToBeUpdatedRQ: Option[Node], airportsToBeUpdatedRQ: Option[Node]) = {
    logger.info("About to save files which contain xDist request")
    def save(filename: String, requestData: Option[Node], counter: Int) = {
      if (requestData.isDefined) {
        XML.save(filename, requestData.get, "UTF-8")
        counter + 1
      } else
        counter
    }
    val outDir = config.outDir
    var savedFiles: Int = 0
    savedFiles = save(filename = {outDir + "toBeAddedRQ_" + sufix + ".xml"}, toBeAddedRQ, savedFiles)
    savedFiles = save(filename = {outDir + "toBeUpdatedRQ_" + sufix + ".xml"}, toBeUpdatedRQ, savedFiles)
    savedFiles = save(filename = {outDir + "toBeUpdatedParentsRQ_" + sufix + ".xml"}, parentsToBeUpdatedRQ, savedFiles)
    savedFiles = save(filename = {outDir + "toBeUpdatedAirportRQ_" + sufix + ".xml"}, airportsToBeUpdatedRQ, savedFiles)
    logger.info(s"Number of $savedFiles files were saved in directory $outDir")
  }

}
