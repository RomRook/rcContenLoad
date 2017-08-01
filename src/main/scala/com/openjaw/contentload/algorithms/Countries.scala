package com.openjaw.contentload.algorithms

import java.io.File

import com.openjaw.connectors.{DistributorConnector, RentalCarsConnector}
import com.openjaw.contentload._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.{NodeSeq, XML, Node}


case class Countries(locHelper: DefaultHierarchyHelper, config: Config) extends Algorithm {

  val logger = Logger(LoggerFactory.getLogger("Countries"))

  //DefaultHierarchy locations
  val locations = locHelper.locationNodes
  val countries = locHelper.countries
  val continents = locHelper.continents

  //helper maps
  val locationLookup = locHelper.locationLookup

  //external systems
  val rcConnector = new RentalCarsConnector(config.rcEndpoint, config.rcUser, config.rcPassword, config.rcRemoteIp)

  //function's alias
  def extract(n: Node) = locHelper.extractLocation(n)

  override def run() = {

    val toBeProcessed = rcConnector.getCountries()
    logger.info(s"toBeProcessed: ${toBeProcessed.size}")

    val defaultHierarchyLookup = Countries.getDefaultHierarchyKeyLookup(countries,extract)

    val (alreadyDefined, toBeAdded) = toBeProcessed.partition(n => Countries.isAlreadyDefined(n,defaultHierarchyLookup))
    logger.info(s"alreadyDefined: ${alreadyDefined.size}")
    logger.info(s"toBeAdded: ${toBeAdded.size}")

    val fileWithMapping = config.inDir + Countries.COUNTRY_MAPPING_FILENAME
    val countriesMappings = Countries.userDefinedMapping(skipIgnored = true, fileWithMapping)

    val toBeAddedXML  = getCountriesToBeAdded(toBeAdded, countriesMappings)

    //for each new country its parent (continent) should be updated
    val parentsToBeUpdatedXML = getContinentsToBeUpdated(toBeAddedXML)

    val (toBeAddedRQ, continentsToBeUpdatedRQ) = Countries.generateRequests(toBeAddedXML,parentsToBeUpdatedXML)

    config.mode match   {
      case Mode.SaveToFile => {
        Countries.saveToFiles(config,toBeAddedRQ, continentsToBeUpdatedRQ)
      }
      case Mode.SendRequest => {
        Countries.saveToFiles(config,toBeAddedRQ, continentsToBeUpdatedRQ)
        Countries.sendRequests(toBeAddedRQ, continentsToBeUpdatedRQ)
      }
    }

    //generate summary report
    val toBeAddedList = toBeAddedXML.map(n => locHelper.extractLocation(n).generalInfo.locationNameEN)
    val parentsToBeUpdatedList = parentsToBeUpdatedXML.map(n => locHelper.extractLocation(n).generalInfo.locationNameEN)
    <Raport>
      <Countries currentCount={countries.size.toString} toBeProcessedCount={toBeProcessed.size.toString} toBeAddedCount={toBeAdded.size.toString}>
        <ToBeAdded>{toBeAddedList.sorted.map(item => <Country>{item}</Country>)}</ToBeAdded>
      </Countries>
      <Parents>
        <ToBeUpdated>{parentsToBeUpdatedList.sorted.map(item => <Location>{item}</Location>)}</ToBeUpdated>
      </Parents>
      <Config> {config.xml}</Config>
    </Raport>
  }


   def getContinentsToBeUpdated(newCountries: NodeSeq) = {
    var continentToBeUpdatedMap: Map[String, List[String]] = Map()

    //populate continents to be updated
    //map contains continent as a key and list of countries as values
    newCountries.foreach(n => {
      val continentId = (n \\ "ParentLocations" \ "LocationReference" \ "@Id").text
      val countryId = (n \ "@Id").text

      if (continentToBeUpdatedMap.isDefinedAt(continentId)) {
        val currentList = continentToBeUpdatedMap(continentId)
        val listUpdated = currentList.::(countryId)
        continentToBeUpdatedMap += (continentId -> listUpdated)
      } else {
        continentToBeUpdatedMap += (continentId -> List(countryId))
      }
    })

    // create XML containing new ChildLocation nodes
    val continentsToBeUpdated = continents
      .filter((n: Node) => {
      val continentId = ((n \ "@Id").text)
      continentToBeUpdatedMap.isDefinedAt(continentId)})
      .map(n => {
      val currentContinent = (n \ "@Id").text
      locHelper.addMultipleChildLocation(n, continentToBeUpdatedMap(currentContinent))
    })
    logger.info("continentsToBeUpdated size: " + continentsToBeUpdated.size)

    continentsToBeUpdated
  }


  def getCountriesToBeAdded(list: NodeSeq, countryMap: collection.mutable.LinkedHashMap[String,RentalCarsLocationInfo]) = {
    val result = list
      .filter(n => {
        val countryName = n.text.trim
        val key = Countries.getKey(countryName)
        if (countryMap.isDefinedAt(key)) {
          //if locationId is empty then create new definition for this country
          val node =  countryMap(key).node
          val locationId = (node \ "@LocationId").text
          (locationId.length == 0)
        } else
          false
      })
      .map(n => {
        val countryName = n.text.trim
        val key = Countries.getKey(countryName)
        val userDefinedData = countryMap(key).node
        val continentName = (userDefinedData \ "@Continent").text
        val continentId = Countries.getContinentId(continentName,countryName)
        val isoCountryCode = (userDefinedData \ "@ISOCountryCode").text
        val timeZone = (userDefinedData \ "@TZ").text
        Countries.newDefinition(countryName, continentId, isoCountryCode, timeZone)
    })
    result
  }

}

object Countries {

  val logger = Logger(LoggerFactory.getLogger("Countries"))

  val COUNTRY_MAPPING_FILENAME = "rc_countries_mappings.xml"

  //Name of continents from DefaultHierarchy
  var continentLookup: Map[String, String] = Map(
    "Europe" -> "CONT_EUROPE",
    "Carribbean" -> "CONT_CARIBBEAN",
    "Asia" -> "CONT_ASIA",
    "Indian Ocean" -> "CONT_INDIANOCEA",
    "Africa" -> "CONT_AFRICA",
    "Oceania" -> "CONT_AUSTRALASI",
    "North America" -> "CONT_NORTHAMERI",
    "South America" -> "CONT_SouthCent")
  logger.info("continentLookup size: " + continentLookup.size)
  logger.info("continentLookup: " + continentLookup.toString())

  def userDefinedMapping(skipIgnored: Boolean = true, file: String) = {
    //val name = "rc_countries_mappings.xml"
    //key: countryName, value: isoCountryCode
    //val lookup = collection.mutable.Map.empty[String, RentalCarsLocationInfo]
    val lookup = collection.mutable.LinkedHashMap.empty[String, RentalCarsLocationInfo]
    //val lookup = collection.mutable.ListMap.empty[String, RentalCarsLocationInfo]

    //val file = config.inDir + name
    //logger.info(s"Input file with data provided by user: $file")

    val temp = new File(file)
    if (!temp.exists)
      throw new RuntimeException(s"File '$file' not exists, please prepare file with country mappings and try again")

    val xml = XML.loadFile(file)
    val nodes = (xml \\ "Country")
    //logger.error(s"Number of countries from file: ${nodes.size}")

    nodes.sortBy(_.text.trim).foreach(n => {
      val country = n.text.trim
      val key = getKey(country)
      if (lookup.isDefinedAt(key))
        logger.error(s"Duplicated country in file for key '$key'")
      else {
        val ignored = (n \ "@Ignored").text
        val isoCountryCode = (n \ "@ISOCountryCode").text
        val locationId = (n \ "@LocationId").text
        val locationType = (n \ "@LocationType").text
        val locationNameEN = (n \ "@CurrentName").text
        val stateCodeInUSA = (n \ "@StateCode").text
        val keyType = (n \ "@CityMergeType").text

        if (skipIgnored && !(ignored == "true")) {
          val locationInfo = RentalCarsLocationInfo(locationId, locationType, locationNameEN, isoCountryCode , stateCodeInUSA, keyType, n)
          lookup.put(key, locationInfo)
        }
      }
    })
    //logger.info(s"Map was populated and its size:  ${lookup.size}")
    lookup
  }

  def getKey(countryName: String) = {
    countryName.trim
  }

  def newDefinition(countryName: String, continentId: String, isoCountryCode: String, timeZone: String) = {
    val countryId = "COUNTRY_" + isoCountryCode
    <Location OnSale="false" ignoreDiff="true" TZ={timeZone} HideName="false" LocationType="country" Version="1" Searchable="true" IndexNumber="5" Id={countryId}>
      <ParentLocations>
        <LocationReference Index="2" Id={continentId}>
        </LocationReference>
      </ParentLocations>
      <Names>
        <Name AjaxString={countryName} Value={countryName} Language="en" AlternativeSpelling="FALSE">
        </Name>
      </Names>
      <Codes>
        <Code Value={isoCountryCode} Context="ISOCountryCode"></Code>
      </Codes>
      <Capabilities>
        <Capability Type="car" Value=" "/>
      </Capabilities>
    </Location>
  }

  def generateRequests(toBeAdded: NodeSeq, continentsToBeUpdated: NodeSeq ) = {
    logger.info("Preparing requests to be sent to xDist ")
    val toBeAddedRQ = DistributorConnector.createLocationHierarchyRQ(<Locations>{toBeAdded}</Locations>)
    val continentsToBeUpdatedRQ = DistributorConnector.createLocationHierarchyRQ(<Locations>{continentsToBeUpdated}</Locations>)

    (toBeAddedRQ,continentsToBeUpdatedRQ)
  }

  def saveToFiles(config: Config, toBeAddedRQ: Node,continentsToBeUpdatedRQ: Node ) = {
    logger.info("About to save files which contain xDist request")
    val outDir = config.outDir
    XML.save(filename = {outDir + "toBeAddedRQ_Countries.xml"}, toBeAddedRQ, "UTF-8")
    XML.save(filename = {outDir + "toBeUpdatedRQ_Continents.xml"}, continentsToBeUpdatedRQ, "UTF-8")
    logger.info("Files were saved in directory " + outDir)
  }

  def sendRequests(toBeAddedRQ: Node, continentsToBeUpdatedRQ: Node ) = {
//    logger.info("About to send toBeAddedRQ requests to xDist")
//    DistributorConnector.sendSOAP(toBeAddedRQ)
//    logger.info("About to send continentsToBeUpdatedRQ requests to xDist")
//    DistributorConnector.sendSOAP(continentsToBeUpdatedRQ)
    throw new RuntimeException("sendRequests not implemented")
  }


  def getDefaultHierarchyKeyLookup(countries: NodeSeq, extract: (Node) => LocationInfo) = {
    logger.info("About to populate map that contains key: CountryName, value: RentalCarsLocationInfo")
    val keyLookup = collection.mutable.Map.empty[String, RentalCarsLocationInfo]
    countries.foreach(n => {
      val locationInfo = extract(n)
      val generalInfo = locationInfo.generalInfo
      val codesInfo = locationInfo.codesInfo
      val locationNameEN = generalInfo.locationNameEN
      val locationType = generalInfo.locationType

      val key = getKey(locationNameEN)
      val rentalCarsLocationInfo = RentalCarsLocationInfo(
        generalInfo.locationId,
        locationType,
        locationNameEN,
        codesInfo.isoCountryCode,
        codesInfo.stateCodeInUSA,
        "",
        n)

      if (keyLookup.isDefinedAt(key)) {
        logger.error(s"Duplicated location for key '$key' " +
          s"current:[locationName: $locationNameEN, locationType: $locationType] " +
          s"alreadyInMap:[${keyLookup(key).locationName}, ${keyLookup(key).locationType}]")
      } else
      {
        keyLookup.put(key, rentalCarsLocationInfo)
      }
    })
    logger.info(s"Map was populated and its size:  ${keyLookup.size}")
    keyLookup.toMap
  }


  def isAlreadyDefined(supplierNode: Node, defaultHierarchyLookup: Map[String, RentalCarsLocationInfo]) = {
    val country = supplierNode.text.trim
    val key = getKey(country)
    //check if country from supplier is already defined in DefaultHierarchy
    defaultHierarchyLookup.isDefinedAt(key)
  }

  def getContinentId(continentName: String, countryName: String) = {
    if (continentName == "")
      throw new RuntimeException(s"Provide continent for country '$countryName'")

    if (!continentLookup.isDefinedAt(continentName))
      throw new RuntimeException(s"There is no such continent: '$continentName'")

    continentLookup(continentName)
  }

  def isLocationIdEmpty(node: Node) = {
    val locationId = (node \ "@LocationId").text
    val a = locationId.length
    (locationId.length == 0)
  }

  def isIgnored(node: Node) = {
    val ignored = (node \ "@Ignored").text
    val result = if (ignored == "false") false else true
    result
  }

}


