package com.openjaw.contentload

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.xml.transform.{RewriteRule, RuleTransformer}
import scala.xml.{Elem, Node, NodeSeq}
class DefaultHierarchyHelper(defaultHierarchyXML: NodeSeq) {

  val logger = Logger(LoggerFactory.getLogger("LocationHelper"))

  val locationNodes = defaultHierarchyXML \\ "Location"

  val cities = locationNodes.filter((n:Node) => (n.attribute("LocationType").get.text == "city"))
  val countries = locationNodes.filter((n:Node) => (n.attribute("LocationType").get.text == "country"))
  val continents = locationNodes.filter((n:Node) => (n.attribute("LocationType").get.text == "continent"))
  val states = locationNodes.filter((n:Node) => (n.attribute("LocationType").get.text == "state"))
  val airports = locationNodes.filter((n:Node) => (n.attribute("LocationType").get.text == "airport"))

  logger.info("all location: " + locationNodes.size)
  logger.info("continents: " + continents.size)
  logger.info("countries: " + countries.size)
  logger.info("cities: " + cities.size)
  logger.info("airports: " + airports.size)

  val locationLookup = getLocationLookup

  def extractLocationInfoUTS(locationNode: Node) = {
    try {

      val locationId = (locationNode \ "@Id").text
      val locationType = (locationNode \ "@LocationType").text

      //location name
      val namesNode = locationNode \\ "Location" \ "Names" \ "Name"
      //first english name (might be more then one name defined in english that's way take the first one)

      val firstEnglishName = namesNode.filter(name => {
        (name \ "@Language").text == "en"
      }).headOption.getOrElse(<Empty></Empty>)
      val locationNameEN = (firstEnglishName \ "@Value").text
      val locationAjaxNameEN = (firstEnglishName \ "@AjaxString").text

      val firstRussianName = namesNode.filter(name => {
        (name \ "@Language").text == "ru"
      }).headOption.getOrElse(<Empty></Empty>)
      val locationNameRU = (firstRussianName \ "@Value").text
      val locationAjaxNameRU = (firstRussianName \ "@AjaxString").text

      //UTS Code
      val codesNode = locationNode \\ "Location" \ "Codes" \ "Code"
      val utsCodeNode = codesNode.filter(code => {
        (code \ "@Context").text == "UTS"
      })
      //if multiple uts codes defined for the same location
      val utsCodeSeq: Seq[String] = utsCodeNode.map(code => {
        val uts = (code \ "@Value").text
        uts
      })

      //ISO code
      val isoCountryCodeNode = codesNode.filter(code => {
        (code \ "@Context").text == "ISOCountryCode"
      })
      val isoCode = (isoCountryCodeNode \ "@Value").text

      //State should be used only for USA
      val stateInUSA = {
        if (isoCode == "US") {
          val stateNode = codesNode.filter(code => {
            (code \ "@Context").text == "StateCode"
          })
          (stateNode \ "@Value").text
        } else {
          ""
        }
      }
      (locationId, locationType, locationNameEN.trim, locationNameRU.trim, utsCodeSeq, isoCode, stateInUSA.trim, locationAjaxNameEN, locationAjaxNameRU )

    } catch {
      case e: Exception => throw new Exception(e.getMessage + " locationNode " + locationNode)
    }

  }

  def extractLocationGeneralInfo(locationNode: Node): LocationGeneralInfo = {
    try {

      val locationId = (locationNode \ "@Id").text
      val locationType = (locationNode \ "@LocationType").text

      //location name
      val namesNode = locationNode \\ "Location" \ "Names" \ "Name"
      //first english name (might be more then one name defined in english that's way take the first one)

      val firstEnglishName = namesNode.filter(name => {
        (name \ "@Language").text == "en"
      }).headOption.getOrElse(<Empty></Empty>)
      val locationNameEN = (firstEnglishName \ "@Value").text
      val locationAjaxNameEN = (firstEnglishName \ "@AjaxString").text

      val firstRussianName = namesNode.filter(name => {
        (name \ "@Language").text == "ru"
      }).headOption.getOrElse(<Empty></Empty>)
      val locationNameRU = (firstRussianName \ "@Value").text
      val locationAjaxNameRU = (firstRussianName \ "@AjaxString").text

      LocationGeneralInfo(locationId,locationType, locationNameEN.trim, locationNameRU.trim, locationAjaxNameEN, locationAjaxNameRU, locationNode)

    } catch {
      case e: Exception => throw new Exception(e.getMessage + " locationNode " + locationNode)
    }

  }

  //extra info comes from <Codes> elemnet
  //  <Code Context="ISOCountryCode" Value="PT"/>
  //  <Code Context="UTS" Value="7495"/>
  //  <Code Value="Russia" Context="RentalCarsCountry"></Code>
  //  <Code Value="Gelendzik" Context="RentalCarsCity"></Code>
  def extractLocationCodesInfo(locationNode: Node): LocationCodesInfo = {
    try {

      //UTS Code
      val codesNode = locationNode \\ "Location" \ "Codes" \ "Code"
      val utsCodeNode = codesNode.filter(code => {
        (code \ "@Context").text == "UTS"
      })
      //if multiple uts codes defined for the same location
      val utsCodeSeq: Seq[String] = utsCodeNode.map(code => {
        val uts = (code \ "@Value").text
        uts
      })

      //ISO code
      val isoCountryCodeNode = codesNode.filter(code => {
        (code \ "@Context").text == "ISOCountryCode"
      })
      val isoCountryCode = (isoCountryCodeNode \ "@Value").text

      //State should be used only for USA
      val stateCodeInUSA = {
        if (isoCountryCode == "US") {
          val stateNode = codesNode.filter(code => {
            (code \ "@Context").text == "StateCode"
          })
          (stateNode \ "@Value").text
        } else {
          ""
        }
      }

      //RentalCars
      val rcCountryNode = codesNode.filter(code => {
        (code \ "@Context").text == "RentalCarsCountry"
      })
      val rcCountryCode = (rcCountryNode \ "@Value").text

      //RentalCars
      val rcCityNode = codesNode.filter(code => {
        (code \ "@Context").text == "RentalCarsCity"
      })
      val rcCityCode = (rcCityNode \ "@Value").text

      LocationCodesInfo(isoCountryCode,stateCodeInUSA,rcCountryCode,rcCityCode,utsCodeSeq)


    } catch {
      case e: Exception => throw new Exception(e.getMessage + " locationNode " + locationNode)
    }

  }

  def extractLocation(locationNode: Node): LocationInfo = {
    val generalInfo  = extractLocationGeneralInfo(locationNode)
    val codesInfo = extractLocationCodesInfo(locationNode)
    LocationInfo(generalInfo ,codesInfo)
  }


  @deprecated
  def addMultipleChildLocation1(originalXML: Node, childCodeList: List[String]) = {

    val node = originalXML
    val childs: Seq[Node] = node.child
    val originalElem:Elem = Elem( node.prefix, node.label, node.attributes, node.scope, false,  childs:_*)
    val newLocations: scala.xml.NodeSeq = childCodeList.map({ locationId =>
      <LocationReference Index="2" Id={locationId}></LocationReference>
    })

    originalElem match {
      case e @ Elem(_, _, _, _, childLocations @ _*) => {
        val changedNodes = childLocations.map {
          case <ChildLocations>{ currentLocations @ _* }</ChildLocations> => {

            var curList = ListBuffer.empty[String]
            currentLocations.foreach(l => {
                val currLocationId = (l \ "@Id").text
                curList += currLocationId
                })
            var uniqueList = childCodeList.filterNot(l => {
                curList.contains(l)
            })

            val uniqueNewLocations: scala.xml.NodeSeq = uniqueList.map({ locationId =>
              <LocationReference Index="2" Id={locationId}></LocationReference>
            })

            <ChildLocations> { currentLocations ++ uniqueNewLocations }</ChildLocations>
          }
          case other => other
        }
        e.copy(child = changedNodes)
      }
      case _ => originalElem
    }
  }

  def addMultipleChildLocation(originalXML: Node, childCodeList: List[String]) = {

    val node = originalXML
    val childs: Seq[Node] = node.child
    val originalElem:Elem = Elem( node.prefix, node.label, node.attributes, node.scope, false,  childs:_*)

    originalElem match {
      case e @ Elem(_, _, _, _, childLocations @ _*) => {
        val changedNodes = childLocations.map {
          case <ChildLocations>{ currentLocations @ _* }</ChildLocations> => {

            val uniqueLocationIds =  collection.mutable.Set.empty[String]
            currentLocations.foreach(l => {
              val currentLocationId = (l \ "@Id").text
              uniqueLocationIds += currentLocationId.trim
            })
            uniqueLocationIds ++= childCodeList.toSet

            val uniqueLocations: scala.xml.NodeSeq = uniqueLocationIds.toList.map(locationId =>
              <LocationReference Index="2" Id={locationId}></LocationReference>)

            <ChildLocations> { uniqueLocations }</ChildLocations>
          }
          case other => other
        }
        e.copy(child = changedNodes)
      }
      case _ => originalElem
    }
  }

  def addUTSCode(originalXML: Node, utsCode: String) = {

    val node = originalXML
    val childs: Seq[Node] = node.child
    val originalElem:Elem = Elem( node.prefix, node.label, node.attributes, node.scope, false,  childs:_*)
    val newCode = <Code Value={utsCode} Context="UTS"></Code>

    originalElem match {
      case e @ Elem(_, _, _, _, childCodes @ _*) => {
        val changedNodes = childCodes.map {
          case <Codes>{ currentCodes @ _* }</Codes> => {
            val ustContext = for {
              item <- currentCodes
              if (item \ "@Context").text == "UTS"
            } yield item
            val result = {if (ustContext.size > 0)
              <Codes> { currentCodes }</Codes>
            else
              <Codes> { currentCodes ++ newCode }</Codes>
            }
            result
          }
          case other => other
        }
        e.copy(child = changedNodes)
      }
      case _ => originalElem
    }
  }


  def addRentalCarsCode(originalXML: Node, value: String, context: String) = {

    val node = originalXML
    val childs: Seq[Node] = node.child
    val originalElem:Elem = Elem( node.prefix, node.label, node.attributes, node.scope, false,  childs:_*)
    val newCode = <Code Value={value} Context={context}></Code>

    originalElem match {
      case e @ Elem(_, _, _, _, childCodes @ _*) => {
        val changedNodes = childCodes.map {
          case <Codes>{ currentCodes @ _* }</Codes> => {
            val rcContext = for {
              item <- currentCodes
              if (item \ "@Context").text == context
            } yield item
            val result = {if (rcContext.size > 0)
              <Codes> { currentCodes }</Codes>
            else
              <Codes> { currentCodes ++ newCode }</Codes>
            }
            result
          }
          case other => other
        }
        e.copy(child = changedNodes)
      }
      case _ => originalElem
    }
  }


  def addHotelCapability(originalXML: Node) = {

    val node = originalXML
    val childs: Seq[Node] = node.child
    val originalElem:Elem = Elem( node.prefix, node.label, node.attributes, node.scope, false,  childs:_*)
    val newCapability = <Capability Type="hotel" Value=""/>

    originalElem match {
      case e @ Elem(_, _, _, _, childCodes @ _*) => {
        val changedNodes = childCodes.map {
          case <Capabilities>{ currentCapabilities @ _* }</Capabilities> => {
            val typesDefined = (currentCapabilities \ "@Type")
            val hotelCapability = for {
              item <- currentCapabilities
              if (item \ "@Type").text == "hotel"
            } yield item
            val result = {if (hotelCapability.size > 0)
              <Capabilities> {currentCapabilities}</Capabilities>
            else
              <Capabilities> {currentCapabilities ++ newCapability}</Capabilities>
            }

            result
          }
          case other => other
        }
        e.copy(child = changedNodes)
      }
      case _ => originalElem
    }
  }

  def addCarCapability(originalXML: Node) = {

    val node = originalXML
    val childs: Seq[Node] = node.child
    val originalElem:Elem = Elem( node.prefix, node.label, node.attributes, node.scope, false,  childs:_*)
    val newCapability = <Capability Type="car" Value=""/>

    originalElem match {
      case e @ Elem(_, _, _, _, childCodes @ _*) => {
        val changedNodes = childCodes.map {
          case <Capabilities>{ currentCapabilities @ _* }</Capabilities> => {
            val typesDefined = (currentCapabilities \ "@Type")
            val carCapability = for {
              item <- currentCapabilities
              if (item \ "@Type").text == "car"
            } yield item
            val result = {if (carCapability.size > 0)
              <Capabilities> {currentCapabilities}</Capabilities>
            else
              <Capabilities> {currentCapabilities ++ newCapability}</Capabilities>
            }

            result
          }
          case other => other
        }
        e.copy(child = changedNodes)
      }
      case _ => originalElem
    }
  }

  def addNameRUIfNotDefined(originalXML: Node, name: String, ajaxString: String ) = {

    val node = originalXML
    val childs: Seq[Node] = node.child
    val originalElem:Elem = Elem( node.prefix, node.label, node.attributes, node.scope, false,  childs:_*)
    val newNameRU = <Name AjaxString={ajaxString}  Value={name} Language="ru" AlternativeSpelling="FALSE"></Name>

    originalElem match {
      case e @ Elem(_, _, _, _, childCodes @ _*) => {
        val changedNodes = childCodes.map {
          case <Names>{ currentNames @ _* }</Names> => {
            val currentNameRU = for {
              item <- currentNames
              if (item \ "@Language").text == "ru"
            } yield item
            val result = {if (currentNameRU.size > 0)
              <Names> {currentNames}</Names>
            else
              <Names> {currentNames ++ newNameRU}</Names>
            }

            result
          }
          case other => other
        }
        e.copy(child = changedNodes)
      }
      case _ => originalElem
    }
  }

  def addNameRU(originalXML: Node, name: String, ajaxString: String ) = {

    val node = originalXML
    val childs: Seq[Node] = node.child
    val originalElem:Elem = Elem( node.prefix, node.label, node.attributes, node.scope, false,  childs:_*)
    val newNameRU = <Name AjaxString={ajaxString}  Value={name} Language="ru" AlternativeSpelling="FALSE"></Name>

    originalElem match {
      case e @ Elem(_, _, _, _, childCodes @ _*) => {
        val changedNodes = childCodes.map {
          case <Names>{ currentNames @ _* }</Names> => {
              <Names> {currentNames ++ newNameRU}</Names>
          }
          case other => other
        }
        e.copy(child = changedNodes)
      }
      case _ => originalElem
    }
  }

  def removeNameRU(originalXML: Node) = {
    val removeIt = new RewriteRule {
      override def transform(n: Node): NodeSeq = n match {
        case e: Elem if ((e \\ "Name" \\ "@Language").text == "ru") => {
          NodeSeq.Empty
        }
        case n => n
      }
    }
    (new RuleTransformer(removeIt).transform(originalXML)).head
  }



  def removeUTSCode(originalXML: Node) = {
    val removeIt = new RewriteRule {
      override def transform(n: Node): NodeSeq = n match {
        case e: Elem if ((e \\ "Code" \\ "@Context").text == "UTS") => {
          NodeSeq.Empty
        }
        case n => n
      }
    }
    (new RuleTransformer(removeIt).transform(originalXML)).head
  }

  def removeHotelCapability(originalXML: Node) = {
    val removeIt = new RewriteRule {
      override def transform(n: Node): NodeSeq = n match {
        case e: Elem if ((e \\ "Capability" \\ "@Type").text == "hotel") => {
          NodeSeq.Empty
        }
        case n => n
      }
    }
    new RuleTransformer(removeIt).transform(originalXML).head
  }

  def updateLocationAttr(node: Node) = {
    val locationId = (node \ "@Id").text
    val onSale = (node \ "@OnSale").text
    val tz = (node \ "@TZ").text
    val hideName   = (node \ "@HideName").text
    val locationType   = (node \ "@LocationType").text
    val version   = (node \ "@Version").text
    val searchable   = (node \ "@Searchable").text
    val indexNumber   = (node \ "@IndexNumber").text

    val updatedNode = node match {
      case <Location>{values @ _*}</Location> =>
        <Location Id ={locationId} OnSale={onSale} ignoreDiff="true" TZ={tz} HideName={hideName}  LocationType={locationType} Version={version} Searchable={searchable} IndexNumber={indexNumber} > {values}</Location>
    }
    updatedNode
  }


  def populateMapsUtsCityCode() = {

    val mapUTSToLocationId = collection.mutable.Map.empty[String, List[String]]
    val mapLocationIdToUts = collection.mutable.Map.empty[String, List[String]]

    val listNonCountryType = List("city","airport", "region")

    //non country (uts city codes might be mapped to: city, airport, region
    locationNodes
      .filter(n => {
          val locationType = (n \ "@LocationType").text
          (locationType != "" && listNonCountryType.contains(locationType))
        })
      .foreach(n => {

      val (locationId, locationType, locationName, _, utsCodeSeq, _, _, _, _) = extractLocationInfoUTS(n)
      utsCodeSeq.size match {
        case 1 => {
          val utsCityCode = utsCodeSeq.head
          if (mapUTSToLocationId.isDefinedAt(utsCityCode)) {
            val message = s"Duplicated UTSCityCodes: $utsCityCode. " +
              s"Currently processed location: [locationId: $locationId, locationType: $locationType, locationName: $locationName]. " +
              s"Already defined location: [locationId: ${mapUTSToLocationId(utsCityCode)}]"
            logger.error(message)
            val alreadyDefinedList = mapUTSToLocationId(utsCityCode)
            mapUTSToLocationId.put(utsCityCode, (locationId :: alreadyDefinedList))
            //throw new Exception(message)
          }
          mapLocationIdToUts.put(locationId, utsCodeSeq.toList)
          mapUTSToLocationId.put(utsCityCode, List(locationId))
        }
        case 0 => {
          val message = ""
        }
        case _ => {
          val message = s"Multiple UTSCityCodes: ${utsCodeSeq.mkString(",")} defined for location [locationId: $locationId, locationType: $locationType, locationName: $locationName]"
          logger.error(message)
          throw new Exception(message)
        }
      }
    })
    (mapUTSToLocationId, mapLocationIdToUts)
  }

  def populateMapsUtsCountryCode() = {

    val mapUTSToLocationId = collection.mutable.Map.empty[String, List[String]]
    val mapLocationIdToUts = collection.mutable.Map.empty[String, List[String]]

    //only country
    countries
      .foreach(n => {
        val (locationId, locationType, locationName, _, utsCodeSeq, _, _, _, _) = extractLocationInfoUTS(n)
        utsCodeSeq.size match {
        case 1 => {
          val utsCode = utsCodeSeq.head
          if (mapUTSToLocationId.isDefinedAt(utsCode)) {
            val alreadyDefinedList = mapUTSToLocationId(utsCode)
            val message = s"Duplicated UTSCountryCodes: $utsCode. Currently processed location: [locationId: $locationId, locationType: $locationType, locationName: $locationName]. Already defined location: [locationId: ${mapUTSToLocationId(utsCode)}]"
            logger.error(message)
            mapUTSToLocationId.put(utsCode, (locationId :: alreadyDefinedList))
            throw new Exception(message)
          }
          mapLocationIdToUts.put(locationId, utsCodeSeq.toList)
          mapUTSToLocationId.put(utsCode, List(locationId))
        }
        case 0 => {
          val message = ""
        }
        case _ => {
          val message = s"Multiple UTSCountryCodes: ${utsCodeSeq.mkString(",")} defined for location [locationId: $locationId, locationType: $locationType, locationName: $locationName]"
          //ic case country such situation is allowed
          // e.g China has defined two UTSCountryCode (the first China and the second Macau which is marked as country by UTS supplier)
          logger.debug(message)
        }
      }
    })
    (mapUTSToLocationId, mapLocationIdToUts)
  }

  def generateNewLocationId(ustName: String, isoCode: String, utsCode: String ) = {
    val name = ustName.replaceAll( "\\W","").trim.take(4).toLowerCase.capitalize
    val iso = isoCode.toLowerCase.capitalize
    //S7_XLOCATION"."HIERARCHY_ROWS"."LOCATION_ID" maximum: 16
    val locationId = ("Cit" + iso + name + utsCode).take(16)

    locationId
  }


  def getLocationNodeByLocationId(locationId: String) = {
    val result = locationNodes.filter(node => (extractLocation(node).generalInfo.locationId == locationId))

    result.size match {
      case 0 => {
        logger.error(s"No location found for locationId: $locationId")
        throw new Exception(s"No location found for locationId: $locationId")
      }
      case count: Int => {
        if (count > 1) {
          logger.error(s"Multiple locations ($count) for locationId: $locationId")
          throw new Exception(s"Multiple locations ($count) locationId: $locationId")
        }
      }
    }
    result.head
  }

  def getCountryNodeByItsChild(childNode: Node) = {
    getParentId(childNode)
  }

  def getParentId(childNode: Node): Option[Node] = {
    def extractParent(node: Node) = {
      val parentLocations = (node \\ "ParentLocations" \ "LocationReference")
      // the first but non-continent location
      val parentLocation = parentLocations.filter(n => {
        val parentId = (n \ "@Id").text

        val result = if (locationLookup.isDefinedAt(parentId)) {
          val parentNode = locationLookup(parentId).generalInfo.node
          val locationType = (parentNode \ "@LocationType").text
          (locationType != "continent")
        } else {
          val message = s"Parent not found in Location Hierarchy for location [locationNode: $parentId] "
          logger.error(message)
          false
        }
        result
        })
        parentLocation.headOption
    }

    val parentLocation = extractParent(childNode).getOrElse(<Empty></Empty>)
    val parentId = (parentLocation \ "@Id").text
    val childId = (childNode \ "@Id").text

    val result = {
      if (!locationLookup.isDefinedAt(parentId) || parentId == "" ) {
        val message = s"Parent not found in Location Hierarchy for location [locationNode: $childNode] "
        logger.error(message)
        None
        //throw new Exception(message)
      } else {
        val parentNode = locationLookup(parentId).generalInfo.node
        val locationType = (parentNode \ "@LocationType").text
        locationType match {
          case "country" => {
            //logger.debug(s"Found country [parentId: $parentId, childId: $childId]")
            Some(parentNode)
          }
          case "continent" => {
            logger.debug(s"Location of type country not found for [parentId: $parentId, childId: $childId, parentLocationType: $locationType ]")
            None
          }
          case _ => {
            //logger.debug(s"Continue search [parentId: $parentId, childId: $childId]")
            getParentId(parentNode)
          }
        }
      }
    }
    result
  }

  def getChildToItsCountryMap(locationType: String) = {
    logger.info("About to populate map that contains key: childLocation, value: ItsCountry")
    val mapFromChildToItsCountry = collection.mutable.Map.empty[String, String]
    //all location except country and continent ones
    locationNodes
      .filter(n => (extractLocation(n).generalInfo.locationType == locationType))
      .map(n => {
        val childId = extractLocation(n).generalInfo.locationId
        val countryNode = getCountryNodeByItsChild(n)
        val countryId = countryNode match {
            case Some(node) => extractLocation(node).generalInfo.locationId
            case None => ""
        }
        mapFromChildToItsCountry.put(childId, countryId)
    })
   logger.info(s"Map was populated and its size: ${mapFromChildToItsCountry.size}")
   mapFromChildToItsCountry.toMap
  }

  def getAirportToItsCountryMap = {
    logger.info("About to populate map that contains key: airport, value: ItsCountry")
    val mapFromChildToItsCountry = collection.mutable.Map.empty[String, String]
    //all location except country and continent ones
    airports
      .map(n => {
      val childId = extractLocation(n).generalInfo.locationId
      val countryNode = getCountryNodeByItsChild(n)
      val countryId = countryNode match {
        case Some(node) => extractLocation(node).generalInfo.locationId // locationId
        case None => ""
      }
      mapFromChildToItsCountry.put(childId, countryId)
    })
    logger.info(s"Map was populated and its size: ${mapFromChildToItsCountry.size}")
    mapFromChildToItsCountry.toMap
  }

  def getLocationToItsCountryMap = {
    logger.info("About to populate map that contains key: non-country-location, value: ItsCountry")
    val mapFromChildToItsCountry = collection.mutable.Map.empty[String, String]
    val listNonCountryType = List("city","airport", "region")

    //non country (uts city codes might be mapped to: city, airport, region
    locationNodes
      .filter(n => {
        val locationType = (n \ "@LocationType").text
        (locationType != "" && listNonCountryType.contains(locationType))
        })
      .map(n => {
        val childId = extractLocation(n).generalInfo.locationId
        val countryNode = getCountryNodeByItsChild(n)
        val countryId = countryNode match {
            case Some(node) => extractLocation(node).generalInfo.locationId
            case None => ""
          }
        mapFromChildToItsCountry.put(childId, countryId)
    })
    logger.info(s"Map was populated and its size: ${mapFromChildToItsCountry.size}")
    mapFromChildToItsCountry.toMap
  }

  def getLocationLookup = {
    logger.info("About to populate map that contains key: locationId, value: LocationInfo")
    val lookup = collection.mutable.Map.empty[String, LocationInfo]
    locationNodes.foreach(node => {
      val locationInfo = extractLocation(node)
      val locationId = locationInfo.generalInfo.locationId
      lookup.put(locationId, locationInfo)
      })
    logger.info(s"Map was populated and its size: ${lookup.size}")
    lookup.toMap
  }

  def getStatesInUSALookup = {
    logger.info("About to populate map that contains key: StateCodeInUSA, value: LocationId")
    val lookup = collection.mutable.Map.empty[String, String]
    states.foreach(node => {
      val locationInfo = extractLocation(node)
      val locationId = locationInfo.generalInfo.locationId
      val codesInfo = locationInfo.codesInfo
      val stateCodeInUSA = codesInfo.stateCodeInUSA
      val isoCountryCode = codesInfo.isoCountryCode
      if (isoCountryCode == "US")
        lookup.put(stateCodeInUSA, locationId)
    })
    logger.info(s"Map was populated and its size: ${lookup.size}")
    lookup.toMap
  }

}