package com.openjaw.connectors

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.xml.Node
import scalaj.http.{Http, HttpRequest}


class RentalCarsConnector(endpoint: String, username: String, password: String, remoteIp: String) {

  val logger = Logger(LoggerFactory.getLogger("RentalCarsConnector"))

  def getCountries() = {
    val countryListRQ =
      <PickUpCountryListRQ version="1.1">
        <Credentials username={username} password={password} remoteIp={remoteIp}/>
      </PickUpCountryListRQ>

    val response = sendRequest(countryListRQ)
    val nodes = (response \\ "Country")
    logger.error(s"Number of countries received from supplier: ${nodes.size}")
    if (nodes.size == 0) {
      logger.error("Empty country response from supplier")
      throw new RuntimeException("ERROR: Empty country response from supplier!")
    }
    nodes
 }

  def getCities(country: String, language: String = "en") = {
    val cityListRQ =
      <PickUpCityListRQ version="1.1" preflang={language}>
      <Credentials username={username} password={password} remoteIp="91.151.7.6"/>
      <Country>{country}</Country>
    </PickUpCityListRQ>

    val response = sendRequest(cityListRQ)
    val nodes = (response \\ "City")
    logger.info(s"Number of cities for '$country' received from supplier: ${nodes.size} ")

    if (nodes.size == 0) {
      logger.error("Empty city response from supplier")
      throw new RuntimeException("ERROR: Empty city response from supplier!")
    }
    nodes
  }

  def getDepots(country: String, city: String, language: String = "en") = {
    val depotListRQ =
      <PickUpLocationListRQ version="1.1" preflang={language}>
        <Credentials username={username} password={password} remoteIp={remoteIp}/>
        <Country>{country}</Country>
        <City>{city}</City>
      </PickUpLocationListRQ>

    val response = sendRequest(depotListRQ)
    val nodes = (response \\ "Location")
    logger.debug(s"Number of depots for '$country' in city '$city' received from supplier: ${nodes.size} ")

    if (nodes.size == 0) {
      logger.error("Empty depots response from supplier")
      logger.error(s"Request: $depotListRQ \nResponse: $response")
      //throw new RuntimeException("ERROR: Empty depots response from supplier!")
    }
    nodes
  }

  private def sendRequest (req: Node) = {
    val data = s"xml=$req"
    val request: HttpRequest = Http (endpoint)
      .postData(data)
      .timeout(connTimeoutMs = 2000, readTimeoutMs = 5000)

    logger.debug (s"About to send request to: $endpoint postData:\n $data")
    val response = request.execute(parser = { inputStream => scala.xml.XML.load(inputStream) })
    logger.debug("Request was successfully sent")
    val body = response.body
    logger.debug(s"Response: $response")

    if (response.isSuccess) {
      body
    }
    else
    {
      logger.error("Error while sending request: " + request.asString.code)
      throw new Exception("ERROR: " + request.asString)
    }
  }


}
