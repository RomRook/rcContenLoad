package com.openjaw.connectors

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scalaj.http.{Http, HttpRequest}

object GoogleConnector {

  val logger = Logger(LoggerFactory.getLogger("GoogleConnector"))

  def getRussianNameAndGeoCodes(query: String) = {

    val baseUrl = "http://maps.google.com/maps/api/geocode/xml?language=ru&address="
    val fullUrl = baseUrl + query.replaceAll("\\s", "+")

    logger.debug(s"HTTP REQUEST: $fullUrl")
    val response = sendRequest(fullUrl)
    logger.debug(s"HTTP RESPONSE: $response")
    val status = (response \ "status").text
    logger.debug(s"RESPONSE STATUS: $status")

    status match {
      case "OK" =>
      case "ZERO_RESULTS" => logger.error(s"No result found for '$fullUrl'")
      case "OVER_QUERY_LIMIT" => logger.error("Daily query limit of 2500 was reached")
      case "HTTP_ERROR" => logger.error("Error while sending Http request")
      case otherStatus => logger.error("Google returned this message for: " + query + " - " + otherStatus)
    }

    val tuple4 = if (status == "OK") {
      val addressNodes = (response \ "result" \ "address_component")
      val russianName = (addressNodes.head \ "long_name").text
      val latitude = (response \ "result" \ "geometry" \ "location" \ "lat").head.text
      val longitude = (response \ "result" \ "geometry" \ "location" \ "lng").head.text
      (russianName, latitude, longitude, (response \ "result").size)
    } else
      ("", 0, 0, (response \ "result").size)

    tuple4

  }

  def sendRequest(url: String) = {
    val request: HttpRequest = Http(url).timeout(connTimeoutMs = 1000, readTimeoutMs = 10000)

    logger.debug("About to send request to Google API: " + url.toString())
    val response = request.execute(parser = {inputStream => scala.xml.XML.load(inputStream)})
    logger.debug(" response from xDist: " + response)

    if (response.isSuccess) {
      response.body
    }
    else {
      logger.error(s"Http error: ${response.code} while sending request: ${request.asString}")
      <status>"HTTP_ERROR"</status>
    }
  }

}
