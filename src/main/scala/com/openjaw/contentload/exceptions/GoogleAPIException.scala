package com.openjaw.contentload.exceptions

object GoogleAPIException {

  case class GeocodingAPIException(s: String) extends Exception(s)

  case class QueryLimitException(s: String) extends Exception(s)

  case class ZeroResultsException(s: String) extends Exception(s)


}

