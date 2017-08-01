package com.openjaw.contentload

import java.util.Date

import akka.actor._
import ch.qos.logback.classic.{Level => LogbackLevel, Logger => LogbackLogger}
import com.openjaw.connectors.DistributorConnector
import com.typesafe.scalalogging.Logger
import org.apache.commons.cli.{CommandLine, CommandLineParser, DefaultParser, HelpFormatter, Option => CLIOption, Options => CLIOptions}
import org.slf4j.LoggerFactory

import scala.xml.{Node, NodeSeq, XML}

case class Supervisor(name: String, countryName: String, requestType: String, nodesToBeSent: NodeSeq)
case class Progress(sentCount: Int, processedCount: Int, successCount: Int, errorCount: Int)
case class SenderProgress(processedCount: Int, successCount: Int, errorCount: Int)
case object Finish

class Main(config: Config) extends Actor {

  val logger = Logger(LoggerFactory.getLogger("Main"))

  var supervisors: List[ActorRef] = List.empty
  val supervisorProgress = collection.mutable.Map.empty[AnyRef, Progress]
  //statistics based on all actors
  var totalReceivedCount = 0
  var totalProcessedCount = 0
  var totalSuccessCount = 0
  var totalErrorCount = 0

  def receive = {
    case supervisor: Supervisor => createSupervisor(supervisor)
    case progress: Progress => {
      logger.debug(s"${sender.toString()}: delegated: ${progress.sentCount}, processed: ${progress.processedCount}, successCount: ${progress.successCount}, errorCount: ${progress.errorCount}")
      supervisorProgress.put(sender, progress)
      totalProcessedCount = supervisorProgress.map(pair => pair._2.processedCount).sum
      totalSuccessCount = supervisorProgress.map(pair => pair._2.successCount).sum
      totalErrorCount = supervisorProgress.map(pair => pair._2.errorCount).sum

      logger.info(s"Current status: $totalProcessedCount/$totalReceivedCount")
      if (totalProcessedCount == totalReceivedCount) {
        logger.info(s"FINISHED - messages processed: [success: ${totalSuccessCount}/${totalReceivedCount}, errors: ${totalErrorCount}/${totalReceivedCount}]")
      }
    }
    case other => logger.error(s"MainActor got not handled message: '$other'")
  }

  def createSupervisor(supervisorName: String, requestType: String, nodesToBeSent: NodeSeq) = {
    val distributorEndpoint = config.distributorEndpoint
    val distributorTimeout = config.distributorTimeout.toInt
    val numberOfWorkers = 3

    val supervisorActor = context.actorOf(Props(new DistributorConnector.Supervisor(supervisorName, requestType, nodesToBeSent, distributorEndpoint, distributorTimeout, numberOfWorkers)), supervisorName)
    supervisors = supervisorActor :: supervisors
    totalReceivedCount += nodesToBeSent.size

    supervisorActor ! "DistributeRequests"
  }

  def createSupervisor(supervisor: Supervisor) = {
    val distributorEndpoint = config.distributorEndpoint
    val distributorTimeout = config.distributorTimeout.toInt
    val distributorWorkers = config.distributorWorkers.toInt

    val supervisorName = {supervisor.name + supervisor.countryName}

    val supervisorActor = context.actorOf(Props(new DistributorConnector.Supervisor(supervisorName, supervisor.requestType, supervisor.nodesToBeSent, distributorEndpoint, distributorTimeout, distributorWorkers)), supervisorName)
    supervisors = supervisorActor :: supervisors
    totalReceivedCount += supervisor.nodesToBeSent.size
    supervisorActor ! "DistributeRequests"
  }
}

object Main  {

  val logger = Logger(LoggerFactory.getLogger("Main"))

  private val CONFIG_OPTION = "config"
  private val ALGORITHM_OPTION = "algo"
  private val MODE_OPTION = "mode"
  private val ISO_COUNTRY_CODE_OPTION = "iso"

  val system = ActorSystem("the-actor-system")

  def main(args: Array[String]): Unit = {

    val root: LogbackLogger =  LoggerFactory.getLogger("root").asInstanceOf[LogbackLogger]
    val logLevel = System.getProperty("log.level")
    root.setLevel(LogbackLevel.toLevel(logLevel,LogbackLevel.INFO))

    logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    logger.info("Process STARTED")
    val optionDef = getOptionsDef //definition for option provided in command line
    commandLineUsage(optionDef)

    logger.info("Args: " +  args.toList)

    val options = getOptionsValues(args, getOptionsDef)
    val config = getConfig(options)
    config.algorithm = getAlgorithmType(options)
    config.mode = getMode(options)
    config.countyIsoCode = getCountryIsoCode(options)

    logger.info("Algorithm: " + config.algorithm)
    logger.info("Mode: " + config.mode)
    logger.info("ISOCountryCode: " + config.countyIsoCode)

    //val locHelper = getDefaultHierarchy(directory = config.defaultHierarchy)

    logger.info(s"About to get DefaultHierarchy")
    val dhFromFile = getDefaultHierarchyFromFile(directory = config.defaultHierarchy)

    val locHelper = dhFromFile match {
      case Some(dh) => {
        logger.info(s"DefaultHierarchy loaded from file [directory: ${config.defaultHierarchy}]")
        dh
      }
      case None => {
        logger.info(s"About to get DefaultHierarchy from xDist [endpoint: ${config.distributorEndpoint}]")
        val (xDistRS, isSuccess) = DistributorConnector.getDefaultHierarchy(config.distributorEndpoint, config.distributorTimeout.toInt)
        if (!isSuccess) throw new RuntimeException("Error while getting DefaultHierarchy from xDist")
        val dh = getDefaultHierarchyFromResponse(xDistRS)
        logger.info(s"DefaultHierarchy loaded from xDist response")
        dh
      }
    }


    val mainActor = {if (config.mode == Mode.SendRequest)
                      Some(system.actorOf(Props(new Main(config)), name = "mainActor"))
                    else None}

    val algorithm = Algorithm(locHelper, config, mainActor)
    val reportXML = algorithm.run

    saveReportToFile(reportXML,config)

    commandLoop()
    system.shutdown()

    logger.info("Process FINISHED")
    logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
  }

  def commandLoop(): Unit = {
    Console.readLine() match {
      case "q"          => return
      case _               => println("Type 'q' to quit program")
    }
    commandLoop()
  }

  def saveReportToFile(report: Node, config: Config) = {
    val printer = new scala.xml.PrettyPrinter(250, 2)
    val date: Date = new Date()
    val formatter = new java.text.SimpleDateFormat("yyMMddHHmmss")
    val fileNameAndPath = {
      if (config.countyIsoCode != "")
        config.outDir + "Report_" + config.algorithm + "_" + config.countyIsoCode + "_" + formatter.format(date) + ".xml"
      else
        config.outDir + "Report_" + config.algorithm + "_" + formatter.format(date) + ".xml"
    }
    XML.save(filename = fileNameAndPath, XML.loadString(printer.format(report)), "UTF-8")
    logger.info(s"Report saved to file $fileNameAndPath")
  }

  def getConfig(cmdLine: CommandLine) = {
    val pathToConfigXML =
      if (cmdLine.hasOption(CONFIG_OPTION))
        cmdLine.getOptionValue(CONFIG_OPTION)
       else "rc_config.xml"
    val configXML = XML.loadFile(pathToConfigXML)
    //throw new Exception("Error: Missing configuration file. Provide path to config.xml as the first parameter")
    Config(configXML)
  }

  def getAlgorithmType(cmdLine: CommandLine) = {
    if (cmdLine.hasOption(ALGORITHM_OPTION))
         cmdLine.getOptionValue(ALGORITHM_OPTION)
    else "NONE"
  }

  def getMode(cmdLine: CommandLine) = {
    val mode = if (cmdLine.hasOption(MODE_OPTION))
                  cmdLine.getOptionValue(MODE_OPTION)
               else "FILE_ONLY"
    mode match {
      case "FILE_ONLY" => Mode.SaveToFile
      case "SEND_RQ" => Mode.SendRequest
      case _ => Mode.None
    }
  }

  def getCountryIsoCode(cmdLine: CommandLine) = {
    if (cmdLine.hasOption(ISO_COUNTRY_CODE_OPTION))
      cmdLine.getOptionValue(ISO_COUNTRY_CODE_OPTION)
    else ""
  }

  def getDefaultHierarchyFromFile(directory: String) = {
    try {
      val path = directory + "DefaultHierarchy.xml"
      val defaultHierarchyXML = XML.loadFile(path)
      logger.info("DefaultHierarchy.xml file loaded")
      val dh = new DefaultHierarchyHelper(defaultHierarchyXML)
      Some(dh)
    } catch {
      case e: java.io.FileNotFoundException => logger.error(e.getMessage)
        None
    }
  }

  def getDefaultHierarchyFromResponse(xDistResponse: NodeSeq) = {
    new DefaultHierarchyHelper(xDistResponse)
  }

  def getDefaultHierarchy(directory: String) = {
    val path = directory + "DefaultHierarchy.xml"
    val defaultHierarchyXML = XML.loadFile(path)
    logger.info("DefaultHierarchy.xml file loaded")
    new DefaultHierarchyHelper(defaultHierarchyXML)
  }

  def getOptionsValues(args: Array[String], options: CLIOptions): CommandLine = {
    val parser: CommandLineParser = new DefaultParser()
    val line: CommandLine = parser.parse(options, args)
    line
  }

  def getOptionsDef() = {
    val config = new CLIOption(CONFIG_OPTION,true, "path and filename to configuration file, e.g. /home/user/rc_config.xml")
    val algo = new CLIOption(ALGORITHM_OPTION, true, "implemented algorithms are as follows: TEMPLATE, COUNTRY, CITY, DEPOT")
    val mode = new CLIOption(MODE_OPTION, true, "mode option might be: \n" +
      "FILE_ONLY - files with requests will be saved, \n" +
      "SEND_RQ - requests to xDist will be sent)")
    val iso = new CLIOption(ISO_COUNTRY_CODE_OPTION, true,  s"" +
      s"1. e.g. for one country: RU \n" +
      s"2. multiple countries : ${"RU|PL|FR|IT"} \n" +
      s"3. If value left empty or option not specified at all then all countries will be processed")

    val options = new CLIOptions()
    options.addOption(config)
    options.addOption(algo)
    options.addOption(mode)
    options.addOption(iso)

    options
  }

  def commandLineUsage(options: CLIOptions) = {
    // automatically generate the help statement
    val formatter: HelpFormatter = new HelpFormatter()
    formatter.setWidth(200)
    formatter.printHelp("java -jar <rc.contentload>", options)
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

}
