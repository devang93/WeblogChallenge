import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.Instant

/**
  * Created by ddevang93 on 6/17/2019.
  */
object Main {

  case class LogEvent(ip: String, log: String)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                .appName("PayTm_WeblogAnalytics")
                .master("local").getOrCreate()

    import spark.implicits._

    val inputLogLines = spark.read.textFile("./data/*")

    val inputLogEvents = inputLogLines.map(line => LogEvent(line.split(" ")(2).split(":")(0), line))

    // Sessionize Data by creating a session_id field for each record. Session_id = ip+session_start_time

    // SessionLength defines the inactivity period to consider for a session's end.
    val sessionLength = 15 * 60

    /***
      *  Function to create sessionIds for each sessionLog given list of all the sessionLogs for an IP.
      *
      *  SessionId Creation Logic:
      *
      *  Given all the sessionLogs for an Ip, function first orders them by log timestamp. Then we create the sessionId for a sessionLog
      *  by simply calculating the time difference between a sessionLog's timestamp and its previous sessionLog's timestamp.
      *  If the difference between timestamps is within the SessionLength then we do consider them as part of same session
      *  otherwise a new session period starts from that sessionLog.
      *
      *  For the first sessionLog of an event we create the sessionId by combining IP+_+(first session's timestamp in epoch seconds)
      */
    val sessionCreator : Seq[String] => Seq[(String, Long, String)] = (logs) =>{

      // sort all sessionLogs by session's timestamp.
      val sortedLogs = logs.map(tuple => {
        (Instant.parse(tuple.split(" ")(0)).getEpochSecond, tuple)
      }).sortBy(_._1)

      // Generate first SessionLog's sessionId.
      val headSessionID = sortedLogs.head._2.split(" ")(2).split(":")(0)+"_"+sortedLogs.head._1

      // Initial Zero Accumulator for the foldLeft function over sortedLogs List.
      val sessionizedLogs = (List((headSessionID, sortedLogs.head._1, sortedLogs.head._2)), (headSessionID, sortedLogs.head._1, sortedLogs.head._2))

      // Generate list of all sessionLogs with sessionId.
      val (sessionLogsWithSessionId, lastSessionLog) = sortedLogs.tail.foldLeft(sessionizedLogs) {
        (acc, current) => {
          if ((current._1 - acc._2._2) <= sessionLength)
            (acc._1 :+ (acc._2._1, current._1, current._2), (acc._2._1, current._1, current._2))
          else {
            val newSessionId = current._2.split(" ")(2).split(":")(0) + "_" + current._1
            (acc._1 :+ ((newSessionId, current._1, current._2)), (newSessionId, current._1, current._2))
          }
        }
      }
      sessionLogsWithSessionId
    }

    // register Spark UDF.
    val createSession = udf(sessionCreator)

    // Create List of all sessionLogs by IP.
    val groupedSessionLogsbyIp = inputLogEvents.groupBy("ip").agg(collect_list("log") as "Logs")

    // Task 1: Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.

    val sessionizedLogsDF = groupedSessionLogsbyIp.withColumn("sessionizedLogs", createSession(col("Logs")))
              .withColumn("sessionLogs", explode(col("sessionizedlogs")))
              .withColumn("sessionId", col("sessionLogs").getField("_1"))
              .withColumn("logTime", col("sessionLogs").getField("_2"))
              .select("ip", "logtime", "sessionId", "sessionLogs")
              .cache()

    println(" ----- SESSIONIZED LOGS ----- ")
    sessionizedLogsDF.show()

    /**
      * For any future analytics or using this sessionized data again we should consider writing it to a persistent storage as well.
      * While writing to storage we can partition the data by sessionLogs Date.
      *
      * sessionizedLogsDF.write.format("parquet").save("/data/sessionizedLogs/")
      */


    // Collect all sessions for an IP for all the subsequent analytics.
    val sessionDurationsByIpDF = sessionizedLogsDF.groupBy("ip", "sessionId")
                 .agg(max("logtime")-min("logtime") as "sessionDuration")
                 .groupBy("ip").agg(collect_list("sessionDuration") as "sessionDurations")

    // Task 2: Average Session Duration per IP.

    // Spark UDF to get Average Session Duration per IP given List of all the sessionDurations for an IP.
    val avgColumn = udf ((arrayInput: Seq[Long]) => arrayInput.foldLeft(0.0)(_ + _) / arrayInput.length)

    val avgSessionDurationByIpDF = sessionDurationsByIpDF.withColumn("AvgSessionDuration", avgColumn(col("sessionDurations")))

    println(" ----- Average Session Duration By IP ----- ")
    avgSessionDurationByIpDF.show()

    // Task 3: Find the most engaged users, ie the IPs with the longest session times

    // Spark UDF to get longest Session Duration per IP.
    val maxColumn = udf ((arrayInput: Seq[Long]) => arrayInput.max)

    val longestSessionDurationByIpDF = sessionDurationsByIpDF.withColumn("longestSessionDuration", maxColumn(col("sessionDurations")))
                                                             .orderBy(col("longestSessionDuration").desc)

    println(" ----- Most Engaged Users / IP with Longest Session Durations ----- ")
    longestSessionDurationByIpDF.show(50, false)

    // Task 4: Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

    // Spark UDF to get URL from given sessionLog.
    val getURL = udf ((sessionLog: String) => sessionLog.split(" ")(12))

    val uniqueURLVisitsBySessionDF = sessionizedLogsDF.withColumn("URL", getURL(col("sessionLogs").getField("_3")))

    println(" ----- Unique URL Visits per Session ----- ")
    uniqueURLVisitsBySessionDF.select("ip", "Logtime", "SessionId", "URL")
                              .groupBy("ip", "sessionId")
                              .agg(collect_set(col("URL")))
                              .show(50, false)

    /***
      * Note: Unique IP doesn't conform to unique user, instead if we can get some more information from the logs.
      * If possible we should try to catch user login credentials (Username/Email) if its logged in session.
      */
  }
}
