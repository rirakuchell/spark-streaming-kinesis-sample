// scalastyle:off println

import java.nio.ByteBuffer
import scala.collection.JavaConverters._

import scala.util.Random

import com.amazonaws.auth.{BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds,Time, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD

// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql.SaveMode.Append


/**
  * Consumes messages from a Amazon Kinesis streams and does wordcount.
  *
  * This example spins up 1 Kinesis Receiver per shard for the given stream.
  * It then starts pulling from the last checkpointed sequence number of the given stream.
  *
  * Usage: KinesisWordCountASL <app-name> <stream-name> <endpoint-url> <batch-interval> <checkpoint-interval> <s3-save-path>
  *   <app-name> is the name of the consumer app, used to track the read data in DynamoDB
  *   <stream-name> name of the Kinesis stream (ie. mySparkStream)
  *   <endpoint-url> endpoint of the Kinesis service
  *     (e.g. https://kinesis.us-east-1.amazonaws.com)
  *
  *
  * Example:
  *      # export AWS keys if necessary
  *      $ export AWS_ACCESS_KEY_ID=<your-access-key>
  *      $ export AWS_SECRET_KEY=<your-secret-key>
  *
  *      # run the example
  *      $ SPARK_HOME/bin/run-example  streaming.KinesisWordCountASL myAppName  mySparkStream \
  *              https://kinesis.us-east-1.amazonaws.com
  *
  * There is a companion helper class called KinesisWordProducerASL which puts dummy data
  * onto the Kinesis stream.
  *
  * This code uses the DefaultAWSCredentialsProviderChain to find credentials
  * in the following order:
  *    Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
  *    Java System Properties - aws.accessKeyId and aws.secretKey
  *    Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
  *    Instance profile credentials - delivered through the Amazon EC2 metadata service
  * For more information, see
  * http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html
  *
  * See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more details on
  * the Kinesis Spark Streaming integration.
  * https://github.com/apache/spark/blob/4f77c0623885d4a7455f9841a888d9f6e098e7f0/external/kinesis-asl/src/main/scala/org/apache/spark/examples/streaming/KinesisWordCountASL.scala
  */
object KinesisWC extends Logging {
  def main(args: Array[String]) {
    // Check that all required args were passed in.
    if (args.length != 4) {
      System.err.println(
        """
          |Usage: KinesisWordCountASL <app-name> <stream-name> <endpoint-url> <s3-save-path>
          |
          |    <app-name> is the name of the consumer app, used to track the read data in DynamoDB
          |    <stream-name> is the name of the Kinesis stream
          |    <endpoint-url> is the endpoint of the Kinesis service
          |                   (e.g. https://kinesis.us-east-1.amazonaws.com)
          |    <s3-save-path> is the path that Kinesis records will be saved
          |Generate input data for Kinesis stream using the example KinesisWordProducerASL.
          |See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more
          |details.
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    // Populate the appropriate variables from the given args
    val Array(appName, streamName, endpointUrl, s3path) = args

    // Determine the number of shards from the stream using the low-level Kinesis Client
    // from the AWS Java SDK.
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified " +
        "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size

    // In this example, we're going to create 1 Kinesis Receiver/input DStream for each shard.
    // This is not a necessity; if there are less receivers/DStreams than the number of shards,
    // then the shards will be automatically distributed among the receivers and each receiver
    // will receive data from multiple shards.
    val numStreams = numShards

    // Spark Streaming batch interval
    val batchInterval = Milliseconds(10000)

    // Kinesis checkpoint interval is the interval at which the DynamoDB is updated with information
    // on sequence number of records that have been received. Same as batchInterval for this
    // example.
    val kinesisCheckpointInterval = Milliseconds(10000)

    // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
    // DynamoDB of the same region as the Kinesis stream
    val regionName = getRegionNameByEndpoint(endpointUrl)

    val checkpointDirectory = "file:///C:/Users/sysadmin/workspace/spark-streaming-kinesis-sample/sparkstreaming-checkpoint"

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/Users/sysadmin/workspace/spark-streaming-kinesis-sample/spark-warehouse")
      .getOrCreate()
    val sc = spark.sparkContext
    //val ssc = new StreamingContext(sc, batchInterval)

    val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => {
      val ssc = new StreamingContext(sc, batchInterval)
      ssc.checkpoint(checkpointDirectory)
      // Create the Kinesis DStreams
      val kinesisStreams = (0 until numStreams).map { i =>
        KinesisInputDStream.builder
          .streamingContext(ssc)
          .streamName(streamName)
          .endpointUrl(endpointUrl)
          .regionName(regionName)
          .initialPositionInStream(InitialPositionInStream.LATEST)
          .checkpointAppName(appName)
          .checkpointInterval(kinesisCheckpointInterval)
          .storageLevel(StorageLevel.MEMORY_ONLY) //.storageLevel(StorageLevel.MEMORY_AND_DISK_2)
          .build()
      }

      System.out.println(s3path)

      // Union all the streams
      val unionStreams = ssc.union(kinesisStreams)

      // DStream.foreachRDD gives you an RDD[Array[Byte]], NOT a single ByteArray
      unionStreams.foreachRDD ((rdd: RDD[Array[Byte]], time: Time) => {
        if(rdd.count() > 0){
          System.out.println("converting dstream to DataFrame. count: " + rdd.count().toString())
          val jsonRDD = rdd.map(byteArray => new String(byteArray))
          val df = spark.sqlContext.read.json(jsonRDD)
          df.show()
          df.repartition(1).write.mode("append").csv(s3path)
        }
      })

      ssc
    })

    // Start the streaming context and await termination
    ssc.start()
    ssc.awaitTermination()
  }

  def getRegionNameByEndpoint(endpoint: String): String = {
    val uri = new java.net.URI(endpoint)
    RegionUtils.getRegionsForService(AmazonKinesis.ENDPOINT_PREFIX)
      .asScala
      .find(_.getAvailableEndpoints.asScala.toSeq.contains(uri.getHost))
      .map(_.getName)
      .getOrElse(
        throw new IllegalArgumentException(s"Could not resolve region for endpoint: $endpoint"))
  }
}


/**
  *  Utility functions for Spark Streaming examples.
  *  This has been lifted from the examples/ project to remove the circular dependency.
  */
private object StreamingExamples extends Logging {
  // Set reasonable logging levels for streaming if the user has not configured log4j.
  def setStreamingLogLevels() {
    //Logger.getRootLogger.setLevel(Level.INFO)
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
// scalastyle:on println