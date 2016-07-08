package org.qcri.rheem.apps.kmeans

import java.util

import org.qcri.rheem.api._
import org.qcri.rheem.apps.util.Parameters
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.function.ExecutionContext
import org.qcri.rheem.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.qcri.rheem.core.optimizer.cardinality.FixedSizeCardinalityEstimator
import org.qcri.rheem.core.platform.Platform

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * K-Means app for Rheem.
  */
class Kmeans(platforms: Platform*) {

  def apply(k: Int, inputFile: String, iterations: Int = 20, isResurrect: Boolean = true): Iterable[Point] = {
    // Set up the RheemContext.
    implicit val rheemCtx = new RheemContext
    platforms.foreach(rheemCtx.register)

    // Read and parse the input file(s).
    val points = rheemCtx
      .readTextFile(inputFile).withName("Read file")
      .map { line =>
        val fields = line.split(",")
        Point(fields(0).toDouble, fields(1).toDouble)
      }.withName("Create points")

    // Create initial centroids.
    val initialCentroids = rheemCtx
      .readCollection(Kmeans.createRandomCentroids(k)).withName("Load random centroids")

    // Do the k-means loop.
    val finalCentroids = initialCentroids.repeat(iterations, { currentCentroids =>
      val newCentroids = points
        .mapJava(new SelectNearestCentroid,
          udfCpuLoad = (in1: Long, in2: Long) => in1 * in2 * 1000L
        )
        .withBroadcast(currentCentroids, "centroids").withName("Find nearest centroid")
        .reduceByKey(_.centroidId, _ + _).withName("Add up points")
        .withCardinalityEstimator(k)
        .map(_.average).withName("Average points")


      if (isResurrect) {
        // Resurrect "lost" centroids (that have not been nearest to ANY point).
        val _k = k
        val resurrectedCentroids = newCentroids
          .map(centroid => 1).withName("Count centroids (a)")
          .reduce(_ + _).withName("Count centroids (b)")
          .flatMap(num => {
            if (num < _k) println(s"Resurrecting ${_k - num} point(s).")
            Kmeans.createRandomCentroids(_k - num)
          }).withName("Resurrect centroids")
        newCentroids.union(resurrectedCentroids).withName("New+resurrected centroids")
      } else newCentroids
    }).withName("Loop")

    // Collect the result.
    finalCentroids
      .map(_.toPoint).withName("Strip centroid names")
      .withUdfJarsOf(classOf[Kmeans])
      .collect(jobName = s"k-means ($inputFile, k=$k, $iterations iterations)")
  }


}

/**
  * Companion object of [[Kmeans]].
  */
object Kmeans {

  def main(args: Array[String]): Unit = {
    // Parse args.
    if (args.length == 0) {
      println("Usage: scala <main class> <platform(,platform)*> <point file> <k> <#iterations>")
      sys.exit(1)
    }

    val platforms = Parameters.loadPlatforms(args(0), () => new Configuration)
    val file = args(1)
    val k = args(2).toInt
    val numIterations = args(3).toInt

    // Initialize k-means.
    val kmeans = new Kmeans(platforms: _*)

    // Run k-means.
    val centroids = kmeans(k, file, numIterations)

    // Print the result.
    println(s"Found ${centroids.size} centroids:")

  }

  /**
    * Creates random centroids.
    *
    * @param n      the number of centroids to create
    * @param random used to draw random coordinates
    * @return the centroids
    */
  def createRandomCentroids(n: Int, random: Random = new Random()) =
  // TODO: The random cluster ID makes collisions during resurrection less likely but in general permits ID collisions.
    for (i <- 1 to n) yield TaggedPoint(random.nextGaussian(), random.nextGaussian(), random.nextInt())

}

/**
  * UDF to select the closest centroid for a given [[Point]].
  */
class SelectNearestCentroid extends ExtendedSerializableFunction[Point, TaggedPointCounter] {

  /** Keeps the broadcasted centroids. */
  var centroids: util.Collection[TaggedPoint] = _

  override def open(executionCtx: ExecutionContext) = {
    centroids = executionCtx.getBroadcast[TaggedPoint]("centroids")
  }

  override def apply(point: Point): TaggedPointCounter = {
    var minDistance = Double.PositiveInfinity
    var nearestCentroidId = -1
    for (centroid <- centroids) {
      val distance = point.distanceTo(centroid)
      if (distance < minDistance) {
        minDistance = distance
        nearestCentroidId = centroid.centroidId
      }
    }
    new TaggedPointCounter(point, nearestCentroidId, 1)
  }
}


/**
  * Represents objects with an x and a y coordinate.
  */
sealed trait PointLike {

  /**
    * @return the x coordinate
    */
  def x: Double

  /**
    * @return the y coordinate
    */
  def y: Double

}

/**
  * Represents a two-dimensional point.
  *
  * @param x the x coordinate
  * @param y the y coordinate
  */
case class Point(x: Double, y: Double) extends PointLike {

  /**
    * Calculates the Euclidean distance to another [[Point]].
    *
    * @param that the other [[PointLike]]
    * @return the Euclidean distance
    */
  def distanceTo(that: PointLike) = {
    val dx = this.x - that.x
    val dy = this.y - that.y
    math.sqrt(dx * dx + dy * dy)
  }

  override def toString: String = f"($x%.2f, $y%.2f)"
}

/**
  * Represents a two-dimensional point with a centroid ID attached.
  */
case class TaggedPoint(x: Double, y: Double, centroidId: Int) extends PointLike {

  /**
    * Creates a [[Point]] from this instance.
    *
    * @return the [[Point]]
    */
  def toPoint = Point(x, y)

}

/**
  * Represents a two-dimensional point with a centroid ID and a counter attached.
  */
case class TaggedPointCounter(x: Double, y: Double, centroidId: Int, count: Int = 1) extends PointLike {

  def this(point: PointLike, centroidId: Int, count: Int = 1) = this(point.x, point.y, centroidId, count)

  /**
    * Adds coordinates and counts of two instances.
    *
    * @param that the other instance
    * @return the sum
    */
  def +(that: TaggedPointCounter) = TaggedPointCounter(this.x + that.x, this.y + that.y, this.centroidId, this.count + that.count)

  /**
    * Calculates the average of all added instances.
    *
    * @return a [[TaggedPoint]] reflecting the average
    */
  def average = TaggedPoint(x / count, y / count, centroidId)

}
