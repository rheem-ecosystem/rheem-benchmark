package org.qcri.rheem.apps.wordcount

import org.qcri.rheem.api._
import org.qcri.rheem.apps.util.Parameters
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval
import org.qcri.rheem.core.platform.Platform

/**
  * This is app counts words in a file.
  *
  * @see [[org.qcri.rheem.apps.wordcount.Main]]
  */
class WordCountScala(platforms: Platform*) {

  /**
    * Run the word count over a given file.
    *
    * @param inputUrl URL to the file
    * @return the counted words
    */
  def apply(inputUrl: String) = {
    val rheemCtx = new RheemContext
    platforms.foreach(rheemCtx.register)

    rheemCtx
      .readTextFile(inputUrl).withName("Load file")
      .flatMap(_.split("\\W+"), selectivity = new ProbabilisticDoubleInterval(100, 10000, .8d)).withName("Split words")
      .filter(_.nonEmpty).withName("Filter empty words")
      .map(word => (word.toLowerCase, 1)).withName("To lower case, add counter")
      .reduceByKey(_._1, (c1, c2) => (c1._1, c1._2 + c2._2)).withName("Add counters")
      .withCardinalityEstimator((in: Long) => math.round(in * 0.01))
      .withUdfJarsOf(classOf[WordCountScala])
      .collect(jobName = s"WordCount ($inputUrl)")
  }

}

/**
  * Companion object for [[WordCountScala]].
  */
object WordCountScala {

  def main(args: Array[String]) {
    if (args.isEmpty) {
      println("Usage: <main class> <platform(,platform)*> <input file>")
      sys.exit(1)
    }

    val platforms = Parameters.loadPlatforms(args(0), () => new Configuration)

    val inputFile = args(1)

    val wordCount = new WordCountScala(platforms: _*)
    val words = wordCount(inputFile).toSeq.sortBy(-_._2)

    println(s"Found ${words.size} words:")
    words.foreach(wc => println(s"${wc._2}x ${wc._1}"))
  }

}
