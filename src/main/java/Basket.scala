import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Basket {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("spark-dataset-basic")
      .getOrCreate()

    val inputFile = "data/retail.csv"

    val dataFrame = sparkSession.read
      .option("delimiter", ";")
      .option("header", "true")
      .csv(inputFile)
    val myData = dataFrame.createOrReplaceTempView("transactTable")
    val tra = sparkSession.sql("select InvoiceNo,StockCode from transactTable " +
      "where StockCode is not null and InvoiceNo is not null")
    val dic = sparkSession.sql("select StockCode, Description from transactTable " +
      "where StockCode is not null and Description is not null").distinct()
      .rdd.map(x => (x(0).toString, x(1).toString)).collect().toMap

    val rdd2 = tra.rdd.map(r => (r(0), r(1)))
    val transactions = rdd2.groupByKey()
    val tran = transactions.map(t => t._2.toArray.distinct)
    val fpg = new FPGrowth()
      .setMinSupport(0.02)

    val model = fpg.run(tran)
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    //get association rules
    val minConfidence = 0.03
    val rules = model.generateAssociationRules(minConfidence)

    rules.collect().foreach { rule =>
      println(
        rule.antecedent.map(x => dic(x.toString)).mkString("[", ",", "]")
          + " => " + rule.consequent.map(x => dic(x.toString)).mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }

}