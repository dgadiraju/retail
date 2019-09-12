package retail

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by itversity on 05/06/17.
 */
object DailyProductRevenue {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))

    val inputBaseDir = envProps.getString("input.base.dir")
    val outputBaseDir = envProps.getString("output.base.dir")

    val conf = new SparkConf().
      setAppName("Daily Product Revenue").
      setMaster(envProps.getString("execution.mode"))
    val sc = new SparkContext(conf)

    val orders = sc.textFile(inputBaseDir + "/orders")
    val orderItems = sc.textFile(inputBaseDir + "/order_items")

    val ordersFiltered = orders.
      filter(o => List("COMPLETE", "CLOSED").contains(o.split(",")(3)))

    val ordersFilteredMap = ordersFiltered.
      map(o => (o.split(",")(0).toInt, o.split(",")(1)))

    val orderItemsMap = orderItems.
      map(oi => {
        val i = oi.split(",")
        (i(1).toInt, (i(2).toInt, i(4).toFloat))
      })

    val ordersJoin = ordersFilteredMap.
      join(orderItemsMap)
    val dailyProductRevenue = ordersJoin.
      map(o => ((o._2._1, o._2._2._1), o._2._2._2)).
      reduceByKey((agg, ele) => agg + ele)

    val dailyProductRevenueSorted = dailyProductRevenue.
      map(o => {
        ((o._1._1, -o._2),
          (o._1._1, o._1._2, o._2))
      }).
      sortByKey().
      map(o => o._2.productIterator.mkString(","))

    dailyProductRevenueSorted.
      saveAsTextFile(outputBaseDir + "/daily_product_revenue")

  }
}