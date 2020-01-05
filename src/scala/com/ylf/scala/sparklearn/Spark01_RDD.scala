package com.ylf.scala.sparklearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object Spark01_RDD {
  def main(args: Array[String]): Unit = {
       val config: SparkConf= new SparkConf().setMaster("local[*]").setAppName("WordCount")
       //创建Sparks上下文对象
       val sc = new SparkContext(config)
       //val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
       //listRDD.collect().foreach(println)
       //val arrayRDD: RDD[Int] = sc.parallelize(Array(1,2,3,4))
       //arrayRDD.collect().foreach(println)
       //val fileRDD: RDD[String] = sc.textFile("in",3)
       //fileRDD.saveAsTextFile("ouput")

       //val source = sc.parallelize(1 to 10)
       //source.collect().foreach(println)

       //source.map(_*2).collect().foreach(println)
       //source.mapPartitions(x => x.map(_*2) ).collect().foreach(println)

       // 有问题
       // mapPartitionsWithIndex(func)
       // val rdd = sc.parallelize(Array(1,2,3,4))
       // rdd.mapPartitionsWithIndex((index,items)=>(items.map((index,_)))).collect().foreach(println)

       // val sourceFlat = sc.parallelize(1 to 5)
       // sourceFlat.flatMap(1 to _).collect().foreach(println)

       // val rdd = sc.parallelize(1 to 16,4)
       // val arrayLists= rdd.glom().collect()
       // for (arrayList <- arrayLists) {
       //  for (name <- arrayList){
       //     print(name)
       //  }
       //  println(',')
       //}

       // val rdd = sc.parallelize(1 to 4)
       // rdd.groupBy(_%2).collect().foreach(println)


       // var sourceFilter = sc.parallelize(Array("xiaoming","xiaojiang","xiaohe","dazhi"))
       // sourceFilter.filter(_.contains("xiao")).collect().foreach(println)

       // val rDD = sc.parallelize(1 to 10)
       // rDD.sample(true,0.4,1).collect().foreach(println)

       // val rDD  = sc.parallelize(List(1,2,1,5,2,9,6,1))
       // rDD.distinct(2).collect().foreach(println)

       // val rdd = sc.parallelize(1 to 16,4)
       // println(rdd.partitions.size)
       // val coalesceRDD = rdd.coalesce(3)
       // println(coalesceRDD.partitions.size)

       // val rdd = sc.parallelize(List(2,1,3,4))
       // rdd.sortBy(x=>x).collect().foreach(println)
       // rdd.sortBy(x=>x%3).collect().foreach(println)

       // val rdd1= sc.parallelize(1 to 5)
       // val rdd2 = sc.parallelize(5 to 10)
       // val rdd3 = rdd1.union(rdd2)
       // rdd3.collect().foreach(println)

       // val rdd1= sc.parallelize(3 to 8)
       // val rdd2 = sc.parallelize(1 to 5)
       // rdd1.subtract(rdd2).collect().foreach(println)

       // val rdd1= sc.parallelize(1 to 7)
       // val rdd2 = sc.parallelize(5 to 10)
       // rdd1.intersection(rdd2).collect().foreach(println)

       // val rdd1= sc.parallelize(1 to 3)
       // val rdd2 = sc.parallelize(2 to 5)
       // rdd1.cartesian(rdd2).collect().foreach(println)

       // val rdd1 = sc.parallelize(Array(1,2,3),3)
       // val rdd2 = sc.parallelize(Array("a","b","c"),3)
       // rdd1.zip(rdd2).collect().foreach(println)
       // rdd2.zip(rdd1).collect().foreach(println)

       // val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
       // print(rdd.partitions.size)
       // val rdd2 = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))
       // println(rdd2.partitions.size)

       // val word = Array("one","two","two","three","three","three")
       // val wordPairsRDD = sc.parallelize(word).map(word => (word,1))
       // val group = wordPairsRDD.groupByKey().collect()

       // val rdd = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)))
       // val reduce = rdd.reduceByKey((x,y) => x+y)
       // reduce.collect().foreach(println)

       // val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
       // val agg = rdd.aggregateByKey(0)(math.max(_,_), _+_)
       // agg.collect().foreach(println)

       // val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),2)
       // val agg = rdd.foldByKey(0)(_+_)
       // agg.collect().foreach(println)

       // val rdd3 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
       // rdd3.mapValues(_+"|||").collect().foreach(println)

       // val rdd1 = sc.makeRDD(1 to 10,2)
       // print(rdd1.reduce(_+_))

       // val rdd2 = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))
       // print(rdd2.reduce((x,y)=>(x._1 + y._1,x._2 + y._2)))

       // val rdd = sc.parallelize(1 to 10)
       // rdd.collect().foreach(println)

       // val rdd = sc.parallelize(1 to 10)
       // println(rdd.count())

       // var rdd1 = sc.makeRDD(1 to 10,2)
       // println(rdd1.aggregate(0)(_+_,_+_))

       val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
       println(rdd.countByKey)




  }
}
