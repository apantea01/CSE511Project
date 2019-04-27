package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  class Point(xInput: String, yInput: String) {        
    def x: Double = xInput.trim.toDouble
    def y: Double = yInput.trim.toDouble

    def isWithinDistanceTo(aPoint: Point, distance: Double): Boolean = {
      // Calculate Euclidean distance
      // https://en.wikipedia.org/wiki/Euclidean_distance
      var euclideanDistance = Math.sqrt(Math.pow((this.x - aPoint.x), 2) + Math.pow((this.y - aPoint.y), 2))
      if (euclideanDistance <= distance)
        return true 
      
      return false
    }   
  }

  class Rectangle(rectangleData: String) {
    var dataStringArray: Array[String] = rectangleData.split(",")
    var point1: Point = new Point(dataStringArray(0), dataStringArray(1))
    var point2: Point = new Point(dataStringArray(2), dataStringArray(3))

    def lowerBound_x: Double = math.min(point1.x, point2.x)
    def higherBound_x: Double = math.max(point1.x, point2.x)

    def lowerBound_y: Double = math.min(point1.y, point2.y)
    def higherBound_y: Double = math.max(point1.y, point2.y)

    def containsPoint(aPoint: Point): Boolean = {
      // Check if point.x value is outside rectangle x bounds
      if (aPoint.x < this.lowerBound_x || aPoint.x > this.higherBound_x)
        return false
      
      // Check if point.y value is outside rectangle y bounds
      if (aPoint.y > this.higherBound_y || aPoint.y < this.lowerBound_y)
        return false

      return true
    }   
  }

  def ST_Contains(rectangleData: String, pointData: String): Boolean = {
    var rectangle = new Rectangle(rectangleData)
    var pointStringArray: Array[String] = pointData.split(",")  
    var point = new Point(pointStringArray(0), pointStringArray(1))

    return rectangle.containsPoint(point)
  }

  def ST_Within(point1String: String, point2String: String, distance: Double):Boolean={
    var pointStringArray: Array[String] = point1String.split(",")  
    var point1 = new Point(pointStringArray(0), pointStringArray(1))

    pointStringArray = point2String.split(",")  
    var point2 = new Point(pointStringArray(0), pointStringArray(1))

    return point1.isWithinDistanceTo(point2, distance)
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1,pointString2,distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1,pointString2,distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

}
