package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // At this point pickupInfo variable contains all the x, y, z data we need to create our space 
    // time cube as described in the project assignment "Problem Definition"
    // 
    // X and Y represent geo spatial data and Z represents time data.
    // This is why we call the cube a "Space Time Cude"
    // Note that we dont need to process Z in the geo spatial sense (elevation) 
    // because it would not help to look at data about elevation whan analyzing popular 
    // taxi cab pickup points. :)
    //
    // Define the min and max of x, y, z
    //      Setting minX, maxX, minY, and maxY lets us define the longitude and longitude boundaries of 
    //      where what we are processing taxi cab service data for 
    //      We dont want taxi cab data that is outside these geo spatial boundaries
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    
    //      Setting minZ and maxZ lets us define the number of days in a month
    //      That is, each value of Z will be for a seperate day of taxi cab service data
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    // All Code Changed by Team 1 below this line

    pickupInfo.createOrReplaceTempView("pickupInfo")

    // Create and populate SpaceTimeCube
    SpaceTimeCube.create(minX, maxX, minY, maxY, minZ, maxZ, numCells)
    var filteredDataForCube = spark.sql("SELECT * FROM pickupInfo WHERE " +
      "x >= '" + minX + "' and x <= '" + maxX +
      "' and y >= '" + minY + "' and y <= '" + maxY +
      "' and z >= '" + minZ + "' and z <= '" + maxZ + "'")
    filteredDataForCube.show()
    filteredDataForCube.createOrReplaceTempView("pickupInfo")
    filteredDataForCube.foreach(row =>
      SpaceTimeCube.incrementCubeEntry(row.getInt(0), row.getInt(1), row.getInt(2))
    )

    // Compute Cube stats
    SpaceTimeCube.computeStatistics()
    spark.udf.register("computeGetisOrdStat", (x: Int, y: Int, z: Int) => SpaceTimeCube.computeGetisOrdStat(x,y,z))
    val getisOrdStats = spark.sql(
      "SELECT x, y, z, computeGetisOrdStat(x, y, z) AS getisOrdStat FROM (SELECT distinct * from pickupInfo)")
    getisOrdStats.show()
    getisOrdStats.createOrReplaceTempView("getisOrdStats")

    // Sort by getisOrdStat and return first 50
    val result = spark.sql("SELECT x, y, z FROM getisOrdStats ORDER BY getisOrdStat desc LIMIT 50")
    return result
  }


  object SpaceTimeCube {
    var cube: Array[Array[Array[Int]]] = Array.ofDim[Int](0, 0, 0)
    var minXInt: Int = 0
    var minYInt: Int = 0
    var minZInt: Int = 0
    var sizeX: Int = 0
    var sizeY: Int = 0
    var sizeZ: Int = 0
    var numCells: Double = 0
    var mean: Double = 0.0
    var variance: Double = 0.0

    def create (minX: Double, maxX: Double, minY: Double, maxY: Double, minZ: Int, maxZ: Int, size: Double) = {
        minXInt = Math.floor(minX).toInt
        minYInt = Math.floor(minY).toInt
        minZInt = minZ
        sizeX = Math.floor(maxX - minX + 1).toInt
        sizeY = Math.floor(maxY - minY + 1).toInt
        sizeZ = maxZ - minZ + 1
        numCells = size
        // Initialize the cube and set every cell value to 0 (zero)
        cube = Array.fill[Int](sizeX, sizeY, sizeZ)(0)    
        //Console.println("Cube X size is " + sizeX)
        //Console.println("Cube Y size is " + sizeY)
        //Console.println("Cube Z size is " + sizeZ)
    }
    def incrementCubeEntry (x: Int, y: Int, z: Int) = {
        // We add one to existing value at x, y, z in cube
        // This is saying another taxi ride took place on the same day at the
        // same location
        var xLocation: Int = x - minXInt
        var yLocation: Int = y - minYInt
        var zLocation: Int = z - minZInt
        cube(xLocation)(yLocation)(zLocation) = cube(xLocation)(yLocation)(zLocation) + 1
    }
    def computeStatistics () = {
        // Compute Mean Value
        var meanSum = 0.0
        var varianceSum = 0.0
        var cubeVal = 0
        for ( x <- 0 until sizeX ) {
          for ( y <- 0 until sizeY ) {
            for ( z <- 0 until sizeZ ) {
              cubeVal = cube(x)(y)(z)
              meanSum = meanSum + cubeVal
              varianceSum = varianceSum + Math.pow(cubeVal, 2)
            }
          }
        }
        mean = meanSum / numCells
        variance = Math.sqrt((varianceSum / numCells) - Math.pow(mean, 2))
    }
    def computeGetisOrdStat (x: Int, y: Int, z: Int): Double = {
        var inputX: Int = x - minXInt
        var inputY: Int = y - minYInt
        var inputZ: Int = z - minZInt
        var locX: Int = 0
        var locY: Int = 0
        var locZ: Int = 0
        var sum: Double = 0.0
        var count: Int = 0
        for (x <- -1 until 2) {
          for (y <- -1 until 2) {
            for (z <- -1 until 2) {
              locX = inputX + x
              locY = inputY + y
              locZ = inputZ + z
              if (locX >= 0 && locX < sizeX && locY >= 0 && locY < sizeY && locZ >= 0 && locZ < sizeZ) {
                sum = sum + cube(locX)(locY)(locZ)
                count = count + 1
              }
            }
          }
        }
        val numerator: Double = sum - (mean * count)
        val denominator: Double = variance * Math.sqrt((numCells * count - Math.pow(count, 2)) / (numCells - 1))
        var result = numerator / denominator
        return result
    }
  }
}
