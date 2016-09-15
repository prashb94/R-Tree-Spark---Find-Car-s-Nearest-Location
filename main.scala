package hw3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import archery._

object Main {

  def pointOnLine(X1:Float,Y1:Float,X2:Float,Y2:Float,X3:Float,Y3:Float) ={
    val XX = X2 - X1
    val YY = Y2 - Y1
    val ShortestLength = ((XX * (X3 - X1)) + (YY * (Y3 - Y1))) / ((XX * XX) + (YY * YY))
    val X4 = X1 + XX * ShortestLength
    val Y4 = Y1 + YY * ShortestLength
    if(X4 < X2 && X4 > X1 && Y4 < Y2 && Y4 > Y1)
      (X4,Y4)
  }
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HW3")
    val sc = new SparkContext(sparkConf)
    val roadNodes = sc.textFile("node.csv")
    val pointsTree = roadNodes.map {
      line =>
      {
        val arr = line.split(",")
        (arr(0).toLong,arr(1).toFloat,arr(2).toFloat)
      }
    }
    val roadEdges = sc.textFile("edge.csv")
    val edgeTree = roadEdges.map {
      line =>
      {
        val arr = line.split(",")
        (arr(0).toLong,arr(1).toLong,arr(2).toLong,arr(3).toDouble)
      }
    }
    val rdd1_pair = pointsTree.map{case(node_id, x, y) => (node_id, (x,y))}
    val rdd2_pair1 = edgeTree.map{case(edge_id, node_id1, node_id2, distance) => (node_id1, (edge_id, distance))}
    val rdd2_pair2 = edgeTree.map{case(edge_id, node_id1, node_id2, distance) => (node_id2, (edge_id, distance))}
    val joined_node1 = rdd2_pair1.join(rdd1_pair)
    val joined_node2 = rdd2_pair2.join(rdd1_pair)
    val edge_joined_node1 = joined_node1.map{case(node_id, ((edge_id, distance), (x, y))) => (edge_id, (node_id, (x, y, distance)))}
    val edge_joined_node2 = joined_node2.map{case(node_id, ((edge_id, distance), (x, y))) => (edge_id, (node_id, (x, y, distance)))}
    val all_data_merged = edge_joined_node1.join(edge_joined_node2).collect
    var i = 0
    var finalTree:RTree[Double] = RTree(Entry(Point(0,0),-1.0))

    for( i <- 0 to all_data_merged.length-1)
    {
    var id = all_data_merged(i)._1
      var x1 = all_data_merged(i)._2._1._2._1
      var y1 =  all_data_merged(i)._2._1._2._2
      var x2 =  all_data_merged(i)._2._2._2._1
      var y2 =  all_data_merged(i)._2._2._2._2
      finalTree = finalTree.insert(Entry(Box(x1,y1,x2,y2),all_data_merged(i)._1))
    }
  //  var temp:RTree[Double] = all_data_merged.foreach{case(edge_id,((node_id1,(x1,y1,l1)),(node_id2,(x2,y2,l2)))) => {var value = Entry(Box(x1,y1,x2,y2),edge_id); finalTree.insert(value);}}

    val carPoints = sc.textFile("car.csv")
    val carRDD = carPoints.map {
      line =>
      {
        val arr = line.split(",")
        (arr(0).toLong,arr(2).toFloat,arr(3).toFloat)
      }
    }

  var minDist = carRDD.map{case(carID,carX,carY) => {
    var nearestEdge = finalTree.nearest(Point(carX,carY)).map{case(Entry(Box(x,y,z,w),l)) => (x,y,z,w)}.toArray
    (carID,(nearestEdge(0)._1+nearestEdge(0)._3)/2,(nearestEdge(0)._2+nearestEdge(0)._4)/2)
  }}
	
	minDist.foreach(println)
  }
}
