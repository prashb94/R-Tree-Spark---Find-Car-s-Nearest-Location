# R-Tree-Spark---Find-Car-s-Nearest-Location

Given a road network represented as a graph and some cars’ locations from GPS, the car may exactly be on the road because of GPS deviation. In this solution, an R-Tree is used to figure out the road nearest to a car and then the perpendicular distance to that road is calculated (approximated here), for a dataset provided by Uber in Beijing.
The archery library for Scala (https://github.com/meetup/archery) is used to implement the R-Tree. Please compile the jar file from the above link directly. Sample data is also provided in this repository.
The exact location of a car is the perpendicular distance between the car and the nearest road. For example, 

<img height=250 src=https://github.com/prashb94/R-Tree-Spark---Find-Car-s-Nearest-Location/blob/master/pic.png />

Tested on the spark-shell. To run
1.	spark-shell -classpath archery.jar
2.	Paste the code in the main method in main.scala
