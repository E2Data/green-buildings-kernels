# Spark Works E2Data Analytics Kernels

##### This is a sample for the E2Data Spark Works kernels with plain java
##### It computes min, max, sum and average values of a primitive double array
##### The main class is the AnalyticsSampleEngine and accepts as arg either a number indicating the number of double values randomly generated or a text holding the path of a csv file that contains comma separated measurements in the following notation: urn,timestamp.value    

#### Running AnalyticsSampleEngine

1. In project's root dir, compile the classes with
`javac -d ./ src/net/sparkworks/e2data/*.java`

2. Make an executable jar
`jar cvfm analytics.jar META-INF/MANIFEST.MF /net/sparkworks/e2data/*.class`

3. Execute it with 
`java -jar analytics.jar 100` for 100 random values or 
`java -jar analytics.jar /path/to/file.csv` to load values from a csv file. A sample csv file is available in dataset.csv

