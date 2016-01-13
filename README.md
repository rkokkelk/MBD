# Detecting the number of freelancers in the Netherlands using Twitter data
This project analyses Twitter data on a Hadoop cluster (owned by the University of Twente) to attempt to determine the number of freelancers. The analysis is done using Java 1.7 and the MapReduce paradigm. The data set only contains tweets from Dutch users, so that is where the analysis focusses on. The paper can be found [here](paper.pdf).

# Run
To run the analysis code, follow these steps:

1. Compile the source using the following [Maven](https://maven.apache.org/) command:
    ```sh
    mvn clean package assembly:single
    ```
    
1. Make sure your jar file (that includes the dependencies) is on the ctithead1.ewi.utwente.nl cluster.
1. Use the following command on the cluster:
    ```sh
    hadoop jar <jar> /data/twitterNL/ <outputdir>
    ```
    
    `<jar>` is the jar file that includes the dependencies. 
    `<outputdir>` is the directory where the output data will be stored.
    If you want only a specific part of the data set as input, use `/data/twitterNL/YYYYmm/` where `YYYY` equals the year and `mm` equals the month (with leading zero).
1. Wait for the cluster to produce results.
1. Find the results in your chosen output directory.
