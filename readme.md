# Flink Processing Checks

## Build
```mvn clean package```

## Compiled job
according to ```<mainClass>``` at project/pom.xml, change as required

## Example
1. Start the job  
    a. set ```<mainClass>``` to ```com.tglanz.stream.UdpEchoJob```  
    b. ```mvn clean package```  
    c. ```FLINK_PATH/bin/start-cluster.sh```  
    d. ```FLINK_PATH/bin/flink run REPOSITORY/project/targetflink_procesing-1.0-SNAPSHOT.jar --port PORT --output OUTPUT_PATH```  
2. Send data over udp  
    a. ```echo -n "hi" >/dev/udp/localhost/PORT```