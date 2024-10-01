## Instructions

### Compile

```.../babel-example> mvn compile package -U```

### Run

1. **First process:** `.../babel-example> java -cp target/asdProj.jar Main port=10101`  
2. **Second and subsequent processes:** `java -cp target/asdProj.jar Main port=10102
     contact=127.0.0.1:10101`
     - Use a different port for each new process
     - Every new process should point to the first process via the `contact=ip:port` parameter
