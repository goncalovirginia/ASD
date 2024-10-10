## Instructions

### Compile

```.../asd2024-proj1-main> mvn clean compile package```

### Run

1. **First process:** `.../asd2024-proj1-main> java -cp target/asdProj.jar Main port=10101 n_peers=10 processSequence=1`  
2. **Second and subsequent processes:** `.../asd2024-proj1-main> java -cp target/asdProj.jar Main port=10102
     contact=127.0.0.1:10101 n_peers=10 processSequence=2`
     - Use a different port and processSequence for each new process
     - Every new process should point to an existing process via the `contact=ip:port` parameter
