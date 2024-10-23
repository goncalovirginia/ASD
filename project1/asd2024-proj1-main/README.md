## Instructions

### Compile

```.../asd2024-proj1-main> mvn clean compile package```

### Run

- `babel_config.properties`:
  - `n_peers`: maximum amount of peers the DHT will operate with (Default value is 100)
  - `id_bits`: number of bits used for peer ID's, either 256 or 512 (Default value is 256)

1. **First process:** `.../asd2024-proj1-main> java -cp target/asdProj.jar Main port=10101 processSequence=1`  
2. **Second and subsequent processes:** `.../asd2024-proj1-main> java -cp target/asdProj.jar Main port=10102
     contact=127.0.0.1:10101 processSequence=2`
     - Use a different port and processSequence for each new process
     - Every new process should point to an existing process via the `contact=ip:port` parameter
