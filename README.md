# Project Phase 2

## Organisational details

- **Same groups** as defined in phase 1 of the project.
- **Deadline: 23:59:59 2024-11-29 (Sexta-feira)**
- Cluster access is provided for trialling your system.

## Project description

[See the PDF](./docs/Project-Phase2.pdf)

## Commands

### Compile

```bash
cd src/asd-project2-base
mvn compile package
```

### Run replicas

You can use the `start-processes-local.sh` script file to start multiple client processes. For example, the following command starts 3 replicas with p2p ports 34000-34002, and server ports 35000-35002.

```bash
cd src/asd-project2-base
bash start-processes-local.sh 3
```

You can also start a process manually by running the following command, where you must modify `config.properties` or use flags to indicate the ports/network interfaces that you would like to use.

```bash
java -cp target/asdProj2.jar Main
```

### Run YCSB clients

Once you have a system of processes running, you can use the following command to run an instance of YCSB.

```bash
cd src/asd-project2-base/client
bash exec.sh <number_of_processes> <size_of_hash_map_values> <comma_separated_ip_addresses_and_ports> <proportion_of_reads> <proportion_of_writes>
```

For instance, to start YCSB for a system with:

- 3 replicas
- 1024 byte payloads
- 0% read
- 100% write

you can use the following command:

```bash
./exec.sh 3 1024 127.0.0.1:35000,127.0.0.1:35001,127.0.0.1:35002 0 100
```
