FROM amazoncorretto:21-alpine

# update and install iproute2
RUN apk add --no-cache bind-tools bash iproute2

# working directory inside docker image
WORKDIR /home/asd

# copy the jar created by assembly to the docker image
# COPY target/tardis-simple-usecase-*.jar babel.jar
# Disabled this because we will set an external mount point instead
RUN mkdir jars

# create the config directory for files used in emulations
RUN mkdir config

# create the results folder
RUN mkdir logs

# copy files to support latency creation
COPY ./inet200Latencies_x0.01.txt config/latencyMap.txt
COPY ./ips200.txt config/ips.txt
COPY ./* .

COPY ./babel_config.properties .
COPY ./log4j2.xml .

# make script executable
RUN chmod 755 ./setupTc.sh

# define entrypoint to be the script that executes tc
ENTRYPOINT ["./setupTc.sh"]
