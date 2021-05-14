# The build-stage image:
FROM whantana/taxi-rides-conda-env AS build
LABEL stage=build

# Install and use conda-pack:
RUN conda install -c conda-forge conda-pack
RUN conda-pack -n taxi_rides -o /tmp/env.tar && \
  mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
  rm /tmp/env.tar
RUN /venv/bin/conda-unpack

# The runtime-stage image;
FROM debian:buster AS runtime

# Copy /venv from the previous stage:
COPY --from=build /venv /venv

# install curl, wget java and ant
RUN apt-get update -y && apt-get install -y curl wget openjdk-11-jdk ant ca-certificates-java && apt-get clean && update-ca-certificates -f

# install az-cli
RUN sh -c 'curl -sL https://aka.ms/InstallAzureCLIDeb | /bin/bash'

# setup JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# install azcopy
RUN wget https://aka.ms/downloadazcopy-v10-linux && tar -xvf downloadazcopy-v10-linux && \
  cp ./azcopy_linux_amd64_*/azcopy /usr/bin/azcopy

# make taxi-rides directory and cert
RUN mkdir /taxi_rides
RUN mkdir /taxi_rides/bash

# copy certificate and python, bash and jars
COPY python /taxi_rides/python
COPY bash/data-ingestion /taxi_rides/bash/data-ingestion
COPY jars /taxi_rides/jars