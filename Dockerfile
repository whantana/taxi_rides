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

# install az-cli
RUN apt-get update -y && apt-get install -y curl wget
RUN sh -c 'curl -sL https://aka.ms/InstallAzureCLIDeb | /bin/bash'

# install azcopy
RUN wget https://aka.ms/downloadazcopy-v10-linux && tar -xvf downloadazcopy-v10-linux && \
  cp ./azcopy_linux_amd64_*/azcopy /usr/bin/azcopy

# make taxi-rides directory
RUN mkdir /taxi_rides

# copy certificate and python
COPY python /taxi_rides/python
COPY bash/taxi-rides-data-ingestion/containers/taxi-rides-data-ingestion-pandas.sh /taxi_rides/bash/taxi-rides-data-ingestion-pandas.sh