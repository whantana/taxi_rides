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

# install az-cli
RUN apt-get -y update && apt-get -y install curl && curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Copy /venv from the previous stage:
COPY --from=build /venv /venv