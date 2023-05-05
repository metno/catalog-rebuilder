# Create the catalog-rebuilder container.
FROM ubuntu
RUN mkdir /app

ADD . /app

WORKDIR /app
RUN apt-get update
RUN apt-get -y install python3
RUN apt-get -y install python3-setuptools
RUN apt-get -y install python3-pip

ARG CATALOG_REBUILDER_VERSION=main

# Set to True when run container to start rebuilder job.
ENV CATALOG_REBUILDER_ENABLED=False

RUN pip install -r requirements.txt

ENTRYPOINT [ "/bin/bash" ]

# Start application
CMD [ "entrypoint.sh" ]
