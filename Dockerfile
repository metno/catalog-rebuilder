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
ENV CATALOG_REBUILDER_ENABLED=True

RUN pip install -r requirements.txt

ENTRYPOINT [ "python3" ]

# Start application
CMD [ "catalog_rebuilder.py" ]
