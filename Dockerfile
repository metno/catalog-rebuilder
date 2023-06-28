# Create the catalog-rebuilder container.
FROM ubuntu
RUN mkdir /app

ARG GUNICORN_VERSION="~=20.1"
ARG MMD_REPO=https://github.com/metno/mmd
ARG MMD_VERSION=v3.5

# Set config file for dmci
ENV DMCI_CONFIG=/config.yaml

RUN apt-get -qqy update && \
    apt-get -qqy install \
      ca-certificates \
      dumb-init \
      git \
      htop \
      libxml2 \
      libxslt1.1 \
      python3-lxml \
      python3-pip \
      python3-wheel \
      wget \
      python3 \
    && rm -rf /var/lib/apt/lists/* && \
    pip install "gunicorn${GUNICORN_VERSION}"


# Download MMD and use local copy of schema (see sed command below)
RUN git config --global advice.detachedHead false
RUN git clone --depth 1 --branch ${MMD_VERSION} ${MMD_REPO} /tmp/mmd && \
    mkdir -p /usr/share/mmd/xslt $DST/usr/share/mmd/xsd && \
    cp -a /tmp/mmd/xslt/* /usr/share/mmd/xslt && \
    cp -a /tmp/mmd/xsd/* /usr/share/mmd/xsd && \
    sed -Ei 's#http\://www.w3.org/2001/(xml.xsd)#\1#g' /usr/share/mmd/xsd/*.xsd && \
    rm -rf /tmp/mmd && \
    rm -fr /dst

ADD . /app

WORKDIR /app

ARG CATALOG_REBUILDER_VERSION=main

# Set to True when run container to start rebuilder job.
ENV CATALOG_REBUILDER_ENABLED=False

RUN pip install -r requirements.txt

# Default port to
EXPOSE 5000

# Override directory, expected to have persistent storage
VOLUME /dmci

# Override directory, expected to have persistent storage
VOLUME /repo
RUN git config --global --add safe.directory /repo

VOLUME /archive

# Catch interrupts and send to all sub-processes
ENTRYPOINT ["dumb-init", "--"]

# Start application
CMD gunicorn --worker-class sync --workers "1" --bind 0.0.0.0:5000 'main:create_app()' --keep-alive "1" --log-level DEBUG
