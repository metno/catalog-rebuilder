# Create the catalog-rebuilder container.
FROM ubuntu
RUN mkdir /app

ARG GUNICORN_VERSION="~=20.1"
ARG MMD_REPO=https://github.com/metno/mmd
ARG MMD_VERSION=v3.5.2

WORKDIR /tmp

# Set config file for dmci
ENV DMCI_CONFIG=/config.yaml

# Set prefix for git config file
ENV PREFIX=/tmp
RUN mkdir -p /tmp/etc && \
    touch /tmp/etc/gitconfig

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
  #python3-full \
  #python3-venv \
  #python3-gunicorn \
  wget \
  python3 \
  && rm -rf /var/lib/apt/lists/* && \
  pip install "gunicorn${GUNICORN_VERSION}" --break-system-packages \
  pip install backports.shutil_get_terminal_size --break-system-packages \
  pip install scandir --break-system-packages

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

#ENV VIRTUAL_ENV=/opt/venv
#RUN python3 -m venv $VIRTUAL_ENV
#ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip install -r requirements.txt --break-system-packages

# Default port to
EXPOSE 5000

# Override directory, expected to have persistent storage
VOLUME /dmci

# Override directory, expected to have persistent storage
VOLUME /repo
RUN git config --global --add safe.directory /repo
RUN git config --global  user.name "catalog-rebuilder"

VOLUME /archive

# Catch interrupts and send to all sub-processes
ENTRYPOINT ["dumb-init", "--"]

# Start application
CMD gunicorn --worker-class sync --workers "1" --bind 0.0.0.0:5000 'main:create_app()' --timeout 600 --keep-alive "5" --log-level DEBUG

# For Celery App override with:
# celery --app catalog_rebuilder.app worker --loglevel=DEBUG
