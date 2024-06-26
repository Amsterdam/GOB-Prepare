# syntax=docker/dockerfile:1
FROM amsterdam/gob_wheelhouse:3.9-slim-bullseye as wheelhouse
MAINTAINER datapunt@amsterdam.nl


# Application stage.
FROM amsterdam/gob_baseimage:3.9-slim-bullseye as application
MAINTAINER datapunt@amsterdam.nl
# GOB base image: SQL Server driver, Oracle driver.

# Fill the wheelhouse.
COPY --from=wheelhouse /opt/wheelhouse /opt/wheelhouse

# Install gobprepare in /app folder.
WORKDIR /app

# Install required Python packages.
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install --no-cache-dir \
	--find-links /opt/wheelhouse --requirement requirements.txt
RUN rm requirements.txt
# Wheelhouse cleanup.
RUN rm -rf /opt/wheelhouse

# Copy gobprepare module
COPY gobprepare gobprepare

# For the current test period, add the datafiles
COPY data data

USER datapunt
CMD ["python", "-m", "gobprepare"]


# Development.
FROM application as development
# Install GOB-Core and GOB-Config in development mode. Will be overridden with
# mounts from the developer filesystem. Changes made to GOB-Core and GOB-Config
# will be available immediately.
USER root
RUN git clone --depth=1 https://github.com/Amsterdam/GOB-Core.git
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install -e /app/GOB-Core
RUN git clone --depth=1 https://github.com/Amsterdam/GOB-Config.git
RUN pip install -e /app/GOB-Config
USER datapunt


# Test.
FROM application as test
USER root

# Remove gobcore/gobconfig tests.
RUN rm -rf /app/src/gobcore/tests
RUN rm -rf /app/src/gobconfig/tests

# Copy test module and tests.
COPY test.sh pyproject.toml ./
COPY tests tests

# Copy Jenkins files.
COPY .jenkins /.jenkins

RUN mkdir -m 2755 -p /app/shared/message_broker && chown datapunt.datapunt /app/shared /app/shared/message_broker
USER datapunt
