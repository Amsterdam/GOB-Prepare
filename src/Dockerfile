FROM amsterdam/python:3.9-buster
MAINTAINER datapunt@amsterdam.nl

# Install gobprepare in /app folder
WORKDIR /app

# Copy the required db drivers
COPY drivers drivers

# Install Oracle driver
# https://oracle.github.io/odpi/doc/installation.html#linux
ENV ORACLE_DIR=/app/opt/oracle
RUN mkdir -p ${ORACLE_DIR}
RUN unzip drivers/instantclient-basic-linux.x64-18.3.0.0.0dbru.zip -d ${ORACLE_DIR}
ENV LD_LIBRARY_PATH=${ORACLE_DIR}/instantclient_18_3:$LD_LIBRARY_PATH
RUN apt-get update && apt-get -y --no-install-recommends install libaio1

# Install SQL Server driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Install GDAL
RUN apt-get install -y libgdal-dev

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install required Python packages
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobprepare module
COPY gobprepare gobprepare

# For the current test period, add the datafiles
COPY data data

CMD ["python", "-m", "gobprepare"]
