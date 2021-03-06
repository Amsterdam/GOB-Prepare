FROM amsterdam/python:3.7.2-stretch
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
RUN apt-get -y --no-install-recommends install libaio1

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobprepare module
COPY gobprepare gobprepare

# For the current test period, add the datafiles
COPY data data

CMD ["python", "-m", "gobprepare"]
