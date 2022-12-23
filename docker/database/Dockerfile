FROM postgres:12-bullseye

RUN apt-get update
RUN apt-get install -y curl
RUN curl https://install.citusdata.com/community/deb.sh > add-citus-repo.sh
RUN bash add-citus-repo.sh
RUN apt-get install -y postgresql-12-citus-10.2

COPY postgresql.conf /etc/postgresql/postgresql.conf
RUN echo "CREATE EXTENSION citus;" > /docker-entrypoint-initdb.d/create-extension-citus.sql
