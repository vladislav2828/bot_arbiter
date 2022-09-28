FROM postgres:alpine
ENV POSTGRES_PASSWORD valo
ENV POSTGRES_USER valo
ENV POSTGRES_DB valo
WORKDIR /db/
COPY db.sql /docker-entrypoint-initdb.d
EXPOSE 5432