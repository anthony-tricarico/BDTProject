FROM postgres:latest

# copies the .sql file to the entrypoint of the db and the operations in the .sql files will be executed
COPY trentino_trasporti_cleaned /docker-entrypoint-initdb.d/trentino_trasporti
COPY make_db.sql /docker-entrypoint-initdb.d/