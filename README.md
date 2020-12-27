## Data Modeling for RDBMS and ETL with pandas
A simple project to meet the requirements of project 01 for the Udacity Data Engineering Nano Degree program. 


### Setup Local Development Environment 

* start a posgresql database as a docker container:

```bash
# start a container psql1 with credentials specified
# map port 5432
# add custom data subdirectory pgdata
# mount ${home}/psqldata
$ docker run -d \
    --name psql1 \
    -e POSTGRES_PASSWORD=student \
    -e POSTGRES_USER=student \
    -e POSTGRES_DB=studentdb \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    -v ${home}/psqldata:/var/lib/postgresql/data \
    -p 5432:5432 \
    -d postgres
```
- Source: https://hub.docker.com/_/postgres

* Install psql client on ubuntu:
```bash
sudo apt-get install -y postgresql-client
```

* Test connection to local database 
```bash
psql -h localhost -p 5432 -U student -W student studentdb
```


* Install python3 dependencies for psycopg2 on ubuntu: 
```bash
sudo apt install libpq-dev python3-dev
```

* Install python packages in virtual environment 
```
virtualenv -p /usr/bin/python3 venv
source venv/bin/activate
pip3 install -r requirements.txt
```

