# CEIRS

**C**ombined-**E**ncoded-**I**ndex-based **R**etrieval **S**ystem

## How to build

1. Clone this project

```shell
git clone https://github.com/MKMaS-GUET/CEIRS
git submodule update --init --recursive
```

Maybe it's need to update the submodule for this project

```shell
git submodule update --remote
```

2. Build this project 
 
```shell
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

Or use the `build.sh` script to build this project directly

```shell
./scripts/build.sh
```

## How to use

```
usage: hsindb <command> [<args>]
These are common commands used in various situations:
  build                build the data index for the given RDF data file path:
    -n,--name NAME       specify the database name
    -f,--file FILE       specify the RDF data file path

  query                query the SPARQL statement for the given file path:
    -n,--name NAME       specify the database name
    -f,--file FILE       specify the SPARQL statement file path

positional arguments:
  command              the command to run, e.g. build, execute, serve

optional arguments
  -h,--help            show this help message and exit

```

Build RDF database:

```shell
hsindb build -n <rdf_db_name> -f <rdf_file_name>
```

Execute SPARQL query:

```shell
hsindb query -n <rdf_db_name> -f <sparql_query_file_name>
```

## Deploy via Docker

```shell
docker build --tag hsindb:latest .
docker run -it --name hsindb hsindb:latest
```
