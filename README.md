# EPEI

An RDF Retrieval System Based on **E**fficient **P**redicate-**E**ntity **I**ndexing

A Vue front-end is avaiable  [here](https://github.com/LiuYipeng42/RDF_Retrieval_System).

## How to build

1. Clone this project

```shell
git clone https://github.com/MKMaS-GUET/EPEI
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

## RDF data and Queries

Download the RDF data and queries that we want to use:
- 1 [Watdiv100M](https://mega.nz/folder/4r1iRCZZ#JKCi9mCCMKOaXadr73kDdQ)
- 2 [Wikidata](https://mega.nz/folder/5vUBHKTQ#TwpzwSzWhzniK1CeykxUCw)

## How to use

```
Usage: epei <command> <args>

Description:
  Common commands for various situations using EPEI.

Commands:
  build      Build the data index for the given RDF data file path.
  query      Query the SPARQL statement for the given file path.
  server     Start the EPEI server.

Args:
  -h, --help      Show this help message and exit.
  --db, --name <NAME>    Specify the database name.
  -f, --file <FILE>    Specify the RDF data file path.
```

Build RDF database:

```shell
epei build --db <rdf_db_name> -f <rdf_file_name>
```

Execute SPARQL query:

sparql:

```shell
epei query --db <rdf_db_name> -f <sparql_file_name>
```

Run http server:

```shell
epei server --port <server port>
```

