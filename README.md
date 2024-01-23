# EPEI

An RDF Retrieval System Based on **E**fficient **P**redicate-**E**ntity **I**ndexing

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

## How to use

```
Usage: epei <command> [<args>]

Description:
  Common commands for various situations using EPEI.

Commands:
  build      Build the data index for the given RDF data file path.
  query      Query the SPARQL statement for the given file path.
  server     Start the EPEI server.

Options:
  -h, --help      Show this help message and exit.

Command-specific options:
  build:
    -n, --name <NAME>    Specify the database name.
    -f, --file <FILE>    Specify the RDF data file path.

Positional Arguments:
  command       The command to run (e.g., build, query, server).
```

Build RDF database:

```shell
epei build -n <rdf_db_name> -f <rdf_file_name>
```

Execute SPARQL query:

sparql:

```shell
Usage: sparql [options]

Description:
  Run a SPARQL query.

Options:
  -h, --help     Show this help message and exit.        
```

file:

```shell
Usage: file [options] [arguments]

Description:
  Run SPARQL queries from a file and output the results to a file.

Options:
  -i, --input <file>    Specify the input file containing SPARQL queries.
  -o, --output [file]   Specify the output file for the query results.   
```

Run http  server:

```shell
Usage: epei server [-p,--port PORT]

Description:
  Start the HTTP server for EPEI.

Options:
  -p, --port <PORT>   Specify the HTTP server port.

Optional Arguments:
  -h, --help          Show this help message and exit.

Examples:
  epei server --port 8080
```

