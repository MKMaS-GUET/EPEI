from SPARQLWrapper import SPARQLWrapper, JSON
import time
import traceback
import sys


def query(file_path, output_path):

    queries_file = open(file_path, "r")
    output_file = open(output_path, "w")

    queries = []
    for line in queries_file:
        # print(line.strip())
        queries.append(line.strip())
    queries_file.close()

    query_number = 0
    for query in queries:
        count = 0
        sparql.setQuery(query)
        results = sparql.query()

        json_results = results.convert()
        for result in json_results["results"]["bindings"]:
            count += 1

        output_file.write(
            "{0};{1};{2}\n".format(query_number, count, json_results["time"])
        )
        query_number += 1

    output_file.close()


sparql = SPARQLWrapper("http://127.0.0.1:8080/epei/sparql")
sparql.setTimeout(300)
sparql.setReturnFormat(JSON)

# q = "SELECT ?v0 ?v1 ?v3 WHERE { ?v0 <http://schema.org/jobTitle> ?v1 . <http://db.uwaterloo.ca/~galuc/wsdbm/City134> <http://www.geonames.org/ontology#parentCountry> ?v3 . ?v0 <http://schema.org/nationality> ?v3 . } "
# start_time = time.time()
# sparql.setQuery(q)
# results = sparql.query()
# end_time = time.time()
# json_results = results.convert()
# # for result in json_results["results"]["bindings"]:
# #     print(result)
# print(json_results["time"])
# print(int((end_time - start_time) * 1000))

output_dir = sys.argv[1]

# query(
#     "/home/lyp/DATA/rdf_systems/EPEI/bin/queries/watdiv_sparqls",
#     output_dir + "/watdiv_result",
# )

types = [
    "J3.txt",
    "J4.txt",
    "P2.txt",
    "P3.txt",
    "P4.txt",
    "S1.txt",
    "S2.txt",
    "S3.txt",
    "S4.txt",
    "T2.txt",
    "T3.txt",
    "T4.txt",
    "TI2.txt",
    "TI3.txt",
    "TI4.txt",
    "Tr1.txt",
    "Tr2.txt",
]

for t in types:
    print(t)
    query("/home/lyp/DATA/datasets/wiki/WGPB/queries/" + t, output_dir + "/" + t)
