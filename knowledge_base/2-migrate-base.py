#encoding: utf-8
from grakn.client import GraknClient
import csv


def build_base_graph(inputs):
        with GraknClient(uri="localhost:48555") as client:
            with client.session(keyspace="baseconhecimento") as session:
                for input in inputs:
                    print("Loading from [" + input["data_path"] + "] into Grakn ...")
                    load_data_into_grakn(input, session)


def load_data_into_grakn(input, session):
    items = parse_data_to_dictionaries(input)

    for item in items:
        with session.transaction().write() as transaction:
            graql_insert_query = input["template"](item)
            print("Executing Graql Query: " + graql_insert_query)
            transaction.query(graql_insert_query)
            transaction.commit()
            print("\nInserted {str(len(items))} items from [{input['data_path']}] into Grakn.\n")


def base_template(base):
    graql_insert_query = "insert $base isa base"
    graql_insert_query += ', has servico "' + base["servico"] + '"'
    graql_insert_query += ', has pergunta "' + base["pergunta"] + '"'
    graql_insert_query += ', has resposta "' + base["resposta"] + '"'
    graql_insert_query += ";"
    return graql_insert_query


def parse_data_to_dictionaries(input):
    items = []
    with open(input["data_path"] + ".csv") as data:  # 1
        for row in csv.DictReader(data, skipinitialspace=True):
            item = {key: value for key, value in row.items()}
            items.append(item)  # 2
    return items


if __name__ == "__main__":
    inputs = [
        {"data_path": "./data/base", "template": base_template},
            ]

    build_base_graph(inputs)
