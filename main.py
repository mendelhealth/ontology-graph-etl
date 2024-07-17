import os
import json
from string import Template
import re
from neo4j import GraphDatabase
import time
import openpyxl

# local graph database
uri = "bolt://localhost:7687"
user = "neo4j"
password = "FILL_IN_YOUR_PASSWORD"

# production graph database
# uri = "bolt://104.154.42.184:7687"
# user = "neo4j"
# password = ""


def convert_to_json(read_file_path, write_file_path):
  lines = []
  # Open the file in read mode and read lines
  with open(read_file_path, 'r') as rfile:
    lines = rfile.readlines()

  # Open the file in write mode
  with open(write_file_path, 'w') as wfile:
    for line in lines:
      wfile.write(line.rstrip('\n') + ',\n')

def sanitize_value(input_value):
  input_string = str(input_value)
  if input_string:
    return re.sub(r'[^a-zA-Z0-9\s]', '', input_string)
  else:
    return ""

# "MERGE (TheMatrix:Movie {title:'The Matrix'}) ON CREATE SET TheMatrix.released=1999, TheMatrix.tagline='Welcome to the Real World'"
def create_cypher_concept(read_file_path, write_file_path):
  concepts = []
  with open(read_file_path, 'r') as rfile:
    concepts = rfile.readlines()

  with open(write_file_path, 'w') as wfile:
    for concept in concepts:
      concept_dict = json.loads(concept)
      str_id = str(concept_dict['id'])
      name = sanitize_value(concept_dict['name'])
      cypress_command = Template("MERGE (m$str_id:Concept {name:'$name'}) ON CREATE SET m$str_id.id='m$str_id'").substitute(str_id=str_id, name=name)
      semantic_type = concept_dict['semantic_type']
      if (semantic_type):
        cypress_command += Template(", m$str_id.semantic_type='$semantic_type'").substitute(str_id=str_id, semantic_type=semantic_type)
      cui = sanitize_value(concept_dict['cui'])
      if (cui):
        cypress_command += Template(", m$str_id.cui='$cui'").substitute(str_id=str_id, cui=cui)
      search_type = sanitize_value(concept_dict['search_type'])
      if (search_type):
        cypress_command += Template(", m$str_id.search_type='$search_type'").substitute(str_id=str_id, search_type=search_type)
      # description = sanitize_value(concept_dict['description'])
      # if (description):
      #   cypress_command += Template(", m$str_id.description='$description'").substitute(str_id=str_id, description=description)
      property_concept = sanitize_value(concept_dict['property_concept'])
      if (property_concept):
        cypress_command += Template(", m$str_id.property_concept='$property_concept'").substitute(str_id=str_id, property_concept=property_concept)
      wfile.write(cypress_command + '\n')

# MATCH (m$str_parent_id {id: 'm$str_parent_id'}) MATCH (m$str_child_id {id: 'm$str_child_id'}) CREATE (m$str_parent_id)-[:PARENT_OF]->(m$str_child_id)
def create_cypher_relationships(read_file_path, write_file_path):
  relationships = []
  with open(read_file_path, 'r') as rfile:
    relationships = rfile.readlines()
  
  with open(write_file_path, 'w') as wfile:
    for relationship in relationships:
      relationship_dict = json.loads(relationship)
      str_child_id = str(relationship_dict['child_id'])
      str_parent_id = str(relationship_dict['parent_id'])
      cypress_command = Template("MATCH (m$str_parent_id {id: 'm$str_parent_id'}) MATCH (m$str_child_id {id: 'm$str_child_id'}) CREATE (m$str_parent_id)-[:PARENT_OF]->(m$str_child_id)").substitute(str_parent_id=str_parent_id, str_child_id=str_child_id)
      wfile.write(cypress_command + '\n')
  return

def create_cypher_node_with_relationship(read_file_path, read_sheet_index, write_file_path):
  # read rows in a sheet in a google sheet
  columns_to_read = 4
  worksheet_metadata = [
    {},
    {
      "node1": { "type": "SurgicalExtent", "name": "Surgical Extent", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "SurgicalProcedureType", "name": "Surgical Procedure Type", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "TYPE_OF"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "OutcomeType", "name": "Outcome Type", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "CAUSED_OUTCOME"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 13, "column_node_id": 14 },
      "node2": { "type": "Gene", "name": "Gene", "column_node_value": 1, "column_node_id": 2 },
      "relationship": "HAS_BIOMARKER"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "TREATS"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "Gene", "name": "Gene", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "HAS_PREDICTIVE_BIOMARKER"
    },
    {
      "node1": { "type": "DiseaseType", "name": "Disease Type", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "Technique", "name": "Technique", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "HAS_REASON"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "MorphologyType", "name": "Morphology Type", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "HAS_MORPHOLOGY"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "Stage", "name": "Prognostic Measures Value Type", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "HAS_STAGE"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "Mechanism", "name": "Mechanism of action", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "HAS_MECHANISM"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "BodyPart", "name": "Body Part", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "HAS_LOCATION"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "SurgicalExtent ", "name": "Surgical Extent", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "TREATED_WITH_SURGERY"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "Behavior ", "name": "Behavior", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "HAS_BEHAVIOR"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "Technique ", "name": "Technique", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "TREATS"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 1, "column_node_id": 2 },
      "node2": { "type": "DiseaseType ", "name": "Disease Type", "column_node_value": 3, "column_node_id": 4 },
      "relationship": "CAUSED_SIDE_EFFECT"
    },
  ]
  print("worksheet index " + str(read_sheet_index))
  print(worksheet_metadata[read_sheet_index]["node1"])
  column_node1_type = worksheet_metadata[read_sheet_index]["node1"]["type"]
  column_node1_value = worksheet_metadata[read_sheet_index]["node1"]["column_node_value"]
  column_node1_id = worksheet_metadata[read_sheet_index]["node1"]["column_node_id"]
  column_node2_type = worksheet_metadata[read_sheet_index]["node2"]["type"]
  column_node2_value = worksheet_metadata[read_sheet_index]["node2"]["column_node_value"]
  column_node2_id = worksheet_metadata[read_sheet_index]["node2"]["column_node_id"]
  relationship = worksheet_metadata[read_sheet_index]["relationship"]
  workbook = openpyxl.load_workbook(read_file_path)
  sheet = workbook.worksheets[read_sheet_index]
  row_count = 0
  items = []
  for row in sheet.iter_rows(values_only=True):
    if row_count == 0:
      row_count += 1
      continue;
    # break if the row is empty [assuming the first column is the key]
    if row[0] is None: 
      break
    row_count += 1
    items.append({
      "node1": { "id": row[column_node1_id], "value": row[column_node1_value], "type": column_node1_type },
      "node2": {  "id": row[column_node2_id], "value": row[column_node2_value], "type": column_node2_type },
      "relationship": relationship
    })
    break
  print("Number of rows in sheet: " + str(row_count))

  # for each row
  for item in items:
    print(item)
    break
    


  # create cypher command to create two nodes and a relationship between them
  # write the cypher command to a file
  return;

def run_cypher_file(file_path):
    # Connect to Neo4j
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def execute_queries(tx, queries):
        for query in queries:
            tx.run(query)
    
    # Read Cypher file
    with open(file_path, 'r') as file:
        queries = file.readlines()
    
    with driver.session() as session:
        batches = int(len(queries)/100)
        for i in range(0, batches + 1):
          start = i*100
          end = i*100+99
          start_time = time.time()
          session.execute_write(execute_queries, queries[start : end])
          end_time = time.time()
          execution_time = end_time - start_time
          print(str(i) + ": running queries from " + str(start) + " to " + str(end) + " out of " + str(len(queries)) + f" executed in {execution_time} seconds")
    # Close the driver connection
    driver.close()

# convert_to_json('data/concept.json', 'json-data/concept.json')
# convert_to_json('data/concept_hierarchy.json', 'json-data/concept_hierarchy.json')
create_cypher_concept('data/concept.json','cypher/concept_v4.cypher')
# create_cypher_relationships('data/concept_hierarchy.json','cypher/concept_hierarchy_v3.cypher')
# run_cypher_file('cypher/concept_v3.cypher')
# run_cypher_file('cypher/concept_hierarchy_v3.cypher')
# create_cypher_node_with_relationship('excel/relationships.xlsx', 1,'cypher/relationships.cypher')