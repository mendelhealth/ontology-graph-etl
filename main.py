import os
import json
from string import Template
import re
from neo4j import GraphDatabase
import time
import openpyxl
import logging
import random
import requests

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler("ontology-etl.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# local graph database
# uri = "bolt://localhost:7689"
# user = "neo4j"
# password = "mendel123"

# production graph database
uri = "bolt://104.154.42.184:7687"
user = "neo4j"
password = "DydeKc.2Y.dmdWMc7Hwj"

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
      cypress_command = Template("MERGE (m$str_id:Concept {name:'$name'}) ON CREATE SET m$str_id.id=$str_id").substitute(str_id=str_id, name=name)
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
      cypress_command = Template("MATCH (m$str_parent_id {id: $str_parent_id}) MATCH (m$str_child_id {id: $str_child_id}) CREATE (m$str_parent_id)-[:PARENT_OF]->(m$str_child_id)").substitute(str_parent_id=str_parent_id, str_child_id=str_child_id)
      wfile.write(cypress_command + '\n')
  return

def create_cypher_node_with_relationship(read_file_path, read_sheet_index, write_file_path):
  # read rows in a sheet in a google sheet
  columns_to_read = 4
  # worksheet_metadata = [
  #   {},
  #   {
  #     "node1": { "type": "SurgicalExtent", "name": "Surgical Extent", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "SurgicalProcedureType", "name": "Surgical Procedure Type", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "TYPE_OF"
  #   },
  #   {
  #     "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "OutcomeType", "name": "Outcome Type", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "CAUSED_OUTCOME"
  #   },
  #   {
  #     "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 12, "column_node_id": 13 },
  #     "node2": { "type": "Gene", "name": "Gene", "column_node_value": 0, "column_node_id": 1 },
  #     "relationship": "HAS_BIOMARKER"
  #   },
  #   {
  #     "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "TREATS"
  #   },
  #   {
  #     "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "Gene", "name": "Gene", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "HAS_TARGET"
  #   },
  #   {},
  #   {
  #     "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "Gene", "name": "Gene", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "HAS_PREDICTIVE_BIOMARKER"
  #   },
  #   {
  #     "node1": { "type": "DiseaseType", "name": "Disease Type", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "Technique", "name": "Technique", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "HAS_REASON"
  #   },
  #   {
  #     "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "MorphologyType", "name": "Morphology Type", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "HAS_MORPHOLOGY"
  #   },
  #   {
  #     "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "Stage", "name": "Prognostic Measures Value Type", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "HAS_STAGE"
  #   },
  #   {
  #     "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "Mechanism", "name": "Mechanism of action", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "HAS_MECHANISM"
  #   },
  #   {
  #     "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "BodyPart", "name": "Body Part", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "HAS_LOCATION"
  #   },
  #   {
  #     "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "SurgicalExtent ", "name": "Surgical Extent", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "TREATS"
  #   },
  #   {
  #     "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "Behavior ", "name": "Behavior", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "HAS_BEHAVIOR"
  #   },
  #   {
  #     "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "Technique ", "name": "Technique", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "TREATS"
  #   },
  #   {
  #     "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "DiseaseType ", "name": "Disease Type", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "CAUSED_SIDE_EFFECT"
  #   },
  #   {
  #     "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
  #     "node2": { "type": "MedicationClass ", "name": "Medication Class", "column_node_value": 2, "column_node_id": 3 },
  #     "relationship": "MEMBER_OF"
  #   },
  # ]
  worksheet_metadata = [
    {},
    {},
    {
      "node1": { "type": "SurgicalExtent", "name": "Surgical Extent", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "OutcomeType", "name": "Outcome Type", "column_node_value": 8, "column_node_id": 6 },
      "relationship": "ASSOCIATED_WITH"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "OutcomeType", "name": "Outcome Type", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "CAUSED_OUTCOME"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 12, "column_node_id": 13 },
      "node2": { "type": "Gene", "name": "Gene", "column_node_value": 0, "column_node_id": 1 },
      "relationship": "HAS_BIOMARKER"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "TREATS"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "Gene", "name": "Gene", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "HAS_TARGET"
    },
    {},
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "Gene", "name": "Gene", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "HAS_PREDICTIVE_BIOMARKER"
    },
    {
      "node1": { "type": "DiseaseType", "name": "Disease Type", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "Technique", "name": "Technique", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "HAS_REASON"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "MorphologyType", "name": "Morphology Type", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "HAS_MORPHOLOGY"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "Stage", "name": "Prognostic Measures Value Type", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "HAS_STAGE"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "Mechanism", "name": "Mechanism of action", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "HAS_MECHANISM"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "BodyPart", "name": "Body Part", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "HAS_LOCATION"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "SurgicalExtent ", "name": "Surgical Extent", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "TREATS"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "Behavior ", "name": "Behavior", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "HAS_BEHAVIOR"
    },
    {
      "node1": { "type": "NeoplasmType", "name": "Neoplasm Type", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "Technique ", "name": "Technique", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "TREATS"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "DiseaseType ", "name": "Disease Type", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "CAUSED_SIDE_EFFECT"
    },
    {
      "node1": { "type": "MedicationAPI", "name": "Medication: API", "column_node_value": 0, "column_node_id": 1 },
      "node2": { "type": "MedicationClass ", "name": "Medication Class", "column_node_value": 2, "column_node_id": 3 },
      "relationship": "MEMBER_OF"
    },
  ]
  print("worksheet index " + str(read_sheet_index))
  # print(worksheet_metadata[read_sheet_index]["node1"])
  column_node1_id = worksheet_metadata[read_sheet_index]["node1"]["column_node_id"]
  column_node1_type = worksheet_metadata[read_sheet_index]["node1"]["type"]
  column_node1_name = worksheet_metadata[read_sheet_index]["node1"]["name"]
  column_node1_value = worksheet_metadata[read_sheet_index]["node1"]["column_node_value"]
  column_node2_id = worksheet_metadata[read_sheet_index]["node2"]["column_node_id"]
  column_node2_type = worksheet_metadata[read_sheet_index]["node2"]["type"]
  column_node2_name = worksheet_metadata[read_sheet_index]["node2"]["name"]
  column_node2_value = worksheet_metadata[read_sheet_index]["node2"]["column_node_value"]
  relationship = worksheet_metadata[read_sheet_index]["relationship"]
  workbook = openpyxl.load_workbook(read_file_path)
  sheet = workbook.worksheets[read_sheet_index]
  row_count = 0
  print(relationship)
  with open(write_file_path, 'w') as wfile:
    for row in sheet.iter_rows(values_only=True):
      # break if the row is empty [assuming the first column is the key]
      if row[0] is None: 
        break
      if row_count == 0:
        row_count += 1
        continue;
      randomId1 = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for i in range(16))
      randomId2 = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for i in range(16))
      node1 = { "id": row[column_node1_id], "value": row[column_node1_value], "type": column_node1_type, "name": column_node1_name }
      node2 = { "id": row[column_node2_id], "value": row[column_node2_value], "type": column_node2_type, "name": column_node2_name }
      if (row[column_node2_id] == None):
        continue
      else:
        row_count += 1
      # MERGE (p:Person {id: '123'}) ON CREATE SET p.name = 'Alice' MERGE (c:Company {id: '456'}) ON CREATE SET c.name = 'TechCorp' MERGE (p)-[:WORKS_AT]->(c)
      cypress_command = Template("MERGE ($random_id1:$type1 {id: '$id1'}) ON CREATE SET $random_id1.name = '$value1', $random_id1.type = '$type1'  MERGE ($random_id2:$type2 {id: '$id2'}) ON CREATE SET $random_id2.name = '$value2', $random_id2.type = '$type2' MERGE ($random_id1)-[:$relationship]->($random_id2)").substitute(id1=node1["id"], value1=node1["value"], type1=node1["type"], id2=node2["id"], value2=node2["value"], type2=node2["type"], relationship=relationship, random_id1=randomId1, random_id2=randomId2)
      wfile.write(cypress_command + '\n')
  print("Number of rows in sheet: " + str(row_count))
  return;

def run_cypher_file(file_path, batch_size = 1000, offset = 0):
    # Connect to Neo4j
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def execute_queries(tx, queries):
      for query in queries:
          tx.run(query)
    
    # Read Cypher file
    with open(file_path, 'r') as file:
      queries = file.readlines()
    
    with driver.session() as session:
        batches = int(len(queries)/batch_size)
        for i in range(offset, batches + 1):
          start = i * batch_size
          end = i * batch_size + (batch_size - 1)
          start_time = time.time()
          logger.info(str(i) + ": started query from " + str(start))
          # print(str(i) + ": started query from " + str(start) + " at " + str(start_time))
          session.execute_write(execute_queries, queries[start : end])
          end_time = time.time()
          execution_time = end_time - start_time
          logger.info(str(i) + ": ran queries from " + str(start) + " to " + str(end) + " out of " + str(len(queries)) + f" executed in {execution_time} seconds")
          # print(str(i) + ": ran queries from " + str(start) + " to " + str(end) + " out of " + str(len(queries)) + f" executed in {execution_time} seconds")
    # Close the driver connection
    driver.close()

def update_concept_ids(offset = 1):
  concept_id_mapping = {}
  # read concept_id_mapping.json
  with open('data/concept_id_mapping.json', 'r') as rfile:
    concept_id_mapping = json.load(rfile)
  # open concept.json in read mode
  with open('data/concept.json', 'r') as rfile:
    # open add_concept_entity_id.cypher in write mode
    with open('cypher/add_concept_entity_id.cypher', 'a') as wfile:
      lineNum = 0
      while True:
        lineNum += 1
        concept = rfile.readline()
        if ( lineNum < offset):
          continue
        if not concept:
          break
        concept_dict = json.loads(concept)
        id = concept_dict['id']
        if str(id) in concept_id_mapping.keys():
          cypress_command = Template("MATCH(n:Concept) WHERE n.id=$key SET n.entity_id = $value").substitute(key=id, value=concept_id_mapping[str(id)])
          wfile.write(cypress_command + '\n')    
        else:
          print('not found: ', id)

def get_property_types(offset = 1):
  # read file concept.json and read one line at a time
  with open('data/concept.json', 'r') as rfile:
    with open('data/concept_property_types.json', 'a') as wfile:
      lineNum = 0
      while True:
        lineNum += 1
        concept = rfile.readline()
        if lineNum < offset:
          continue
        if not concept:
          break
        concept_dict = json.loads(concept)
        if concept_dict['semantic_type'] == 'Cancer-Numeric-Modifier':
          continue
        # get the property concept by making a POST call to ooo-explorer:80/info
        headers = {'Content-Type': 'application/json'}
        response = requests.post('http://ooo-explorer/info', json={'conceme_id': concept_dict['id']}, headers=headers)
        logger.info(str(concept_dict['id']) + " status: " + str(response.status_code))
        if response.status_code == 200:
          types = response.json()['event_and_property_types']
          values = set()
          if len(types) != 0:
            node_type = types[0].split(':')[0]
          for type in types:
            values.add(type.split(':')[0])
          wfile.write(json.dumps({"id": concept_dict['id'], "property_types": list(values), "node_type": node_type}) + '\n')

def update_missed_updates():
  # read file missing_ids.json and read that as a json array
  with open('data/missing_ids.json', 'r') as rfile:
    missing_ids = json.load(rfile)
    print(missing_ids)
    # for each id in the array, find the matching line from add_concept_entity_id.cypher and run the query on neo4j
    # for id in missing_ids:
    #   with open('cypher/add_concept_entity_id.cypher', 'r') as rfile:
    #     for line in rfile:
    #       if str(id) in line:
    #         # write to a cypher file cypher/missed_updates.cypher
    #         with open('cypher/missed_updates.cypher', 'a') as wfile:
    #           wfile.write(line + '\n')
    #   break

# convert_to_json('data/concept.json', 'json-data/concept.json')
# convert_to_json('data/concept_hierarchy.json', 'json-data/concept_hierarchy.json')
# create_cypher_concept('data/concept.json','cypher/concept_v4.cypher')
# create_cypher_relationships('data/concept_hierarchy.json','cypher/concept_hierarchy_v4.cypher')
# run_cypher_file('cypher/concept.cypher')
# run_cypher_file('cypher/relationships_sheet4.cypher')
# create_cypher_node_with_relationship('excel/relationships.xlsx', 17,'cypher/relationships_sheet17.cypher')
# update_concept_ids()
# run_cypher_file('cypher/add_concept_entity_id.cypher', 100, 3297)
# get_property_types(24217)
update_missed_updates()