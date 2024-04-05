from neo4j import GraphDatabase

driver = GraphDatabase.driver('bolt://0.0.0.0:7687', auth=('neo4j', 'neo4j'))

# deleting data
print('Deleting previous data')

query = '''
MATCH (n) 
DETACH DELETE n
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')

print('Inserting stations')

query = '''
LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
CREATE (:Station { 
    nom_gare: row.nom_gare,
    nom_clean: row.nom_clean,
    Trafic: toInteger(row.Trafic),
    Ville: row.Ville,
    ligne: toString(row.ligne)
});

'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')

print('Inserting ligne')

query = '''
LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
WITH DISTINCT row.ligne AS ligne
CREATE (:Ligne { ligne: toString(ligne)});
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')

print('Inserting connections')

query = '''
LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
MATCH (station:Station {nom_gare: row.nom_gare})
MATCH (ligne:Ligne {ligne: toString(row.ligne)})
CREATE (station)-[:BELONGS_TO]->(ligne);
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')
