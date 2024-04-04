from neo4j import GraphDatabase

driver = GraphDatabase.driver('bolt://0.0.0.0:7687',
                              auth=('neo4j', 'neo4j'))

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
CREATE (:Station { nom_clean: row.nom_clean,
    nom_gare: row.nom_gare,
    latitude: toFloat(row.x),
    longitude: toFloat(row.y),
    trafic: toInteger(row.Trafic),
    ville: row.Ville,
    ligne: toInteger(row.ligne)
});
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')

print('Inserting stations')

query = '''
LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
CREATE (:Station { nom_clean: row.nom_clean,
    nom_gare: row.nom_gare,
    latitude: toFloat(row.x),
    longitude: toFloat(row.y),
    trafic: toInteger(row.Trafic),
    ville: row.Ville,
    ligne: toInteger(row.ligne)
});
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')

print('Inserting liaisons')

query = '''
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/pauldechorgnat/cool-datasets/master/ratp/liaisons.csv' AS row
MATCH (l:Liaison) WHERE l.start = row.start
MATCH (s:Station) WHERE s.nom_clean = row.nom_clean
CREATE (l)-[:APPEAR_IN]->(s);
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')