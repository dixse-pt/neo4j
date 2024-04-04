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

# inserting data
print('Inserting stations')
query = '''
LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
CREATE (:Station {
    name: row.nom_clean,
    gare: row.nom_gare,
    lat: toFloat(row.x),
    long: toFloat(row.y),
    trafic: toInteger(row.Trafic),
    ville: row.Ville,
    ligne: toInteger(row.ligne)
});
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')