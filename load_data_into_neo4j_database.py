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
#
query = '''
LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
CREATE (:Station {
    nom_gare: row.nom_gare,
    nom_clean: row.nom_clean,
    x: toFloat(row.x),
    y: toFloat(row.y),
    Trafic: toInteger(row.Trafic),
    Ville: row.Ville,
    ligne: row.ligne
});
'''