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
CREATE (:Station { nameUppercase: row.nom_clean,
    nomGare: row.nom_gare,
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
LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/liaisons.csv' AS row
CREATE (:Liaison { start: row.start,
    stop: row.stop,
    ligne: toInteger(row.ligne)
});
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')


print('Inserting relationship')
query = '''
LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
MATCH (s1:Station {nomGare: s1.nameUppercase})
MATCH (s2:Station {nomGare: s2.nameUppercase})
WHERE s1.nameUppercase <> s2.nameUppercase AND s1.ligne = s2.ligne
WITH s1, s2, point({latitude: s1.latitude, longitude: s1.longitude}) AS startPoint, point({latitude: s2.latitude, longitude: s2.longitude}) AS endPoint
WITH s1, s2, distance(startPoint, endPoint) / 1000 AS distanceKm
MERGE (s1)-[linked:LINKED]->(s2)
SET linked.distance = distanceKm
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')
 