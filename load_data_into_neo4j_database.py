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

# MATCH (s1:Station {nomGare: s1.nameUppercase})
# MATCH (s2:Station {nomGare: s2.nameUppercase})
# WHERE s1.nameUppercase <> s2.nameUppercase AND s1.ligne = s2.ligne
# WITH s1, s2, point({latitude: s1.latitude, longitude: s1.longitude}) AS startPoint, point({latitude: s2.latitude, longitude: s2.longitude}) AS endPoint
# WITH s1, s2, distance(startPoint, endPoint) / 1000 AS distanceKm
# MERGE (s1)-[linked:LINKED]->(s2)
# SET linked.distance = distanceKm
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')
 
#print('Inserting liaisons')

# query = '''
# LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
# CREATE (:Station {
#     nameUppercase: row.nom_clean,
#     nomGare: row.nom_gare,
#     latitude: toFloat(row.x),
#     longitude: toFloat(row.y),
#     trafic: toInteger(row.Trafic),
#     ville: row.Ville,
#     ligne: toInteger(row.ligne)
# });

# MATCH (s1:Station {nomGare: s1.nameUppercase})
# MATCH (s2:Station {nomGare: s2.nameUppercase})
# WHERE s1.nameUppercase <> s2.nameUppercase AND s1.ligne = s2.ligne
# WITH s1, s2, point({latitude: s1.latitude, longitude: s1.longitude}) AS startPoint, point({latitude: s2.latitude, longitude: s2.longitude}) AS endPoint
# WITH s1, s2, distance(startPoint, endPoint) / 1000 AS distanceKm
# MERGE (s1)-[linked:LINKED]->(s2)
# SET linked.distance = distanceKm
# '''

# with driver.session() as session:
#     print(query)
#     session.run(query)

# print('done')
 
# print('Creating liaisons relationships')

# # Création des relations entre stations et liaisons
# print('Creating relationships')

# queries = [
#     '''
#     LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
#     MATCH (s:Station {id: row.nom_clean})
#     MATCH (l:Liaison {start: row.start})
#     MATCH (l1:Liaison {stop: row.stop})
#     CREATE (s)-[:LIGNE]->(l)
#     CREATE (s)-[:LIGNE]->(l1);
#     ''',
# ]

# Utilisation d'une transaction pour exécuter chaque requête
# with driver.session() as session:
#     for query in queries:
#         print(query)
#         session.run(query)
# print('done')
