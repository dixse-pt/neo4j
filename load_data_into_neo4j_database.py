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

query = '''
LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
CREATE (:Station {
    nom_gare: row.nom_gare,
    nom_clean: row.nom_clean,
    x: toFloat(row.x),
    y: toFloat(row.y),
    Trafic: toInteger(row.Trafic),
    Ville: row.Ville
});
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')

#Importer les données des liaisons depuis le fichier CSV
query = '''
LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/liaisons.csv' AS row
MATCH (start:Station {nom_gare: row.start})
MATCH (stop:Station {nom_gare: row.stop})
MERGE (start)-[:CONNECTS_TO {ligne: row.ligne}]->(stop);
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')

# Créer les relations pour les correspondances entre les lignes
query = '''
MATCH (station:Station)
WITH station, station.nom_gare AS nom_gare, collect(station) AS stations
UNWIND range(0, size(stations)-2) AS i
UNWIND range(i+1, size(stations)-1) AS j
WITH stations[i] AS station1, stations[j] AS station2
CREATE (station1)-[:CORRESPONDS_TO]->(station2)
CREATE (station2)-[:CORRESPONDS_TO]->(station1)
RETURN count(*);
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')


#Créer les relations pour les liaisons à pied
query = '''
MATCH (s1:Station), (s2:Station)
WHERE s1 <> s2
WITH s1, s2, distance(
  point({x: s1.x, y: s1.y}),
  point({x: s2.x, y: s2.y})
) AS dist
WHERE dist < 1000 // Distance inférieure à 1 km
CREATE (s1)-[:WALKS_TO {distance: dist / 4000}]->(s2);
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')

#Créer les relations pour les liaisons par train
query = '''
MATCH (start:Station)-[:CONNECTS_TO]->(stop:Station)
WHERE start <> stop
WITH start, stop, distance(
  point({x: start.x, y: start.y}),
  point({x: stop.x, y: stop.y})
) AS dist
CREATE (start)-[:TAKES_TRAIN_TO {time: dist / 40000}]->(stop);
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')