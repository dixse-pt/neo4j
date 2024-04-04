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
CREATE (:Station {id: row.nom_clean, gare: row.nom_gare, x: row.x, y: row.y, trafic: row.Trafic, ville: row.Ville, ligne: row.ligne });
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')
 
print('Inserting liaisons')

query = '''
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/pauldechorgnat/cool-datasets/master/ratp/liaisons.csv' AS row
CREATE (:Liaison {start: row.start, stop: row.stop, ligne: row.ligne });
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')
 
print('Creating liaisons relationships')

# Création des relations entre stations et liaisons
print('Creating relationships')

queries = [
    '''
    LOAD CSV WITH HEADERS FROM 'https://github.com/pauldechorgnat/cool-datasets/raw/master/ratp/stations.csv' AS row
    MATCH (s:Station {id: row.nom_clean})
    MATCH (l:Liaison {start: row.start})
    MATCH (l1:Liaison {stop: row.stop})
    CREATE (s)-[:LIGNE]->(l)
    CREATE (s)-[:LIGNE]->(l1);
    ''',
]

# Utilisation d'une transaction pour exécuter chaque requête
with driver.session() as session:
    for query in queries:
        print(query)
        session.run(query)
print('done')
