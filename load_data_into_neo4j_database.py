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
CREATE (:Station {name: row.nom_clean, gare: row.nom_gare, x: row.x, y: row.y, trafic: row.Trafic, ville: row.Ville, ligne: row.ligne });
'''

with driver.session() as session:
    print(query)
    session.run(query)

print('done')
 
print('Creating relationships')

queries = [
    '''// Loading acting and changing the labels
    LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/pauldechorgnat/cool-datasets/master/ratp/liaisons.csv' AS row
    MATCH (p:Station) WHERE p.ligne = row.ligne
    CREATE (p)-[:LIGNE]->(c)
    WITH p
    SET p :Ligne
    RETURN p;''',

]

with driver.session() as session:
    for q in queries:
        print(q)
        session.run(q)

print('done')