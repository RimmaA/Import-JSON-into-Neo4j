from py2neo import Graph
import os
graph = Graph("http://Rimma1:Rimma@localhost:7474/db/data/")

def scope(data_file):
    import json
    json = json.load(data_file)
    query = """
        with {json} as data
        UNWIND data.url as url
        UNWIND data.response as response

        MERGE (image: Image {URL: url, isDocument: 'yes'})
        FOREACH (l in response.labelAnnotations | 
            MERGE (label:Label {id: l.mid, description:l.description})
            MERGE (image)-[:TAGGED {score: l.score}]->(label))
        FOREACH (a in response.logoAnnotations | 
            MERGE (image)-[:CONTAINS {score: a.score}]->(logo:Logo {description:a.description}))
        FOREACH (a in response.landmarkAnnotations | 
            MERGE (land:Landmark {id: a.mid}) ON CREATE SET land.description=a.description
            MERGE (image)-[:CONTAINS {score: a.score}]->(land)
            FOREACH (b in a.locations | 
                MERGE (land)-[:LOCATED_AT]->(loc:Location {latitude: b.latLng.latitude, longitude: b.latLng.longitude})))
        FOREACH (a in response.webDetection.webEntities | 
            MERGE (wedent:WebEntity {id: a.entityId}) ON CREATE SET wedent.description=a.description
            MERGE (image)-[:TAGGED {score: a.score}]->(wedent))
            
        FOREACH (a in response.webDetection.fullMatchingImages |
            MERGE (image1:Image {URL:a.url})
            MERGE (image)-[:MATCH {type: 'full'}]->(image1))
          
        FOREACH (a in response.webDetection.partialMatchingImages | 
            MERGE (image1:Image {URL:a.url})
            MERGE (image)-[:MATCH {type: 'partial'}]->(image1))
            
        FOREACH (a in response.webDetection.pagesWithMatchingImages | 
            MERGE (page:Page {URL: a.url})
            MERGE (image)-[:IN]->(page))
    """
    print graph.cypher.execute(query, json = json)


path = 'C:/Users/Rimma/Documents/fall 2017/multidate/homework2data/data/json/'
for filename in os.listdir(path):
    with open(path + filename) as data_file:
        scope(data_file)
