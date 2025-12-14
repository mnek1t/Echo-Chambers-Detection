

`cd kafka`
`docker-compose up -d`

Launch Neo4J and start the instance with the port `7687`: 

`docker rm qdrant`

`docker run -d   --name qdrant   -p 6333:6333   -p 6334:6334   qdrant/qdrant:latest`


To launch : 

`python3 main.py`