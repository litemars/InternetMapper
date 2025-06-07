import logging
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

logger = logging.getLogger(__name__)


class GraphDatabase:
    
    def __init__(self, config):
        self.uri = config.get('uri', 'bolt://localhost:7687')
        self.username = config.get('username', 'neo4j')
        self.password = config.get('password', 'neo4j')
        self.database = config.get('database', 'neo4j')
        self.driver = None
    
    async def connect(self):
        try:
            self.driver = AsyncGraphDatabase.driver(
                self.uri,
                auth=(self.username, self.password)
            )
            
            # Test the connection
            async with self.driver.session(database=self.database) as session:
                await session.run("RETURN 1")
            
            logger.info("Connected to Neo4j database")
            
        except (ServiceUnavailable, AuthError) as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    
    async def ensure_connected(self):
        if self.driver is None:
            await self.connect()
    
    async def store_network_path(self, domain, target_ip, hops):
        await self.ensure_connected()
        
        async with self.driver.session(database=self.database) as session:
            try:
                await session.execute_write(
                    self._create_network_path_tx,
                    domain, target_ip, hops
                )
                logger.info(f"Stored network path for {domain} with {len(hops)} hops")
                
            except Exception as e:
                logger.error(f"Failed to store network path for {domain}: {e}")
                raise
    
    @staticmethod
    async def _create_network_path_tx(tx, domain, target_ip, hops):
        """
        Transaction function to create network path in database.
        """
        # Create or update domain node
        await tx.run(
            """
            MERGE (d:Domain {name: $domain})
            SET d.target_ip = $target_ip,
                d.last_analyzed = datetime(),
                d.hop_count = $hop_count
            """,
            domain=domain,
            target_ip=target_ip,
            hop_count=len(hops)
        )
        
        for i, hop_data in enumerate(hops):
            # Ensure hop_data is a dict
            if not isinstance(hop_data, dict):
                logger.warning(f"Invalid hop_data at index {i}: {hop_data}")
                continue
            ip = hop_data.get('ip')
            geo_data = hop_data.get('geo_data') or {}
            if not isinstance(geo_data, dict):
                geo_data = {}
            
            # Create or update IP node with geolocation data
            await tx.run(
                """
                MERGE (ip:IP {address: $ip})
                SET ip.latitude = $latitude,
                    ip.longitude = $longitude,
                    ip.city = $city,
                    ip.last_seen = datetime()
                """,
                ip=ip,
                latitude=geo_data.get('latitude', None),
                longitude=geo_data.get('longitude', None),
                city=geo_data.get('city', 'Unknown'),
                domain=domain            
                )
            
            # Create hop relationship
            if i == 0:
                # First hop - connect from domain
                await tx.run(
                    """
                    MATCH (d:Domain {name: $domain})
                    MATCH (ip:IP {address: $ip})
                    MERGE (d)-[r:ROUTES_TO {hop_number: $hop_number}]->(ip)
                    SET r.timestamp = datetime(),
                        r.domains = CASE WHEN $domain IN coalesce(r.domains, []) THEN r.domains ELSE coalesce(r.domains, []) + $domain END
                    """,
                    domain=domain,
                    ip=ip,
                    hop_number=i + 1
                )
            else:
                # Subsequent hops - connect from previous IP
                await tx.run(
                    """
                    MATCH (prev:IP {address: $prev_ip})
                    MATCH (curr:IP {address: $curr_ip})
                    MERGE (prev)-[r:ROUTES_TO {hop_number: $hop_number}]->(curr)
                    SET r.timestamp = datetime(),
                        r.domains = CASE WHEN $domain IN coalesce(r.domains, []) THEN r.domains ELSE coalesce(r.domains, []) + $domain END
                    """,
                    prev_ip=hops[i-1]['ip'],
                    curr_ip=ip,
                    hop_number=i + 1,
                    domain=domain
                )
            
    
    async def get_network_paths(self):
        """
        Retrieve all network paths for all domains.
        
        Returns:
            List of network path data
        """
        await self.ensure_connected()
        
        async with self.driver.session(database=self.database) as session:
            result = await session.run(
                """
                MATCH path = (d:Domain)-[:ROUTES_TO*]->(ip:IP)
                RETURN path
                ORDER BY length(path)
                """
            )
            
            paths = []
            async for record in result:
                path = record["path"]
                paths.append(self._format_path_data(path))
            
            return paths
    
    def _format_path_data(self, path):
        """Format Neo4j path data for return."""
        nodes = []
        for node in path.nodes:
            if "Domain" in node.labels:
                nodes.append({
                    'type': 'domain',
                    'name': node['name'],
                    'target_ip': node.get('target_ip')
                })
            elif "IP" in node.labels:
                nodes.append({
                    'type': 'ip',
                    'name': node['name'],
                    'address': node['address'],
                    'country': node.get('country'),
                    'city': node.get('city'),
                    'isp': node.get('isp')
                })
        
        return {
            'nodes': nodes,
            'length': len(nodes)
        }
    
    async def close(self):
        """Close database connection."""
        if self.driver:
            await self.driver.close()
            self.driver = None
            logger.info("Closed Neo4j connection")
    
    def _make_json_serializable(self, obj):
        """
        Recursively convert non-JSON-serializable values (e.g., DateTime) to strings.
        """
        if isinstance(obj, dict):
            return {k: self._make_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(v) for v in obj]
        elif hasattr(obj, 'iso_format'):
            # Neo4j DateTime objects
            return str(obj)
        elif hasattr(obj, '__str__') and not isinstance(obj, (str, int, float, bool, type(None))):
            return str(obj)
        else:
            return obj

    async def export_graph_to_json(self, file_path: str):
        """
        Export the entire graph database (all nodes and relationships) to a local JSON file.
        """
        import json
        await self.ensure_connected()
        async with self.driver.session(database=self.database) as session:
            # Export all nodes
            nodes_result = await session.run("MATCH (n) RETURN id(n) as id, labels(n) as labels, n as node")
            nodes = []
            async for record in nodes_result:
                node_obj = record['node']
                props = self._make_json_serializable(dict(node_obj._properties))
                nodes.append({
                    'id': record['id'],
                    'labels': record['labels'],
                    'properties': props
                })
            # Export all relationships
            rels_result = await session.run("MATCH ()-[r]->() RETURN id(r) as id, type(r) as type, r as rel, id(startNode(r)) as start, id(endNode(r)) as end")
            relationships = []
            async for record in rels_result:
                rel_obj = record['rel']
                props = self._make_json_serializable(dict(rel_obj._properties))
                relationships.append({
                    'id': record['id'],
                    'type': record['type'],
                    'properties': props,
                    'start': record['start'],
                    'end': record['end']
                })
            # Write to file
            with open(file_path, 'w') as f:
                json.dump({'nodes': nodes, 'relationships': relationships}, f, indent=2)
            logger.info(f"Exported graph database to {file_path}")
    
    async def import_graph_from_json(self, file_path: str):
        """
        Import all nodes and relationships from a local JSON file into the graph database.
        Args:
            file_path: Path to the input JSON file
        """
        import json
        await self.ensure_connected()
        with open(file_path, 'r') as f:
            data = json.load(f)
        nodes = data.get('nodes', [])
        relationships = data.get('relationships', [])
        async with self.driver.session(database=self.database) as session:
            # Create nodes
            for node in nodes:
                labels = ":".join(node['labels'])
                props = node['properties']
                props['import_id'] = node['id']  # Temporary property for matching
                await session.run(
                    f"MERGE (n:{labels} {{import_id: $import_id}}) SET n += $props",
                    import_id=node['id'], props=props
                )
            # Create relationships
            for rel in relationships:
                await session.run(
                    """
                    MATCH (a {import_id: $start})
                    MATCH (b {import_id: $end})
                    MERGE (a)-[r:%s]->(b)
                    SET r += $props
                    """ % rel['type'],
                    start=rel['start'], end=rel['end'], props=rel['properties']
                )
            # Remove temporary import_id property
            await session.run("MATCH (n) REMOVE n.import_id")
        logger.info(f"Imported graph database from {file_path}")