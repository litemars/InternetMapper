import asyncio
import logging
import argparse

from src.dns_resolver import DNSResolver
from src.traceroute import TracerouteRunner
from src.geolocation import GeolocationService
from src.graph_db import GraphDatabase
from src.utils import parse_config_yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/network_analyzer.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class NetworkPathAnalyzer:    
    def __init__(self, config):
        self.config = config
        self.dns_resolver = DNSResolver()
        self.traceroute_runner = TracerouteRunner()
        self.geolocation_service = GeolocationService()
        self.graph_db = GraphDatabase(config['neo4j'])
    
    async def analyze_domains(self, domains):
        """Analyze network paths for a list of domains."""
        logger.info(f"Starting analysis for {len(domains)} domains")
        
        for domain in domains:
            try:
                await self.analyze_single_domain(domain)
            except Exception as e:
                logger.error(f"Failed to analyze {domain}: {e}")
    
    async def analyze_single_domain(self, domain):
        """Analyze network path for a single domain."""
        logger.info(f"Analyzing domain: {domain}")
        
        # Step 1: DNS Resolution
        target_ip = await self.dns_resolver.resolve(domain)
        if not target_ip:
            logger.warning(f"Could not resolve {domain}")
            return
        
        logger.info(f"Resolved {domain} to {target_ip}")
        
        # Step 2: Traceroute
        hops = await self.traceroute_runner.run_traceroute(target_ip)
        if not hops:
            logger.warning(f"No traceroute hops found for {target_ip}")
            return
        
        logger.info(f"Found {len(hops)} hops for {domain}")
        
        # Step 3: Geolocation lookup for all hops
        enriched_hops = []
        for hop in hops:
            geo_data = await self.geolocation_service.get_location(hop)
            enriched_hops.append({
                'ip': hop,
                'geo_data': geo_data
            })
        
        # Step 4: Store in graph database
        await self.graph_db.store_network_path(domain, target_ip, enriched_hops)
        logger.info(f"Stored network path for {domain} in graph database")
    
    async def close(self):
        """Cleanup resources."""
        await self.graph_db.close()
        await self.geolocation_service.close()

async def main():

    parser = argparse.ArgumentParser(description='Network Path Analyzer')
    parser.add_argument('domains', nargs='*', help='Domains to analyze (overridden by --domains-file if provided)')
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    parser.add_argument('--domains-file', help='Path to file containing domains (one per line)')
    parser.add_argument('--export-db', help='Export the graph database to a JSON file')
    parser.add_argument('--import-db', help='Import the graph database from a JSON file')
    
    args = parser.parse_args()


    # Determine domains list
    if args.domains_file:
        with open(args.domains_file, 'r') as f:
            domains = [line.strip() for line in f if line.strip()]
    else:
        domains = args.domains
    print(f"Domains to analyze: {domains}")
    
    analyzer = NetworkPathAnalyzer(parse_config_yaml(args.config))

    try:
        if args.export_db:
            await analyzer.graph_db.export_graph_to_json(args.export_db)
            logger.info(f"Exported graph database to {args.export_db}")
        elif args.import_db:
            await analyzer.graph_db.import_graph_from_json(args.import_db)
            logger.info(f"Imported graph database from {args.import_db}")
        else:
            await analyzer.analyze_domains(domains)
    finally:
        await analyzer.close()


if __name__ == "__main__":
    asyncio.run(main())