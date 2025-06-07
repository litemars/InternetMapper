import socket
import asyncio
import logging

logger = logging.getLogger(__name__)


class DNSResolver:
    """Handles DNS resolution for domains."""
    
    def __init__(self):
        pass
    
    async def resolve(self, domain):
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, 
                socket.gethostbyname, 
                domain
            )
            logger.debug(f"Resolved {domain} to {result}")
            return result
            
        except socket.gaierror as e:
            logger.error(f"DNS resolution failed for {domain}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error resolving {domain}: {e}")
            return None
    
    async def resolve_multiple(self, domains: list):
        tasks = [self.resolve(domain) for domain in domains]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            domain: result if not isinstance(result, Exception) else None
            for domain, result in zip(domains, results)
        }