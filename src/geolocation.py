import asyncio
import aiohttp
import logging

logger = logging.getLogger(__name__)


class GeolocationService:
    """Handles IP geolocation using ip-api.com service."""
    
    def __init__(self, rate_limit_delay = 0.1):
        """
        Initialize geolocation service.
        """
        self.base_url = "http://ip-api.com/json"
        self.rate_limit_delay = rate_limit_delay
        self.session = None
        self._last_request_time = 0
    
    async def _ensure_session(self):
        """Ensure aiohttp session is created."""
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
    
    async def get_location(self, ip):
        """
        Get location information for an IP address.
        """
        await self._ensure_session()
        
        try:
            # Rate limiting
            current_time = asyncio.get_event_loop().time()
            time_since_last = current_time - self._last_request_time
            if time_since_last < self.rate_limit_delay:
                await asyncio.sleep(self.rate_limit_delay - time_since_last)
            
            # This can be adjusted to include/exclude specific fields

            fields = "status,message,country,countryCode,region,regionName,city,lat,lon,timezone,isp,org,as,query"
            url = f"{self.base_url}/{ip}?fields={fields}"
            
            async with self.session.get(url) as response:
                self._last_request_time = asyncio.get_event_loop().time()
                
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('status') == 'success':
                        logger.debug(f"Got location for {ip}: {data.get('city')}, {data.get('country')}")
                        return self._normalize_location_data(data)
                    else:
                        logger.warning(f"API returned error for {ip}: {data.get('message')}")
                        return None
                else:
                    logger.error(f"HTTP error {response.status} for IP {ip}")
                    return None
                    
        except asyncio.TimeoutError:
            logger.error(f"Timeout getting location for {ip}")
            return None
        except Exception as e:
            logger.error(f"Error getting location for {ip}: {e}")
            return None
    
    def _normalize_location_data(self, data):
        """
        Normalize and clean location data from API response.

        """
        return {
            'ip': data.get('query', 'Unknown'),
            'country': data.get('country', 'Unknown'),
            'country_code': data.get('countryCode', 'Unknown'),
            'region': data.get('region', 'Unknown'),
            'region_name': data.get('regionName', 'Unknown'),
            'city': data.get('city', 'Unknown'),
            'latitude': data.get('lat', None),
            'longitude': data.get('lon', None),
            'timezone': data.get('timezone', 'Unknown'),
            'isp': data.get('isp', 'Unknown'),
            'org': data.get('org', 'Unknown'),
            'as_info': data.get('as', 'Unknown')
        }
    
    async def close(self):
        """Close the aiohttp session."""
        if self.session:
            await self.session.close()
            self.session = None