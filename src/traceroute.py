"""
Traceroute module for network path analysis.
"""

import asyncio
import subprocess
import re
import logging

logger = logging.getLogger(__name__)


class TracerouteRunner:
    """Handles traceroute execution and parsing."""
    
    def __init__(self, max_hops = 38, timeout = 5):
        self.max_hops = max_hops
        self.timeout = timeout
    
    async def run_traceroute(self, target_ip):
        """
        Run traceroute to target IP and return list of hop IPs.
    
        """
        try:
            cmd = self._build_traceroute_command(target_ip)
            
            # Run traceroute asynchronously
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), 
                timeout=120  # 60 second timeout for entire traceroute
            )
            
            if process.returncode != 0:
                logger.warning(f"Traceroute returned non-zero exit code: {stderr.decode()}")
            
            return self._parse_traceroute_output(stdout.decode())
            
        except asyncio.TimeoutError:
            logger.error(f"Traceroute to {target_ip} timed out")
            return []
        except Exception as e:
            logger.error(f"Traceroute to {target_ip} failed: {e}")
            return []
    
    def _build_traceroute_command(self, target_ip):
            return [
                'traceroute',
                '-m', str(self.max_hops),
                # '-w', str(self.timeout),
                #'-n',  # Don't resolve hostnames
                target_ip
            ]
    
    def _parse_traceroute_output(self, output: str):
        """
        Parse traceroute output and extract hop IPs.
        """
        hops = []
        
        # IP address regex pattern
        ip_pattern = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
        
        for line in output.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            if 'traceroute' in line.lower() or 'tracing route' in line.lower():
                continue
            
            # Find IP addresses in the line
            ip_matches = re.findall(ip_pattern, line)
            
            if ip_matches:
                hop_ip = ip_matches[0]
                
                if self._is_valid_hop_ip(hop_ip):
                    hops.append(hop_ip)
                    
        
        logger.info(f"Parsed {len(hops)} valid hops from traceroute output")
        return hops
    
    def _is_valid_hop_ip(self, ip: str):
        """
        Check if IP address is valid for our analysis.
        TODO: This might be improved.
        """
        if ip.startswith('127.') or ip.startswith('255.'):
            return False
        
        if ip == '0.0.0.0':
            return False
        
        # if (ip.startswith('10.') or 
        #     ip.startswith('192.168.') or 
        #     ip.startswith('172.16.') or
        #     ip.startswith('169.254.')):
        #     return False
        
        return True