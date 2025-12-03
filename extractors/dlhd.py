import asyncio
import logging
import re
import base64
import json
import gzip
import zlib
import zstandard
import random
from urllib.parse import urlparse
import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector, FormData
from aiohttp_proxy import ProxyConnector
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    pass

class DLHDExtractor:
    """
    DLHD Extractor SENZA CACHE
    
    Ogni richiesta ottiene dati freschi:
    - Token sempre nuovi
    - Auth sempre fresco
    - Nessun rischio di stream che si bloccano per token scaduti
    """

    # Host iframe statici (in ordine di priorità)
    IFRAME_HOSTS = [
        'epicplayplay.cfd',
        'dokoplay.xyz',
    ]

    def __init__(self, request_headers: dict, proxies: list = None):
        self.request_headers = request_headers
        self.base_headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        }
        self.session = None
        self.mediaflow_endpoint = "hls_manifest_proxy"
        self._iframe_context = None
        self.proxies = proxies or []
        # Per compatibilità con codice che potrebbe controllare la cache
        self._stream_data_cache = {}

    def _get_random_proxy(self):
        """Restituisce un proxy casuale dalla lista."""
        return random.choice(self.proxies) if self.proxies else None

    async def _get_session(self):
        """Sessione con cookie jar automatico"""
        if self.session is None or self.session.closed:
            timeout = ClientTimeout(total=60, connect=30, sock_read=30)
            proxy = self._get_random_proxy()
            if proxy:
                logger.info(f"🔗 Utilizzo proxy: {proxy}")
                connector = ProxyConnector.from_url(proxy, ssl=False)
            else:
                connector = TCPConnector(
                    limit=10,
                    limit_per_host=3,
                    keepalive_timeout=30,
                    enable_cleanup_closed=True,
                    force_close=False,
                    use_dns_cache=True
                )
            self.session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self.base_headers,
                cookie_jar=aiohttp.CookieJar()
            )
        return self.session

    async def _handle_response_content(self, response: aiohttp.ClientResponse) -> str:
        """Gestisce la decompressione del corpo della risposta."""
        content_encoding = response.headers.get('Content-Encoding')
        raw_body = await response.read()
        
        try:
            if content_encoding == 'zstd':
                dctx = zstandard.ZstdDecompressor()
                with dctx.stream_reader(raw_body) as reader:
                    decompressed_body = reader.read()
                return decompressed_body.decode(response.charset or 'utf-8')
            elif content_encoding == 'gzip':
                decompressed_body = gzip.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            elif content_encoding == 'deflate':
                decompressed_body = zlib.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            else:
                return raw_body.decode(response.charset or 'utf-8', errors='replace')
        except Exception as e:
            logger.error(f"Errore decompressione da {response.url}: {e}")
            raise ExtractorError(f"Decompressione fallita: {e}")

    async def _make_request(self, url: str, headers: dict = None, retries=3, initial_delay=2):
        """Richiesta HTTP con retry"""
        final_headers = headers.copy() if headers else {}
        final_headers['Accept-Encoding'] = 'gzip, deflate, zstd'
        
        for attempt in range(retries):
            try:
                session = await self._get_session()
                logger.info(f"🌐 Richiesta {attempt + 1}/{retries}: {url}")
                
                async with session.get(url, headers=final_headers, ssl=False, auto_decompress=False) as response:
                    response.raise_for_status()
                    content = await self._handle_response_content(response)
                    
                    class MockResponse:
                        def __init__(self, text_content, status, headers_dict, url):
                            self._text = text_content
                            self.status = status
                            self.headers = headers_dict
                            self.url = url
                        
                        async def text(self):
                            return self._text
                        
                        async def json(self):
                            return json.loads(self._text)
                    
                    logger.info(f"✅ OK: {url}")
                    return MockResponse(content, response.status, response.headers, url)
                    
            except (aiohttp.ClientConnectionError, aiohttp.ServerDisconnectedError,
                    aiohttp.ClientPayloadError, asyncio.TimeoutError, OSError) as e:
                logger.warning(f"⚠️ Errore tentativo {attempt + 1}: {e}")
                
                if attempt == retries - 1:
                    if self.session and not self.session.closed:
                        await self.session.close()
                    self.session = None
                    raise ExtractorError(f"Tutti i {retries} tentativi falliti: {e}")
                
                delay = initial_delay * (2 ** attempt)
                logger.info(f"⏳ Attendo {delay}s...")
                await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"❌ Errore: {e}")
                if attempt == retries - 1:
                    raise ExtractorError(f"Errore finale: {e}")

    # Alias per compatibilità con codice esistente
    async def _make_robust_request(self, url: str, headers: dict = None, retries=3, initial_delay=2):
        """Alias di _make_request per compatibilità"""
        return await self._make_request(url, headers, retries, initial_delay)

    def _extract_channel_id(self, url: str) -> Optional[str]:
        """Estrae l'ID canale dall'URL"""
        patterns = [
            r'/premium(\d+)/mono',
            r'/(?:watch|stream|cast|player)/stream-(\d+)\.php',
            r'watch\.php\?id=(\d+)',
            r'(?:%2F|/)stream-(\d+)\.php',
            r'stream-(\d+)\.php',
            r'[?&]id=(\d+)',
            r'daddyhd\.php\?id=(\d+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    async def extract(self, url: str, **kwargs) -> Dict[str, Any]:
        """
        Estrazione SENZA CACHE - sempre dati freschi
        """
        channel_id = self._extract_channel_id(url)
        if not channel_id:
            raise ExtractorError(f"Impossibile estrarre channel ID da: {url}")

        logger.info(f"📺 Estrazione canale {channel_id} (NO CACHE - dati freschi)")

        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
        
        last_error = None
        for iframe_host in self.IFRAME_HOSTS:
            try:
                iframe_url = f'https://{iframe_host}/premiumtv/daddyhd.php?id={channel_id}'
                logger.info(f"🔍 Tentativo: {iframe_url}")
                
                embed_headers = {
                    'User-Agent': user_agent,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://dlhd.dad/',
                }
                
                # Step 1: Fetch iframe
                resp = await self._make_request(iframe_url, headers=embed_headers, retries=2)
                js_content = await resp.text()
                
                # Step 2: Extract auth params
                params = {}
                patterns = {
                    'channel_key': r'(?:const|var|let)\s+(?:CHANNEL_KEY|channelKey)\s*=\s*["\']([^"\']+)["\']',
                    'auth_token': r'(?:const|var|let)\s+AUTH_TOKEN\s*=\s*["\']([^"\']+)["\']',
                    'auth_country': r'(?:const|var|let)\s+AUTH_COUNTRY\s*=\s*["\']([^"\']+)["\']',
                    'auth_ts': r'(?:const|var|let)\s+AUTH_TS\s*=\s*["\']([^"\']+)["\']',
                    'auth_expiry': r'(?:const|var|let)\s+AUTH_EXPIRY\s*=\s*["\']([^"\']+)["\']',
                }
                for key, pattern in patterns.items():
                    match = re.search(pattern, js_content)
                    params[key] = match.group(1) if match else None
                
                missing = [k for k, v in params.items() if not v]
                if missing:
                    logger.warning(f"⚠️ Parametri mancanti da {iframe_host}: {missing}")
                    last_error = ExtractorError(f"Missing: {missing}")
                    continue
                
                logger.info(f"✅ Params OK: channel_key={params['channel_key']}")
                
                # Step 3: Auth POST
                auth_url = 'https://security.newkso.ru/auth2.php'
                iframe_origin = f"https://{iframe_host}"
                
                form_data = FormData()
                form_data.add_field('channelKey', params['channel_key'])
                form_data.add_field('country', params['auth_country'])
                form_data.add_field('timestamp', params['auth_ts'])
                form_data.add_field('expiry', params['auth_expiry'])
                form_data.add_field('token', params['auth_token'])
                
                auth_headers = {
                    'User-Agent': user_agent,
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Origin': iframe_origin,
                    'Referer': iframe_url,
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'cross-site',
                    'Priority': 'u=1, i',
                }
                
                session = await self._get_session()
                async with session.post(auth_url, data=form_data, headers=auth_headers, ssl=False) as auth_resp:
                    auth_text = await auth_resp.text()
                    logger.info(f"Auth response: {auth_resp.status}")
                    
                    if auth_resp.status != 200 or 'Blocked' in auth_text:
                        logger.warning(f"⚠️ Auth bloccato")
                        last_error = ExtractorError(f"Auth blocked: {auth_text[:100]}")
                        continue
                    
                    try:
                        auth_data = json.loads(auth_text)
                        if not (auth_data.get('success') or auth_data.get('valid')):
                            logger.warning(f"⚠️ Auth fallito: {auth_data}")
                            last_error = ExtractorError(f"Auth failed")
                            continue
                    except json.JSONDecodeError:
                        last_error = ExtractorError("Auth response not JSON")
                        continue
                
                logger.info("✅ Auth OK!")
                
                # Piccola pausa per evitare rate limiting
                await asyncio.sleep(0.5)
                
                # Step 4: Server Lookup
                server_lookup_url = f"https://{iframe_host}/server_lookup.js?channel_id={params['channel_key']}"
                lookup_headers = {
                    'User-Agent': user_agent,
                    'Accept': '*/*',
                    'Referer': iframe_url,
                    'Origin': iframe_origin,
                }
                
                lookup_resp = await self._make_request(server_lookup_url, headers=lookup_headers, retries=2)
                server_data = await lookup_resp.json()
                server_key = server_data.get('server_key')
                
                if not server_key:
                    last_error = ExtractorError(f"No server_key: {server_data}")
                    continue
                
                logger.info(f"✅ Server: {server_key}")
                
                # Step 5: Build URL
                channel_key = params['channel_key']
                auth_token = params['auth_token']
                
                if server_key == 'top1/cdn':
                    stream_url = f'https://top1.newkso.ru/top1/cdn/{channel_key}/mono.css'
                else:
                    stream_url = f'https://{server_key}new.newkso.ru/{server_key}/{channel_key}/mono.css'
                
                logger.info(f"✅ Stream: {stream_url}")
                
                return {
                    "destination_url": stream_url,
                    "request_headers": {
                        'User-Agent': user_agent,
                        'Referer': iframe_url,
                        'Origin': iframe_origin,
                        'Authorization': f'Bearer {auth_token}',
                        'X-Channel-Key': channel_key,
                    },
                    "mediaflow_endpoint": self.mediaflow_endpoint,
                }
                
            except Exception as e:
                logger.warning(f"⚠️ Errore {iframe_host}: {e}")
                last_error = e
                continue
        
        raise ExtractorError(f"Tutti gli host falliti. Ultimo errore: {last_error}")

    async def close(self):
        """Chiude la sessione"""
        if self.session and not self.session.closed:
            await self.session.close()
        self.session = None

    async def invalidate_cache_for_url(self, url: str):
        """
        Stub per compatibilità con app.py.
        Non fa nulla perché non c'è cache.
        """
        logger.info(f"🔄 invalidate_cache_for_url chiamato per {url} (no-op, nessuna cache)")
        pass


# ============== TEST ==============
async def test():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Simula gli headers che verrebbero passati dal proxy
    request_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    extractor = DLHDExtractor(request_headers=request_headers)
    
    try:
        # Test canale 877
        result = await extractor.extract("https://daddyhd.com/embed/stream-877.php")
        
        print("\n" + "="*60)
        print("🎉 RISULTATO (NO CACHE)")
        print("="*60)
        print(f"Stream URL: {result['destination_url']}")
        print(f"Referer: {result['request_headers'].get('Referer')}")
        print(f"Origin: {result['request_headers'].get('Origin')}")
        print("="*60)
        
        # Verifica che lo stream funzioni
        async with aiohttp.ClientSession() as session:
            async with session.head(
                result['destination_url'],
                headers=result['request_headers'],
                ssl=False
            ) as resp:
                print(f"\n🔍 Verifica stream: {resp.status}")
                if resp.status == 200:
                    print("✅ Stream VALIDO!")
                else:
                    print(f"⚠️ Status: {resp.status}")
                    
    finally:
        await extractor.close()


if __name__ == "__main__":
    asyncio.run(test())
