import asyncio
import logging
import re
import base64
import json
import os
import gzip
import zlib
import zstandard
import random
import time
from urllib.parse import urlparse, quote_plus
import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector, FormData
from aiohttp_proxy import ProxyConnector
from typing import Dict, Any, Optional
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    pass

class DLHDExtractor:
    """DLHD Extractor con sessione persistente e gestione anti-bot avanzata"""


    def __init__(self, request_headers: dict, proxies: list = None):
        self.request_headers = request_headers
        self.base_headers = {
            # ✅ User-Agent più recente per bypassare protezioni anti-bot
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        }
        self.session = None
        self.mediaflow_endpoint = "hls_manifest_proxy"
        self._session_lock = asyncio.Lock()
        self.proxies = proxies or []
        self._extraction_locks: Dict[str, asyncio.Lock] = {} # ✅ NUOVO: Lock per evitare estrazioni multiple
        self.cache_file = os.path.join(os.path.dirname(__file__), '.dlhd_cache')
        
        # Carica cache e inizializza host
        cache_data = self._load_cache()
        self._stream_data_cache: Dict[str, Dict[str, Any]] = cache_data.get('streams', {})
        
        # ✅ Lista host iframe (caricata da cache o vuota)
        self.iframe_hosts = cache_data.get('hosts', [])
        
        # ✅ Configurazione server dinamica dal worker
        self.auth_url = cache_data.get('auth_url', 'https://security.giokko.ru/auth2.php')
        self.stream_cdn = cache_data.get('stream_cdn', 'https://top1.giokko.ru')
        self.stream_pattern = cache_data.get('stream_pattern', 'https://{KEY}new.giokko.ru')
        
        logger.info(f"Hosts caricati all'avvio: {self.iframe_hosts}")
        logger.info(f"Auth URL: {self.auth_url}")
        logger.info(f"Stream CDN: {self.stream_cdn}")
        logger.info(f"Stream Pattern: {self.stream_pattern}")

    def _load_cache(self) -> Dict[str, Any]:
        """Carica la cache da un file codificato in Base64 all'avvio. Ritorna struttura completa."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    logger.info(f"💾 Caricamento cache dal file: {self.cache_file}")
                    encoded_data = f.read()
                    if not encoded_data:
                         # Tenta di gestire il vecchio formato o ritorna default
                        return {'hosts': [], 'streams': {}}
                    
                    try:
                        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
                        data = json.loads(decoded_data)
                        
                        # Retrocompatibilità: se la cache è il vecchio formato (dict di stream), convertilo
                        if data and not ('hosts' in data and 'streams' in data):
                             # Assumiamo che keys siano channel_ids
                             # Verifica euristica: se le chiavi sono stringhe numeriche, è il vecchio formato
                             is_old_format = any(k.isdigit() for k in data.keys())
                             if is_old_format or not data:
                                 logger.info("Rilevato vecchio formato cache, conversione in corso...")
                                 return {'hosts': [], 'streams': data}
                        
                        return data
                    except Exception:
                        # Se fallisce decode, prova a leggerlo come JSON plain (fallback)
                        f.seek(0)
                        return json.load(f)

        except (IOError, json.JSONDecodeError, Exception) as e:
            logger.error(f"❌ Errore durante il caricamento della cache: {e}. Inizio con una cache vuota.")
        return {'hosts': [], 'streams': {}}

    def _get_random_proxy(self):
        """Restituisce un proxy casuale dalla lista."""
        return random.choice(self.proxies) if self.proxies else None

    async def _get_session(self):
        """✅ Sessione persistente con cookie jar automatico"""
        if self.session is None or self.session.closed:
            timeout = ClientTimeout(total=60, connect=30, sock_read=30)
            proxy = self._get_random_proxy()
            if proxy:
                logger.info(f"🔗 Utilizzo del proxy {proxy} per la sessione DLHD.")
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
                logger.info("ℹ️ Nessun proxy specifico per DLHD, uso connessione diretta.")
            # ✅ FONDAMENTALE: Cookie jar per mantenere sessione come browser reale
            self.session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self.base_headers,
                cookie_jar=aiohttp.CookieJar()
            )
        return self.session

    def _save_cache(self):
        """Salva lo stato corrente della cache su un file, codificando il contenuto in Base64."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                # Struttura completa
                cache_data = {
                    'hosts': self.iframe_hosts,
                    'streams': self._stream_data_cache,
                    'auth_url': self.auth_url,
                    'stream_cdn': self.stream_cdn,
                    'stream_pattern': self.stream_pattern
                }
                json_data = json.dumps(cache_data)
                encoded_data = base64.b64encode(json_data.encode('utf-8')).decode('utf-8')
                f.write(encoded_data)
                logger.info(f"💾 Cache (stream + hosts) salvata con successo.")
        except IOError as e:
            logger.error(f"❌ Errore durante il salvataggio della cache: {e}")

    async def _fetch_iframe_hosts(self) -> bool:
        """Scarica la lista aggiornata degli host iframe."""
        # URL offuscato per evitare scraping statico
        encoded_url = "aHR0cHM6Ly9pZnJhbWUuZGxoZC5kcGRucy5vcmcv"
        url = base64.b64decode(encoded_url).decode('utf-8')
        
        logger.info(f"🔄 Aggiornamento lista host iframe...")
        try:
            session = await self._get_session()
            async with session.get(url, ssl=False, timeout=ClientTimeout(total=10)) as response:
                if response.status == 200:
                    text = await response.text()
                    # ✅ Parsing con supporto per configurazione completa
                    lines = [line.strip() for line in text.splitlines() if line.strip()]
                    new_hosts = []
                    
                    for line in lines:
                        if line.startswith('#AUTH_URL:'):
                            self.auth_url = line.replace('#AUTH_URL:', '').strip()
                            logger.info(f"✅ Auth URL aggiornato: {self.auth_url}")
                        elif line.startswith('#STREAM_CDN:'):
                            self.stream_cdn = line.replace('#STREAM_CDN:', '').strip()
                            logger.info(f"✅ Stream CDN aggiornato: {self.stream_cdn}")
                        elif line.startswith('#STREAM_PATTERN:'):
                            self.stream_pattern = line.replace('#STREAM_PATTERN:', '').strip()
                            logger.info(f"✅ Stream Pattern aggiornato: {self.stream_pattern}")
                        elif not line.startswith('#'):
                            new_hosts.append(line)
                    
                    if new_hosts:
                        self.iframe_hosts = new_hosts
                        logger.info(f"✅ Lista host aggiornata: {self.iframe_hosts}")
                        self._save_cache()
                        return True
                    else:
                         logger.warning("⚠️ Lista host scaricata ma vuota.")
                else:
                    logger.error(f"❌ Errore HTTP {response.status} durante aggiornamento host iframe.")
        except Exception as e:
            logger.error(f"❌ Eccezione durante aggiornamento host iframe: {e}")
        
        return False

    def _get_headers_for_url(self, url: str, base_headers: dict) -> dict:
        """Applica headers specifici per il dominio stream automaticamente"""
        headers = base_headers.copy()
        parsed_url = urlparse(url)
        
        # Estrai dominio base da stream_cdn (es: 'https://top1.giokko.ru' -> 'giokko.ru')
        try:
            stream_domain = urlparse(self.stream_cdn).netloc.split('.', 1)[-1]  # 'top1.giokko.ru' -> 'giokko.ru'
        except:
            stream_domain = 'giokko.ru'  # Fallback
        
        if stream_domain in parsed_url.netloc:
            origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
            special_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                'Referer': origin,
                'Origin': origin
            }
            headers.update(special_headers)
        
        return headers

    async def _handle_response_content(self, response: aiohttp.ClientResponse) -> str:
        """Gestisce la decompressione manuale del corpo della risposta (zstd, gzip, etc.)."""
        content_encoding = response.headers.get('Content-Encoding')
        raw_body = await response.read()
        
        try:
            if content_encoding == 'zstd':
                logger.info(f"Rilevata compressione zstd per {response.url}. Decompressione in corso...")
                try:
                    dctx = zstandard.ZstdDecompressor()
                    # ✅ MODIFICA: Utilizza stream_reader per gestire frame senza dimensione del contenuto.
                    # Questo risolve l'errore "could not determine content size in frame header".
                    with dctx.stream_reader(raw_body) as reader:
                        decompressed_body = reader.read()
                    return decompressed_body.decode(response.charset or 'utf-8')
                except zstandard.ZstdError as e:
                    logger.error(f"Errore di decompressione Zstd: {e}. Il contenuto potrebbe essere incompleto o corrotto.")
                    raise ExtractorError(f"Fallimento decompressione zstd: {e}")
            elif content_encoding == 'gzip':
                logger.info(f"Rilevata compressione gzip per {response.url}. Decompressione in corso...")
                decompressed_body = gzip.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            elif content_encoding == 'deflate':
                logger.info(f"Rilevata compressione deflate per {response.url}. Decompressione in corso...")
                decompressed_body = zlib.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            else:
                # Nessuna compressione o compressione non gestita, prova a decodificare direttamente
                # ✅ FIX: Usa 'errors=replace' per evitare crash su byte non validi
                return raw_body.decode(response.charset or 'utf-8', errors='replace')
        except Exception as e:
            logger.error(f"Errore durante la decompressione/decodifica del contenuto da {response.url}: {e}")
            raise ExtractorError(f"Fallimento decompressione per {response.url}: {e}")

    async def _make_robust_request(self, url: str, headers: dict = None, retries=3, initial_delay=2):
        """✅ Richieste con sessione persistente per evitare anti-bot"""
        final_headers = self._get_headers_for_url(url, headers or {})
        # Aggiungiamo zstd agli header accettati per segnalare al server che lo supportiamo
        # Rimosso 'br' perché non gestito in _handle_response_content
        final_headers['Accept-Encoding'] = 'gzip, deflate, zstd'
        
        for attempt in range(retries):
            try:
                # ✅ IMPORTANTE: Riusa sempre la stessa sessione
                session = await self._get_session()
                
                logger.info(f"Tentativo {attempt + 1}/{retries} per URL: {url}")
                async with session.get(url, headers=final_headers, ssl=False, auto_decompress=False) as response:
                    response.raise_for_status()
                    content = await self._handle_response_content(response)
                    
                    class MockResponse:
                        def __init__(self, text_content, status, headers_dict):
                            self._text = text_content
                            self.status = status
                            self.headers = headers_dict
                            self.url = url
                        
                        async def text(self):
                            return self._text
                            
                        def raise_for_status(self):
                            if self.status >= 400:
                                raise aiohttp.ClientResponseError(
                                    request_info=None, 
                                    history=None,
                                    status=self.status
                                )
                        
                        async def json(self):
                            return json.loads(self._text)
                    
                    logger.info(f"✅ Richiesta riuscita per {url} al tentativo {attempt + 1}")
                    return MockResponse(content, response.status, response.headers)
                    
            except (
                aiohttp.ClientConnectionError, 
                aiohttp.ServerDisconnectedError,
                aiohttp.ClientPayloadError,
                asyncio.TimeoutError,
                OSError,
                ConnectionResetError,
            ) as e:
                logger.warning(f"⚠️ Errore connessione tentativo {attempt + 1} per {url}: {str(e)}")
                
                # ✅ Solo in caso di errore critico, chiudi la sessione
                if attempt == retries - 1:
                    if self.session and not self.session.closed:
                        try:
                            await self.session.close()
                        except:
                            pass
                    self.session = None
                
                if attempt < retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    logger.info(f"⏳ Aspetto {delay} secondi prima del prossimo tentativo...")
                    await asyncio.sleep(delay)
                else:
                    raise ExtractorError(f"Tutti i {retries} tentativi falliti per {url}: {str(e)}")
                    
            except Exception as e:
                # Controlla se l'errore è dovuto a zstd e logga un messaggio specifico
                if 'zstd' in str(e).lower():
                    logger.critical(f"❌ Errore critico con la decompressione zstd. Assicurati che la libreria 'zstandard' sia installata (`pip install zstandard`). Errore: {e}")
                else: # type: ignore
                    logger.error(f"❌ Errore non di rete tentativo {attempt + 1} per {url}: {str(e)}")
                if attempt == retries - 1:
                    raise ExtractorError(f"Errore finale per {url}: {str(e)}")
        await asyncio.sleep(initial_delay)

    async def extract(self, url: str, force_refresh: bool = False, **kwargs) -> Dict[str, Any]:
        """Flusso di estrazione principale: estrae direttamente dall'iframe."""
        
        async def get_stream_data_direct(channel_id: str, hosts_to_try: list) -> Dict[str, Any]:
            """Estrazione diretta dall'iframe senza passare per la pagina principale."""
            
            user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
            
            last_error = None
            for iframe_host in hosts_to_try:
                try:
                    iframe_url = f'https://{iframe_host}/premiumtv/daddyhd.php?id={channel_id}'
                    logger.info(f"🔍 Tentativo estrazione da: {iframe_url}")
                    
                    embed_headers = {
                        'User-Agent': user_agent,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Referer': 'https://dlhd.dad/',
                        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"macOS"',
                    }
                    
                    # Step 1: Fetch iframe page
                    resp = await self._make_robust_request(iframe_url, headers=embed_headers, retries=2)
                    js_content = await resp.text()
                    
                    # Check if it's lovecdn
                    if 'lovecdn.ru' in js_content:
                        logger.info("Detected lovecdn.ru content - using alternative extraction")
                        result = await self._extract_lovecdn_stream(iframe_url, js_content, embed_headers)
                        return result
                    
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
                    
                    missing_params = [k for k, v in params.items() if not v]
                    if missing_params:
                        logger.warning(f"⚠️ Parametri mancanti da {iframe_host}: {missing_params}")
                        last_error = ExtractorError(f"Missing params: {missing_params}")
                        continue
                    
                    logger.info(f"✅ Parametri estratti: channel_key={params['channel_key']}")
                    
                    # Step 3: Auth POST
                    # ✅ DINAMICO: usa self.auth_url completo
                    auth_url = self.auth_url
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
                        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"macOS"',
                    }
                    
                    session = await self._get_session()
                    async with session.post(auth_url, data=form_data, headers=auth_headers, ssl=False) as auth_resp:
                        auth_text = await auth_resp.text()
                        logger.info(f"Auth response: {auth_resp.status} - {auth_text[:100]}")
                        
                        if auth_resp.status != 200 or 'Blocked' in auth_text or 'bad params' in auth_text.lower():
                            logger.warning(f"⚠️ Auth bloccato da {iframe_host}: {auth_text[:50]}")
                            
                            # ✅ NUOVO: Se è il primo host e auth fallisce, prova a refreshare config
                            if iframe_host == hosts_to_try[0] and not getattr(self, '_config_refreshed', False):
                                logger.info("🔄 Auth fallito, provo ad aggiornare config dal worker...")
                                self._config_refreshed = True
                                if await self._fetch_iframe_hosts():
                                    # Aggiorna auth_url per il prossimo tentativo
                                    auth_url = self.auth_url
                                    logger.info(f"✅ Config aggiornata, nuovo auth_url: {auth_url}")
                            
                            last_error = ExtractorError(f"Auth blocked: {auth_text}")
                            continue
                        
                        try:
                            auth_data = json.loads(auth_text)
                            if not (auth_data.get('success') or auth_data.get('valid')):
                                logger.warning(f"⚠️ Auth fallito: {auth_data}")
                                last_error = ExtractorError(f"Auth failed: {auth_data}")
                                continue
                        except json.JSONDecodeError:
                            last_error = ExtractorError(f"Auth response not JSON: {auth_text}")
                            continue
                    
                    logger.info("✅ Auth riuscito!")
                    
                    # Step 4: Server Lookup
                    server_lookup_url = f"https://{iframe_host}/server_lookup.js?channel_id={params['channel_key']}"
                    lookup_headers = {
                        'User-Agent': user_agent,
                        'Accept': '*/*',
                        'Referer': iframe_url,
                        'Origin': iframe_origin,
                    }
                    
                    lookup_resp = await self._make_robust_request(server_lookup_url, headers=lookup_headers, retries=2)
                    server_data = await lookup_resp.json()
                    server_key = server_data.get('server_key')
                    
                    if not server_key:
                        last_error = ExtractorError(f"No server_key in response: {server_data}")
                        continue
                    
                    logger.info(f"✅ Server key: {server_key}")
                    
                    # Step 5: Build final URL
                    channel_key = params['channel_key']
                    auth_token = params['auth_token']
                    
                    # ✅ DINAMICO: usa self.stream_cdn e self.stream_pattern
                    if server_key == 'top1/cdn':
                        stream_url = f'{self.stream_cdn}/top1/cdn/{channel_key}/mono.css'
                    else:
                        # Sostituisci {KEY} con server_key nel pattern
                        base_url = self.stream_pattern.replace('{KEY}', server_key)
                        stream_url = f'{base_url}/{server_key}/{channel_key}/mono.css'
                    
                    logger.info(f"✅ Stream URL costruito: {stream_url}")
                    
                    stream_headers = {
                        'User-Agent': user_agent,
                        'Referer': iframe_url,
                        'Origin': iframe_origin,
                        'Authorization': f'Bearer {auth_token}',
                        'X-Channel-Key': channel_key,
                    }

                    # ✅ Aggiungi cookies dalla sessione corrente
                    if self.session:
                        # Log all cookies for debugging
                        all_cookies = list(self.session.cookie_jar)
                        logger.info(f"🍪 All cookies in jar: {all_cookies}")
                        
                        cookies = self.session.cookie_jar.filter_cookies(stream_url)
                        cookie_str = "; ".join([f"{k}={v.value}" for k, v in cookies.items()])
                        if cookie_str:
                            stream_headers['Cookie'] = cookie_str
                            logger.info(f"🍪 Cookies aggiunti agli headers: {cookie_str[:50]}...")

                    expires_at = None
                    try:
                        if params.get('auth_expiry'):
                            expires_at = float(params['auth_expiry'])
                            logger.info(f"⏳ Auth Expiry: {expires_at} (Current time: {time.time()})")
                    except (ValueError, TypeError):
                        pass
                    
                    # ✅ Reset flag per permettere futuri refresh
                    self._config_refreshed = False
                    
                    return {
                        "destination_url": stream_url,
                        "request_headers": stream_headers,
                        "mediaflow_endpoint": self.mediaflow_endpoint,
                        "expires_at": expires_at
                    }
                    
                except Exception as e:
                    logger.warning(f"⚠️ Errore con {iframe_host}: {e}")
                    last_error = e
                    continue
            
            raise ExtractorError(f"Tutti gli host iframe hanno fallito. Ultimo errore: {last_error}")

        # Helper per estrarre ID (spostato fuori per pulizia scope)
        def extract_channel_id(u: str) -> Optional[str]:
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
                match = re.search(pattern, u, re.IGNORECASE)
                if match:
                    return match.group(1)
            return None

        try:
            channel_id = extract_channel_id(url)
            if not channel_id:
                raise ExtractorError(f"Impossibile estrarre channel ID da {url}")

            logger.info(f"📺 Estrazione per canale ID: {channel_id}")

            # Controlla la cache prima di procedere
            if not force_refresh and channel_id in self._stream_data_cache:
                logger.info(f"✅ Trovati dati in cache per il canale ID: {channel_id}. Verifico la validità...")
                cached_data = self._stream_data_cache[channel_id]
                stream_url = cached_data.get("destination_url")
                stream_headers = cached_data.get("request_headers", {})
                expires_at = cached_data.get("expires_at")

                is_valid = False
                
                # ✅ Check expiry first (con buffer di 30 secondi)
                if expires_at and time.time() > (expires_at - 30):
                     logger.warning(f"⚠️ Cache scaduta per il canale ID {channel_id} (expires_at: {expires_at}).")
                     is_valid = False
                elif stream_url:
                    try:
                        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as validation_session:
                            async with validation_session.head(stream_url, headers=stream_headers, ssl=False) as response:
                                if response.status == 200:
                                    is_valid = True
                                    logger.info(f"✅ Cache per il canale ID {channel_id} è valida.")
                                else:
                                    logger.warning(f"⚠️ Cache per il canale ID {channel_id} non valida. Status: {response.status}. Procedo con estrazione.")
                    except Exception as e:
                        logger.warning(f"⚠️ Errore durante la validazione della cache per {channel_id}: {e}. Procedo con estrazione.")
                
                if not is_valid:
                    if channel_id in self._stream_data_cache:
                        del self._stream_data_cache[channel_id]
                    self._save_cache()
                    logger.info(f"🗑️ Cache invalidata per il canale ID {channel_id}.")
                else:
                    return cached_data

            # Usa un lock per prevenire estrazioni simultanee per lo stesso canale
            if channel_id not in self._extraction_locks:
                self._extraction_locks[channel_id] = asyncio.Lock()
            
            lock = self._extraction_locks[channel_id]
            async with lock:
                # Ricontrolla la cache dopo aver acquisito il lock
                if not force_refresh and channel_id in self._stream_data_cache:
                    cached_data = self._stream_data_cache[channel_id]
                    expires_at = cached_data.get("expires_at")
                    
                    # Se è scaduta anche la nuova cache (improbabile ma possibile), procedi con estrazione
                    if expires_at and time.time() > (expires_at - 30):
                        logger.info(f"⚠️ Cache (ricontrollata) scaduta per {channel_id}, procedo con nuova estrazione.")
                    else:
                        logger.info(f"✅ Dati per il canale {channel_id} trovati in cache dopo aver atteso il lock.")
                        return self._stream_data_cache[channel_id]

                # Procedi con l'estrazione diretta
                # Procedi con l'estrazione diretta
                logger.info(f"⚙️ Nessuna cache valida per {channel_id}, avvio estrazione diretta...")
                
                try:
                    result = await get_stream_data_direct(channel_id, self.iframe_hosts)
                except ExtractorError:
                    # Se fallisce con gli host correnti, prova ad aggiornarli
                    logger.warning("⚠️ Tutti gli host correnti hanno fallito. Tento aggiornamento lista host...")
                    if await self._fetch_iframe_hosts():
                         logger.info(f"🔄 Riprovo con nuovi host: {self.iframe_hosts}")
                         result = await get_stream_data_direct(channel_id, self.iframe_hosts)
                    else:
                        raise
                
                # Salva in cache
                self._stream_data_cache[channel_id] = result
                self._save_cache()
                
                return result
            
        except Exception as e:
            # Per errori 403, non loggare il traceback perché sono errori attesi (servizio temporaneamente non disponibile)
            error_message = str(e)
            if "403" in error_message or "Forbidden" in error_message:
                logger.error(f"Estrazione DLHD completamente fallita per URL {url}")
            else:
                logger.exception(f"Estrazione DLHD completamente fallita per URL {url}")
            raise ExtractorError(f"Estrazione DLHD completamente fallita: {str(e)}")

    async def _extract_lovecdn_stream(self, iframe_url: str, iframe_content: str, headers: dict) -> Dict[str, Any]:
        """
        Estrattore alternativo per iframe lovecdn.ru che usa un formato diverso.
        """
        try:
            # Cerca pattern di stream URL diretto
            m3u8_patterns = [
                r'["\']([^"\']*\.m3u8[^"\']*)["\']',
                r'source[:\s]+["\']([^"\']+)["\']',
                r'file[:\s]+["\']([^"\']+\.m3u8[^"\']*)["\']',
                r'hlsManifestUrl[:\s]*["\']([^"\']+)["\']',
            ]
            
            stream_url = None
            for pattern in m3u8_patterns:
                matches = re.findall(pattern, iframe_content)
                for match in matches:
                    if '.m3u8' in match and match.startswith('http'):
                        stream_url = match
                        logger.info(f"Found direct m3u8 URL: {stream_url}")
                        break
                if stream_url:
                    break
            
            # Pattern 2: Cerca costruzione dinamica URL
            if not stream_url:
                channel_match = re.search(r'(?:stream|channel)["\s:=]+["\']([^"\']+)["\']', iframe_content)
                server_match = re.search(r'(?:server|domain|host)["\s:=]+["\']([^"\']+)["\']', iframe_content)
                
                if channel_match:
                    channel_name = channel_match.group(1)
                    # Estrai dominio base da stream_cdn come fallback
                    try:
                        fallback_domain = urlparse(self.stream_cdn).netloc.split('.', 1)[-1]
                    except:
                        fallback_domain = 'giokko.ru'
                    server = server_match.group(1) if server_match else fallback_domain
                    stream_url = f"https://{server}/{channel_name}/mono.m3u8"
                    logger.info(f"Constructed stream URL: {stream_url}")
            
            if not stream_url:
                # Fallback: cerca qualsiasi URL che sembri uno stream
                url_pattern = r'https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*'
                matches = re.findall(url_pattern, iframe_content)
                if matches:
                    stream_url = matches[0]
                    logger.info(f"Found fallback stream URL: {stream_url}")
            
            if not stream_url:
                raise ExtractorError(f"Could not find stream URL in lovecdn.ru iframe")
            
            # Usa iframe URL come referer
            iframe_origin = f"https://{urlparse(iframe_url).netloc}"
            stream_headers = {
                'User-Agent': headers['User-Agent'],
                'Referer': iframe_url,
                'Origin': iframe_origin
            }
            
            # Determina endpoint in base al dominio dello stream
            # Se è un m3u8 standard, usa hls_manifest_proxy, se richiede chiavi speciali, potrebbe servire altro
            # Ma per ora manteniamo hls_manifest_proxy come nel codice originale
            
            logger.info(f"Using lovecdn.ru stream extraction")
            
            return {
                "destination_url": stream_url,
                "request_headers": stream_headers,
                "mediaflow_endpoint": self.mediaflow_endpoint,
            }
            
        except Exception as e:
            raise ExtractorError(f"Failed to extract lovecdn.ru stream: {e}")

    async def _extract_new_auth_flow(self, iframe_url: str, iframe_content: str, headers: dict) -> Dict[str, Any]:
        """Gestisce il nuovo flusso di autenticazione."""
        
        def _extract_params(js: str) -> Dict[str, Optional[str]]:
            params = {}
            patterns = {
                "channel_key": r'(?:const|var|let)\s+(?:CHANNEL_KEY|channelKey)\s*=\s*["\']([^"\']+)["\']',
                "auth_token": r'(?:const|var|let)\s+AUTH_TOKEN\s*=\s*["\']([^"\']+)["\']',
                "auth_country": r'(?:const|var|let)\s+AUTH_COUNTRY\s*=\s*["\']([^"\']+)["\']',
                "auth_ts": r'(?:const|var|let)\s+AUTH_TS\s*=\s*["\']([^"\']+)["\']',
                "auth_expiry": r'(?:const|var|let)\s+AUTH_EXPIRY\s*=\s*["\']([^"\']+)["\']',
            }
            for key, pattern in patterns.items():
                match = re.search(pattern, js)
                params[key] = match.group(1) if match else None
            return params

        params = _extract_params(iframe_content)
        
        missing_params = [k for k, v in params.items() if not v]
        if missing_params:
            # This is not an error, just means it's not the new flow
            raise ExtractorError(f"Not the new auth flow: missing params {missing_params}")

        logger.info("New auth flow detected. Proceeding with POST auth.")
        
        # 1. Initial Auth POST
        auth_url = self.auth_url  # Usa auth_url dinamico
        form_data = FormData()
        form_data.add_field('channelKey', params["channel_key"])
        form_data.add_field('country', params["auth_country"])
        form_data.add_field('timestamp', params["auth_ts"])
        form_data.add_field('expiry', params["auth_expiry"])
        form_data.add_field('token', params["auth_token"])

        iframe_origin = f"https://{urlparse(iframe_url).netloc}"
        auth_headers = headers.copy()
        auth_headers.update({
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': iframe_origin,
            'Referer': iframe_url,
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'Priority': 'u=1, i',
        })
        
        try:
            session = await self._get_session()
            async with session.post(auth_url, data=form_data, headers=auth_headers, ssl=False) as auth_resp:
                auth_resp.raise_for_status()
                auth_data = await auth_resp.json()
                if not (auth_data.get("valid") or auth_data.get("success")):
                    raise ExtractorError(f"Initial auth failed with response: {auth_data}")
            logger.info("New auth flow: Initial auth successful.")
        except Exception as e:
            raise ExtractorError(f"New auth flow failed during initial auth POST: {e}")

        # 2. Server Lookup
        server_lookup_url = f"https://{urlparse(iframe_url).netloc}/server_lookup.js?channel_id={params['channel_key']}"
        try:
            lookup_resp = await self._make_robust_request(server_lookup_url, headers=headers)
            server_data = await lookup_resp.json()
            server_key = server_data.get('server_key')
            if not server_key:
                raise ExtractorError(f"No server_key in lookup response: {server_data}")
            logger.info(f"New auth flow: Server lookup successful - Server key: {server_key}")
        except Exception as e:
            raise ExtractorError(f"New auth flow failed during server lookup: {e}")

        # 3. Build final stream URL
        channel_key = params['channel_key']
        auth_token = params['auth_token']
        # The JS logic uses .css, not .m3u8
        if server_key == 'top1/cdn':
            stream_url = f'{self.stream_cdn}/top1/cdn/{channel_key}/mono.css'
        else:
            base_url = self.stream_pattern.replace('{KEY}', server_key)
            stream_url = f'{base_url}/{server_key}/{channel_key}/mono.css'
        
        logger.info(f'New auth flow: Constructed stream URL: {stream_url}')

        stream_headers = {
            'User-Agent': headers['User-Agent'],
            'Referer': iframe_url,
            'Origin': iframe_origin,
            'Authorization': f'Bearer {auth_token}',
            'X-Channel-Key': channel_key
        }

        return {
            "destination_url": stream_url,
            "request_headers": stream_headers,
            "mediaflow_endpoint": self.mediaflow_endpoint,
        }

    async def invalidate_cache_for_url(self, url: str):
        """
        Invalida la cache per un URL specifico.
        Questa funzione viene chiamata da app.py quando rileva un errore (es. fallimento chiave AES).
        """
        def extract_channel_id_internal(u: str) -> Optional[str]:
            patterns = [
                r'/premium(\d+)/mono\.m3u8$',
                r'/(?:watch|stream|cast|player)/stream-(\d+)\.php',
                r'watch\.php\?id=(\d+)',
                r'(?:%2F|/)stream-(\d+)\.php',
                r'stream-(\d+)\.php'
            ]
            for pattern in patterns:
                match = re.search(pattern, u, re.IGNORECASE)
                if match: return match.group(1)
            return None

        channel_id = extract_channel_id_internal(url)
        if channel_id and channel_id in self._stream_data_cache:
            del self._stream_data_cache[channel_id]
            self._save_cache()
            logger.info(f"🗑️ Cache per il canale ID {channel_id} invalidata a causa di un errore esterno (es. chiave AES).")

    async def close(self):
        """Chiude definitivamente la sessione"""
        if self.session and not self.session.closed:
            try:
                await self.session.close()
            except:
                pass
        self.session = None
