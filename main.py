import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from playwright.async_api import Browser, BrowserContext, Page, async_playwright
from pydantic import BaseModel, Field, validator

from utils import (
    PageLogger,
    extract_all_sezioni,
    login,
    logout,
    run_visura,
    run_visura_immobile,
    run_visura_persona_fisica,
)

# Carica variabili d'ambiente: .env poi .env.local (override, tipico sviluppo locale)
load_dotenv()
load_dotenv(".env.local", override=True)

# Configurazione logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()

# Create logs directory if it doesn't exist and we have permission
log_handlers = [logging.StreamHandler()]
try:
    if not os.path.exists("./logs"):
        os.makedirs("./logs", exist_ok=True)
    log_handlers.append(logging.FileHandler("./logs/visura.log"))
except (PermissionError, OSError) as e:
    print(f"Warning: Cannot create log file: {e}")

logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=log_handlers,
)
logger = logging.getLogger(__name__)


# Custom Exception Classes
class VisuraError(Exception):
    """Base exception for visura-related errors"""

    pass


class AuthenticationError(VisuraError):
    """Raised when authentication fails"""

    pass


class BrowserError(VisuraError):
    """Raised when browser operations fail"""

    pass


class ValidationError(VisuraError):
    """Raised when input validation fails"""

    pass


@dataclass
class VisuraRequest:
    request_id: str
    tipo_catasto: str
    provincia: str
    comune: str
    foglio: str
    particella: str
    sezione: Optional[str] = None
    subalterno: Optional[str] = None  # Opzionale: restringe la ricerca per fabbricati
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class VisuraIntestatiRequest:
    """Richiesta per ottenere gli intestati di un immobile specifico"""

    request_id: str
    tipo_catasto: str
    provincia: str
    comune: str
    foglio: str
    particella: str
    subalterno: Optional[str] = None
    sezione: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class VisuraPersonaFisicaRequest:
    """Ricerca visura per persona fisica (beni catastali)."""

    request_id: str
    provincia: str
    comune: str
    pf_tipo_catasto: Optional[str] = None
    pf_comune_catastale: Optional[str] = None
    pf_search_by: str = "cognome"
    pf_cognome: Optional[str] = None
    pf_nome: Optional[str] = None
    pf_codice_fiscale: Optional[str] = None
    pf_birth_day: Optional[str] = None
    pf_birth_month: Optional[str] = None
    pf_birth_year: Optional[str] = None
    pf_sesso: Optional[str] = None
    pf_birth_province: Optional[str] = None
    pf_tipo_ispezione: str = "R"
    pf_limitata: Optional[str] = None
    tipo_richiesta: str = "A"
    richiedente: Optional[str] = None
    motivo: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class VisuraResponse:
    request_id: str
    success: bool
    tipo_catasto: str
    data: Optional[Dict] = None
    error: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class BrowserManager:
    def __init__(self):
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.auth_page: Optional[Page] = None
        self.authenticated = False
        self.keep_alive_running = False
        self.last_login_time = None
        self._session_lock = asyncio.Lock()
        self._keep_alive_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Inizializza il browser e il contexto"""
        try:
            # Ferma un'eventuale istanza Playwright precedente per evitare
            # processi Chromium orfani al re-init (session recovery, restart).
            if self.playwright is not None:
                try:
                    await self.playwright.stop()
                except Exception as e:
                    logger.warning(f"Errore stop playwright precedente: {e}")
                self.playwright = None

            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                handle_sigint=False,  # Non chiudere Chromium su Ctrl+C — gestiamo noi il logout
                handle_sigterm=False,  # Idem per SIGTERM
                args=[
                    # NB: '--single-process' rimosso: incompatibile con Docker,
                    # causa crash sporadici su re-init.
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--no-first-run",
                    "--no-zygote",
                    "--disable-extensions",
                ],
            )

            self.context = await self.browser.new_context()
            logger.info("Browser inizializzato")
        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            raise BrowserError(f"Browser initialization failed: {e}") from e

    async def login(self):
        """Esegue il login nella prima tab"""
        try:
            # Chiudi la vecchia pagina prima di crearne una nuova
            if self.auth_page and not self.auth_page.is_closed():
                try:
                    await self.auth_page.close()
                    logger.info("Vecchia pagina di autenticazione chiusa")
                except Exception as e:
                    logger.warning(f"Errore chiudendo vecchia pagina: {e}")

            page = await self.context.new_page()
            await login(page)
            self.auth_page = page
            self.authenticated = True
            self.last_login_time = datetime.now()
            logger.info("Login completato con successo")
        except Exception as e:
            logger.error(f"Errore durante il login: {e}")
            self.authenticated = False
            raise AuthenticationError(f"Login failed: {e}") from e

    async def start_keep_alive(self):
        """Avvia un solo worker keep-alive (annulla eventuale task precedente)."""
        await self._cancel_keep_alive_task()
        self.keep_alive_running = True
        self._keep_alive_task = asyncio.create_task(self._keep_alive_loop())

    async def _keep_alive_loop(self):
        last_check = datetime.now()
        while self.keep_alive_running:
            try:
                if self.auth_page and not self.auth_page.is_closed():
                    current_time = datetime.now()

                    if (current_time - last_check).total_seconds() > 300:
                        await self._perform_session_refresh()
                        last_check = current_time
                    else:
                        await self._perform_light_keepalive()

                await asyncio.sleep(30)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Errore in keep-alive: {e}")
                await asyncio.sleep(60)

    async def _cancel_keep_alive_task(self):
        self.keep_alive_running = False
        if self._keep_alive_task and not self._keep_alive_task.done():
            self._keep_alive_task.cancel()
            try:
                await self._keep_alive_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(f"Errore cancellazione keep-alive: {e}")
        self._keep_alive_task = None

    async def _perform_light_keepalive(self):
        """Keep-alive leggero: movimento del mouse"""
        try:
            await self.auth_page.mouse.move(100, 100)
            await asyncio.sleep(0.1)
            await self.auth_page.mouse.move(200, 200)
            logger.debug("Keep-alive movimento mouse eseguito")
        except Exception as e:
            logger.warning(f"Errore in light keep-alive: {e}")

    async def _perform_session_refresh(self):
        """Refresh approfondito della sessione navigando alla pagina di scelta servizio"""
        try:
            logger.info("Eseguendo refresh della sessione...")

            await self.auth_page.goto(
                "https://sister3.agenziaentrate.gov.it/Visure/SceltaServizio.do?tipo=/T/TM/VCVC_", timeout=30000
            )
            await self.auth_page.wait_for_load_state("networkidle", timeout=15000)

            try:
                provincia_options = await self.auth_page.locator("select[name='listacom'] option").count()
                if provincia_options <= 1:
                    logger.warning("Sessione scaduta durante refresh - province non disponibili")
                    self.authenticated = False
                    return False
                else:
                    logger.info(f"Session refresh completato - {provincia_options-1} province disponibili")
                    return True
            except Exception as e:
                logger.warning(f"Errore nel verificare province: {e}")
                self.authenticated = False
                return False

        except Exception as e:
            logger.error(f"Errore in session refresh: {e}")
            self.authenticated = False
            return False

    async def stop_keep_alive(self):
        """Ferma il keep-alive e attende la terminazione del task."""
        await self._cancel_keep_alive_task()

    async def session_logout(self) -> Dict[str, Any]:
        """Logout esplicito sul portale ADE/SISTER senza chiudere Chromium.

        Se il click su Esci non viene rilevato, pulisce i cookie del context per evitare
        sessioni fantasma che bloccano un nuovo accesso (\"utente già in sessione\").
        """
        async with self._session_lock:
            await self._cancel_keep_alive_task()

            logout_ok = False
            if self.auth_page and not self.auth_page.is_closed():
                try:
                    logout_ok = await logout(self.auth_page)
                except Exception as e:
                    logger.warning(f"Eccezione durante logout UI: {e}")

                try:
                    await self.auth_page.close()
                except Exception as e:
                    logger.warning(f"Errore chiusura pagina post-logout: {e}")

            self.auth_page = None
            self.authenticated = False

            cookies_cleared = False
            if self.context and not logout_ok:
                try:
                    await self.context.clear_cookies()
                    cookies_cleared = True
                    logger.info("Cookie del browser context cancellati dopo logout non confermato da UI")
                except Exception as e:
                    logger.warning(f"Pulizia cookie fallita: {e}")

            return {
                "logout_ui_confirmed": logout_ok,
                "cookies_cleared": cookies_cleared,
                "authenticated": False,
                "message": "Sessione locale terminata; sul portale usa sempre Esci quando possibile.",
            }

    async def session_login(self) -> Dict[str, Any]:
        """Riesegue il login (stesso flusso dell'avvio: SPID o sister_tab da LOGIN_METHOD)."""
        async with self._session_lock:
            await self._cancel_keep_alive_task()
            await self.login()
            await self.start_keep_alive()
            return {
                "authenticated": self.authenticated,
                "message": "Login completato.",
            }

    async def _check_session_validity(self):
        """Verifica se la sessione è ancora valida"""
        try:
            if not self.auth_page or self.auth_page.is_closed():
                logger.warning("Pagina di autenticazione non disponibile")
                return False

            current_url = self.auth_page.url
            if "agenziaentrate.gov.it" not in current_url or "sister" not in current_url:
                logger.warning(f"Non siamo più nel portale SISTER - URL: {current_url}")
                return False

            if "SceltaServizio.do" not in current_url:
                await self.auth_page.goto(
                    "https://sister3.agenziaentrate.gov.it/Visure/SceltaServizio.do?tipo=/T/TM/VCVC_", timeout=30000
                )
                await self.auth_page.wait_for_load_state("networkidle", timeout=15000)

            provincia_options = await self.auth_page.locator("select[name='listacom'] option").count()
            if provincia_options <= 1:
                logger.warning("Province non disponibili - sessione probabilmente scaduta")
                return False

            logger.info(f"Sessione valida - {provincia_options-1} province disponibili")
            return True

        except Exception as e:
            logger.error(f"Errore nella verifica della sessione: {e}")
            return False

    async def _try_session_recovery(self) -> bool:
        """Tenta di recuperare la sessione SISTER senza rifare il login SPID.
        Naviga direttamente alla pagina di scelta servizio e verifica se è ancora valida."""
        try:
            if not self.auth_page or self.auth_page.is_closed():
                return False

            recovery_logger = PageLogger("recovery")
            logger.info("Tentativo di recupero sessione SISTER senza SPID...")

            # Prova a navigare direttamente alla pagina Visure
            await self.auth_page.goto(
                "https://sister3.agenziaentrate.gov.it/Visure/SceltaServizio.do?tipo=/T/TM/VCVC_", timeout=30000
            )
            await self.auth_page.wait_for_load_state("networkidle", timeout=15000)
            await recovery_logger.log(self.auth_page, "goto_scelta_servizio")

            current_url = self.auth_page.url
            content = await self.auth_page.content()

            # Se siamo stati reindirizzati al login → sessione scaduta davvero
            if "iampe.agenziaentrate.gov.it" in current_url or "Login" in current_url:
                logger.info("Sessione SISTER scaduta, serve login SPID completo")
                return False

            # Se c'è errore di sessione bloccata
            if "Utente gia' in sessione" in content or "error_locked.jsp" in current_url:
                logger.warning("Utente già in sessione, serve login SPID completo")
                return False

            # Verifica che ci siano le province (segno che la sessione funziona)
            provincia_options = await self.auth_page.locator("select[name='listacom'] option").count()
            if provincia_options > 1:
                logger.info(f"Sessione SISTER recuperata! {provincia_options-1} province disponibili")
                self.authenticated = True
                self.last_login_time = datetime.now()
                return True

            # Se la pagina è quella giusta ma senza province, proviamo il percorso completo
            if "agenziaentrate.gov.it" in current_url and "sister" in current_url:
                try:
                    await self.auth_page.get_by_role("button", name="Conferma").click(timeout=5000)
                    await recovery_logger.log(self.auth_page, "conferma")
                    await self.auth_page.get_by_role("link", name="Consultazioni e Certificazioni").click(timeout=5000)
                    await recovery_logger.log(self.auth_page, "consultazioni")
                    await self.auth_page.get_by_role("link", name="Visure catastali").click(timeout=5000)
                    await recovery_logger.log(self.auth_page, "visure_catastali")
                    await self.auth_page.get_by_role("link", name="Conferma Lettura").click(timeout=5000)
                    await recovery_logger.log(self.auth_page, "conferma_lettura")

                    logger.info("Sessione SISTER recuperata tramite navigazione interna")
                    self.authenticated = True
                    self.last_login_time = datetime.now()
                    return True
                except Exception as e:
                    logger.warning(f"Navigazione interna fallita: {e}")
                    await recovery_logger.log(self.auth_page, "navigazione_fallita")
                    return False

            await recovery_logger.log(self.auth_page, "stato_sconosciuto")
            return False

        except Exception as e:
            logger.warning(f"Recupero sessione fallito: {e}")
            return False

    async def _ensure_authenticated(self):
        """Assicura che il sistema sia autenticato, ri-autentica se necessario.
        Prima tenta il recupero sessione senza SPID, poi fallback a login completo."""
        if not self.authenticated or not await self._check_session_validity():
            # Step 1: tenta recupero sessione senza SPID
            if await self._try_session_recovery():
                logger.info("Sessione recuperata senza login SPID")
                return

            # Step 2: fallback a login SPID completo
            logger.info("Sessione non recuperabile, login SPID completo...")
            try:
                await self.login()
                await self.start_keep_alive()
                logger.info("Re-autenticazione SPID completata")
            except Exception as e:
                logger.error(f"Errore nella re-autenticazione: {e}")
                raise AuthenticationError(f"Re-authentication failed: {e}") from e

    async def esegui_visura(self, request: VisuraRequest) -> VisuraResponse:
        """Esegue una visura catastale (solo dati catastali, senza intestati)"""
        async with self._session_lock:
            try:
                await self._ensure_authenticated()

                try:
                    result = await run_visura(
                        self.auth_page,
                        request.provincia,
                        request.comune,
                        request.sezione,
                        request.foglio,
                        request.particella,
                        request.tipo_catasto,
                        extract_intestati=False,
                        subalterno=request.subalterno,
                    )
                except Exception as e:
                    raise BrowserError(f"Failed to execute visura: {e}") from e

                logger.info(f"Visura completata per request {request.request_id}")
                return VisuraResponse(
                    request_id=request.request_id,
                    success=True,
                    tipo_catasto=request.tipo_catasto,
                    data=result,
                )

            except (AuthenticationError, BrowserError) as e:
                logger.error(f"Errore in visura {request.request_id}: {e}")
                return VisuraResponse(
                    request_id=request.request_id,
                    success=False,
                    tipo_catasto=request.tipo_catasto,
                    error=str(e),
                )
            except Exception as e:
                logger.error(f"Errore inatteso in visura {request.request_id}: {e}")
                return VisuraResponse(
                    request_id=request.request_id,
                    success=False,
                    tipo_catasto=request.tipo_catasto,
                    error=f"Errore inatteso: {str(e)}",
                )

    async def esegui_visura_intestati(self, request: VisuraIntestatiRequest) -> VisuraResponse:
        """Esegue una visura per ottenere gli intestati di un immobile specifico."""
        async with self._session_lock:
            try:
                await self._ensure_authenticated()

                if request.tipo_catasto == "F" and request.subalterno:
                    result = await run_visura_immobile(
                        self.auth_page,
                        provincia=request.provincia,
                        comune=request.comune,
                        sezione=request.sezione,
                        foglio=request.foglio,
                        particella=request.particella,
                        subalterno=request.subalterno,
                    )
                else:
                    result = await run_visura(
                        self.auth_page,
                        request.provincia,
                        request.comune,
                        request.sezione,
                        request.foglio,
                        request.particella,
                        request.tipo_catasto,
                        extract_intestati=True,
                    )

                logger.info(f"Visura intestati completata per {request.request_id}")
                return VisuraResponse(
                    request_id=request.request_id,
                    success=True,
                    tipo_catasto=request.tipo_catasto,
                    data=result,
                )

            except Exception as e:
                logger.error(f"Errore in visura intestati {request.request_id}: {e}")
                return VisuraResponse(
                    request_id=request.request_id,
                    success=False,
                    tipo_catasto=request.tipo_catasto,
                    error=str(e),
                )

    async def esegui_visura_persona_fisica(self, request: VisuraPersonaFisicaRequest) -> VisuraResponse:
        """Ricerca beni per persona fisica (codice fiscale o anagrafica)."""
        pf_label = "PF"
        async with self._session_lock:
            try:
                await self._ensure_authenticated()

                result = await run_visura_persona_fisica(
                    self.auth_page,
                    provincia=request.provincia,
                    pf_tipo_catasto=request.pf_tipo_catasto,
                    pf_comune_catastale=request.pf_comune_catastale,
                    pf_search_by=request.pf_search_by or "cognome",
                    pf_cognome=request.pf_cognome,
                    pf_nome=request.pf_nome,
                    pf_codice_fiscale=request.pf_codice_fiscale,
                    pf_birth_day=request.pf_birth_day,
                    pf_birth_month=request.pf_birth_month,
                    pf_birth_year=request.pf_birth_year,
                    pf_sesso=request.pf_sesso,
                    pf_birth_province=request.pf_birth_province,
                    pf_tipo_ispezione=request.pf_tipo_ispezione or "R",
                    pf_limitata=request.pf_limitata,
                    tipo_richiesta=request.tipo_richiesta or "A",
                    richiedente=request.richiedente,
                    motivo=request.motivo,
                )

                logger.info(f"Visura PF completata per {request.request_id}")
                return VisuraResponse(
                    request_id=request.request_id,
                    success=True,
                    tipo_catasto=pf_label,
                    data=result,
                )

            except (AuthenticationError, BrowserError) as e:
                logger.error(f"Errore in visura PF {request.request_id}: {e}")
                return VisuraResponse(
                    request_id=request.request_id,
                    success=False,
                    tipo_catasto=pf_label,
                    error=str(e),
                )
            except Exception as e:
                logger.error(f"Errore inatteso in visura PF {request.request_id}: {e}")
                return VisuraResponse(
                    request_id=request.request_id,
                    success=False,
                    tipo_catasto=pf_label,
                    error=str(e),
                )

    async def restart_browser_if_needed(self):
        """Riavvia il browser se necessario"""
        try:
            if self.browser and not self.browser.is_connected():
                logger.info("Browser disconnesso, riavviando...")
                await self.close()
                await self.initialize()
                await self.login()
                await self.start_keep_alive()
                logger.info("Browser riavviato con successo")
        except Exception as e:
            logger.error(f"Errore nel riavvio browser: {e}")
            raise BrowserError(f"Failed to restart browser: {e}") from e

    async def close(self):
        """Chiude il browser e torna sempre al portale"""
        await self.stop_keep_alive()
        try:
            if self.auth_page and not self.auth_page.is_closed():
                try:
                    await self.auth_page.get_by_role("link", name=" Torna al portale").click()
                except Exception as e:
                    logger.warning(f"Impossibile cliccare 'Torna al portale': {e}")
        except Exception as e:
            logger.warning(f"Errore durante il tentativo di tornare al portale: {e}")
        try:
            if self.context:
                await self.context.close()
        except Exception as e:
            logger.warning(f"Errore durante la chiusura del context: {e}")
        try:
            if self.browser:
                await self.browser.close()
        except Exception as e:
            logger.warning(f"Errore durante la chiusura del browser: {e}")
        try:
            if self.playwright is not None:
                await self.playwright.stop()
                self.playwright = None
        except Exception as e:
            logger.warning(f"Errore durante lo stop di playwright: {e}")
        logger.info("Browser chiuso")

    async def graceful_shutdown(self):
        """Effettua uno shutdown graceful con logout"""
        logger.info("Iniziando shutdown graceful...")

        try:
            if self.auth_page and not self.auth_page.is_closed():
                logger.info("Effettuando logout dalla sessione...")
                await logout(self.auth_page)
        except Exception as e:
            logger.warning(f"Errore durante il logout: {e}")

        await self.close()
        logger.info("Shutdown graceful completato")


class VisuraService:
    def __init__(self):
        self.browser_manager = BrowserManager()
        self.request_queue = asyncio.Queue()
        self.response_store: Dict[str, VisuraResponse] = {}
        self.processing = False

    async def initialize(self):
        """Inizializza il servizio"""
        await self.browser_manager.initialize()
        await self.browser_manager.login()
        await self.browser_manager.start_keep_alive()

        # Avvia il worker per processare le richieste
        asyncio.create_task(self._process_requests())

    async def _process_requests(self):
        """Processa le richieste in coda"""
        self.processing = True

        while self.processing:
            try:
                request_data = await self.request_queue.get()
                request = request_data["request"]

                if isinstance(request, VisuraRequest):
                    response = await self.browser_manager.esegui_visura(request)
                    self.response_store[request.request_id] = response
                    logger.info(f"Processata richiesta visura {request.request_id}")

                elif isinstance(request, VisuraIntestatiRequest):
                    response = await self.browser_manager.esegui_visura_intestati(request)
                    self.response_store[request.request_id] = response
                    logger.info(f"Processata richiesta intestati {request.request_id}")

                elif isinstance(request, VisuraPersonaFisicaRequest):
                    response = await self.browser_manager.esegui_visura_persona_fisica(request)
                    self.response_store[request.request_id] = response
                    logger.info(f"Processata richiesta visura PF {request.request_id}")

                else:
                    logger.error(f"Tipo di richiesta sconosciuto: {type(request)}")

                self.request_queue.task_done()

                # Pausa tra le richieste per non sovraccaricare SISTER
                await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"Errore nel processare richieste: {e}")
                await asyncio.sleep(5)

    async def add_request(self, request: VisuraRequest) -> str:
        """Aggiunge una richiesta alla coda"""
        await self.request_queue.put({"request": request})
        logger.info(
            f"Richiesta visura {request.request_id} aggiunta alla coda (posizione: {self.request_queue.qsize()})"
        )
        return request.request_id

    async def add_intestati_request(self, request: VisuraIntestatiRequest) -> str:
        """Aggiunge una richiesta intestati alla coda"""
        await self.request_queue.put({"request": request})
        logger.info(
            f"Richiesta intestati {request.request_id} aggiunta alla coda (posizione: {self.request_queue.qsize()})"
        )
        return request.request_id

    async def add_persona_fisica_request(self, request: VisuraPersonaFisicaRequest) -> str:
        """Aggiunge una richiesta visura persona fisica alla coda"""
        await self.request_queue.put({"request": request})
        logger.info(
            f"Richiesta visura PF {request.request_id} aggiunta alla coda (posizione: {self.request_queue.qsize()})"
        )
        return request.request_id

    async def get_response(self, request_id: str) -> Optional[VisuraResponse]:
        """Ottiene la risposta per un request_id"""
        return self.response_store.get(request_id)

    async def shutdown(self):
        """Chiude il servizio"""
        self.processing = False
        await self.browser_manager.close()

    async def graceful_shutdown(self):
        """Chiude il servizio con logout graceful"""
        logger.info("Iniziando graceful shutdown del servizio...")
        self.processing = False
        await self.browser_manager.graceful_shutdown()
        logger.info("Graceful shutdown del servizio completato")

    async def session_logout(self) -> Dict[str, Any]:
        """Logout esplicito sul portale (senza chiudere il processo)."""
        return await self.browser_manager.session_logout()

    async def session_login(self) -> Dict[str, Any]:
        """Login esplicito (riusa LOGIN_METHOD da ambiente)."""
        return await self.browser_manager.session_login()


# Global service instance - initialized during lifespan
visura_service: Optional[VisuraService] = None


def get_visura_service() -> VisuraService:
    """Dependency to get the visura service"""
    if visura_service is None:
        raise HTTPException(status_code=503, detail="Servizio non inizializzato")
    return visura_service


# Signal handler per shutdown graceful
# Nota: NON usiamo signal handler custom perché sys.exit() uccide il processo
# prima che il logout async possa completare. Uvicorn gestisce già SIGINT/SIGTERM
# e passa per il lifespan shutdown dove il logout viene eseguito correttamente.


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global visura_service
    PageLogger.reset_session()  # Nuova sessione di log per ogni avvio
    visura_service = VisuraService()
    await visura_service.initialize()
    logger.info("Servizio visure avviato")
    yield
    # Shutdown — uvicorn arriva qui dopo SIGINT/SIGTERM
    logger.info("Shutdown in corso, eseguendo logout...")
    if visura_service:
        await visura_service.graceful_shutdown()
    logger.info("Servizio visure fermato con graceful shutdown")


# API FastAPI
app = FastAPI(title="Servizio Visure Catastali", lifespan=lifespan)

# ---------------------------------------------------------------------------
# Modelli di richiesta
# ---------------------------------------------------------------------------


class VisuraInput(BaseModel):
    """Richiesta per una visura catastale (solo dati catastali, senza intestati)"""

    provincia: str = Field(..., min_length=1, description="Nome della provincia")
    comune: str = Field(..., min_length=1, description="Nome del comune")
    foglio: str = Field(..., min_length=1, description="Numero di foglio")
    particella: str = Field(..., min_length=1, description="Numero di particella")
    sezione: Optional[str] = Field(None, description="Sezione (opzionale)")
    subalterno: Optional[str] = Field(None, description="Subalterno (opzionale, restringe la ricerca per fabbricati)")
    tipo_catasto: Optional[str] = Field(
        None, pattern=r"^[TF]$", description="'T' = Terreni, 'F' = Fabbricati (se omesso esegue entrambi)"
    )

    @validator("tipo_catasto")
    def validate_tipo_catasto(cls, v):
        if v is not None and v not in ["T", "F"]:
            raise ValidationError(f"tipo_catasto deve essere 'T' o 'F', ricevuto {v}")
        return v


class VisuraIntestatiInput(BaseModel):
    """Richiesta per ottenere gli intestati di un immobile specifico"""

    provincia: str = Field(..., min_length=1, description="Nome della provincia")
    comune: str = Field(..., min_length=1, description="Nome del comune")
    foglio: str = Field(..., min_length=1, description="Numero di foglio")
    particella: str = Field(..., min_length=1, description="Numero di particella")
    tipo_catasto: str = Field(..., pattern=r"^[TF]$", description="'T' = Terreni, 'F' = Fabbricati")
    subalterno: Optional[str] = Field(None, description="Numero di subalterno (obbligatorio per Fabbricati)")
    sezione: Optional[str] = Field(None, description="Sezione (opzionale)")

    @validator("tipo_catasto")
    def validate_tipo_catasto(cls, v):
        if v not in ["T", "F"]:
            raise ValidationError(f"tipo_catasto deve essere 'T' o 'F', ricevuto {v}")
        return v

    @validator("subalterno")
    def validate_subalterno(cls, v, values):
        tipo_catasto = values.get("tipo_catasto")
        if tipo_catasto == "F" and not v:
            raise ValidationError("subalterno è obbligatorio per i fabbricati (tipo_catasto='F')")
        if tipo_catasto == "T" and v:
            raise ValidationError("subalterno non va indicato per i terreni (tipo_catasto='T')")
        return v


class SezioniExtractionRequest(BaseModel):
    """Richiesta per l'estrazione delle sezioni territoriali"""

    tipo_catasto: str = Field("T", pattern=r"^[TF]$", description="'T' = Terreni, 'F' = Fabbricati")
    max_province: int = Field(
        200, ge=1, le=200, description="Numero massimo di province da processare (default: tutte)"
    )


class VisuraPersonaFisicaInput(BaseModel):
    """Ricerca beni catastali per persona fisica (codice fiscale o anagrafica)."""

    provincia: str = Field(..., min_length=1, description="Provincia dell'ufficio provinciale (ricerca)")
    comune: str = Field(
        ...,
        min_length=1,
        description="Comune (richiesto dal client; in PF può coincidere con comune catastale)",
    )
    pf_tipo_catasto: Optional[str] = Field(
        None, pattern=r"^[ETF]$", description="'E' entrambi, 'T' terreni, 'F' fabbricati"
    )
    pf_comune_catastale: Optional[str] = None
    pf_search_by: Optional[str] = Field("cognome", pattern=r"^(cognome|cf)$")
    pf_cognome: Optional[str] = None
    pf_nome: Optional[str] = None
    pf_codice_fiscale: Optional[str] = None
    pf_birth_day: Optional[str] = None
    pf_birth_month: Optional[str] = None
    pf_birth_year: Optional[str] = None
    pf_sesso: Optional[str] = Field(None, pattern=r"^[MF]$")
    pf_birth_province: Optional[str] = None
    pf_tipo_ispezione: Optional[str] = Field("R", pattern=r"^[RAL]$")
    pf_limitata: Optional[str] = Field(None, pattern=r"^[012]$")
    tipo_richiesta: Optional[str] = Field("A", pattern=r"^[AS]$")
    richiedente: Optional[str] = None
    motivo: Optional[str] = None


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@app.post("/visura")
async def richiedi_visura(request: VisuraInput, service: VisuraService = Depends(get_visura_service)):
    """Richiede una visura catastale fornendo direttamente i dati catastali"""
    try:
        sezione = None if request.sezione == "_" else request.sezione

        tipos_catasto = [request.tipo_catasto] if request.tipo_catasto else ["T", "F"]
        request_ids = []

        for tipo_catasto in tipos_catasto:
            request_id = f"req_{tipo_catasto}_{int(time.time() * 1000)}"
            visura_req = VisuraRequest(
                request_id=request_id,
                tipo_catasto=tipo_catasto,
                provincia=request.provincia,
                comune=request.comune,
                sezione=sezione,
                foglio=request.foglio,
                particella=request.particella,
                subalterno=request.subalterno,
            )
            await service.add_request(visura_req)
            request_ids.append(request_id)

        return JSONResponse(
            {
                "request_ids": request_ids,
                "tipos_catasto": tipos_catasto,
                "status": "queued",
                "message": f"Richieste aggiunte alla coda per {request.comune} F.{request.foglio} P.{request.particella}",
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nella richiesta visura: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/visura/{request_id}")
async def ottieni_visura(request_id: str, service: VisuraService = Depends(get_visura_service)):
    """Ottiene il risultato di una visura"""
    try:
        response = await service.get_response(request_id)

        if response is None:
            return JSONResponse(
                {"request_id": request_id, "status": "processing", "message": "Richiesta in elaborazione"}
            )

        return JSONResponse(
            {
                "request_id": request_id,
                "tipo_catasto": response.tipo_catasto,
                "status": "completed" if response.success else "error",
                "data": response.data,
                "error": response.error,
                "timestamp": response.timestamp.isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Errore nell'ottenere visura: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/visura/persona-fisica")
async def richiedi_visura_persona_fisica(
    request: VisuraPersonaFisicaInput, service: VisuraService = Depends(get_visura_service)
):
    """Accoda una ricerca visura per persona fisica (beni catastali)."""
    try:
        request_id = f"req_pf_{int(time.time() * 1000)}"
        pf_req = VisuraPersonaFisicaRequest(
            request_id=request_id,
            provincia=request.provincia,
            comune=request.comune,
            pf_tipo_catasto=request.pf_tipo_catasto,
            pf_comune_catastale=request.pf_comune_catastale,
            pf_search_by=request.pf_search_by or "cognome",
            pf_cognome=request.pf_cognome,
            pf_nome=request.pf_nome,
            pf_codice_fiscale=request.pf_codice_fiscale,
            pf_birth_day=request.pf_birth_day,
            pf_birth_month=request.pf_birth_month,
            pf_birth_year=request.pf_birth_year,
            pf_sesso=request.pf_sesso,
            pf_birth_province=request.pf_birth_province,
            pf_tipo_ispezione=request.pf_tipo_ispezione or "R",
            pf_limitata=request.pf_limitata,
            tipo_richiesta=request.tipo_richiesta or "A",
            richiedente=request.richiedente,
            motivo=request.motivo,
        )
        await service.add_persona_fisica_request(pf_req)

        return JSONResponse(
            {
                "request_ids": [request_id],
                "tipos_catasto": ["PF"],
                "status": "queued",
                "message": f"Richiesta visura persona fisica accodata ({request_id})",
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nella richiesta visura PF: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/visura/intestati")
async def richiedi_intestati_immobile(
    request: VisuraIntestatiInput, service: VisuraService = Depends(get_visura_service)
):
    """Richiede gli intestati per un immobile specifico."""
    try:
        sezione = None if request.sezione == "_" else request.sezione

        request_id = f"intestati_{request.tipo_catasto}_{request.subalterno or 'none'}_{int(time.time() * 1000)}"

        intestati_request = VisuraIntestatiRequest(
            request_id=request_id,
            tipo_catasto=request.tipo_catasto,
            provincia=request.provincia,
            comune=request.comune,
            foglio=request.foglio,
            particella=request.particella,
            subalterno=request.subalterno,
            sezione=sezione,
        )

        await service.add_intestati_request(intestati_request)

        return JSONResponse(
            {
                "request_id": request_id,
                "tipo_catasto": request.tipo_catasto,
                "subalterno": request.subalterno,
                "status": "queued",
                "message": f"Richiesta intestati aggiunta alla coda per {request.comune} F.{request.foglio} P.{request.particella}",
                "queue_position": service.request_queue.qsize(),
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore nella richiesta intestati: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def verify_visura_session_secret(
    x_visura_session_secret: Optional[str] = Header(None, alias="X-Visura-Session-Secret"),
):
    """Se VISURA_SESSION_SECRET è impostato, richiede lo stesso valore nell'header."""
    secret = os.getenv("VISURA_SESSION_SECRET", "").strip()
    if not secret:
        return True
    if not x_visura_session_secret or x_visura_session_secret.strip() != secret:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Visura-Session-Secret")
    return True


@app.post("/session/login")
async def session_login_endpoint(
    service: VisuraService = Depends(get_visura_service),
    _: bool = Depends(verify_visura_session_secret),
):
    """Esegue login esplicito (stesso flusso dell'avvio: utile per 'Connetti' da un client)."""
    try:
        data = await service.session_login()
        return JSONResponse(
            {
                **data,
                "authenticated": service.browser_manager.authenticated,
            }
        )
    except AuthenticationError as e:
        logger.error(f"session/login fallito: {e}")
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        logger.error(f"Errore session/login: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/session/logout")
async def session_logout_endpoint(
    service: VisuraService = Depends(get_visura_service),
    _: bool = Depends(verify_visura_session_secret),
):
    """Logout sul portale ADE/SISTER senza spegnere il processo (preferibile a /shutdown)."""
    try:
        data = await service.session_logout()
        return JSONResponse(data)
    except Exception as e:
        logger.error(f"Errore session/logout: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check(service: VisuraService = Depends(get_visura_service)):
    """Controlla lo stato del servizio"""
    return JSONResponse(
        {
            "status": "healthy",
            "authenticated": service.browser_manager.authenticated,
            "queue_size": service.request_queue.qsize(),
        }
    )


@app.post("/shutdown")
async def graceful_shutdown_endpoint(service: VisuraService = Depends(get_visura_service)):
    """Effettua uno shutdown graceful del servizio"""
    try:
        logger.info("Shutdown graceful richiesto via API")
        await service.graceful_shutdown()
        return JSONResponse({"status": "success", "message": "Shutdown graceful completato"})
    except Exception as e:
        logger.error(f"Errore durante shutdown graceful via API: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sezioni/extract")
async def extract_sezioni(request: SezioniExtractionRequest, service: VisuraService = Depends(get_visura_service)):
    """
    Estrae le sezioni territoriali d'Italia per il tipo catasto specificato.
    ATTENZIONE: Questa operazione può richiedere diverse ore!
    I dati vengono restituiti nella risposta.
    """
    try:
        logger.info(
            f"Iniziando estrazione sezioni per tipo catasto: {request.tipo_catasto}, max province: {request.max_province}"
        )

        if not service.browser_manager.authenticated or not service.browser_manager.auth_page:
            raise HTTPException(status_code=503, detail="Servizio non autenticato")

        sezioni_data = await extract_all_sezioni(
            service.browser_manager.auth_page, request.tipo_catasto, request.max_province
        )

        if not sezioni_data:
            return JSONResponse({"status": "no_data", "message": "Nessuna sezione estratta", "count": 0})

        logger.info(f"Estrazione sezioni completata: {len(sezioni_data)} totali")

        return JSONResponse(
            {
                "status": "success",
                "message": f"Estrazione completata per tipo catasto {request.tipo_catasto}",
                "total_extracted": len(sezioni_data),
                "tipo_catasto": request.tipo_catasto,
                "sezioni": sezioni_data,
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Errore durante estrazione sezioni: {e}")
        raise HTTPException(status_code=500, detail=str(e))
