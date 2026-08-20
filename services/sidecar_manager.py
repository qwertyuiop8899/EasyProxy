"""Lifecycle management for the embedded Toastflix audio sidecar."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import sys
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)


class SidecarManager:
    """Start, health-check, and stop the FastAPI sidecar process.

    The sidecar is deliberately bound to loopback.  EasyProxy is the only
    public entry point and forwards requests to the child process through the
    :class:`HLSProxySidecarMixin`.
    """

    def __init__(
        self,
        cache_dir: str | os.PathLike[str] | None = None,
        sidecar_dir: str | os.PathLike[str] | None = None,
    ):
        project_dir = Path(__file__).resolve().parent.parent
        self.sidecar_dir = Path(
            sidecar_dir
            or os.environ.get("SIDECAR_DIR", project_dir / "toastflix-sidecar-main")
        ).resolve()
        self.cache_dir = Path(
            cache_dir
            or os.environ.get("SIDECAR_CACHE_DIR", project_dir / "recordings" / "sidecar_data")
        ).resolve()

        self.host = os.environ.get("SIDECAR_HOST", "127.0.0.1").strip() or "127.0.0.1"
        self.configured_port = self._parse_port(
            os.environ.get("SIDECAR_INTERNAL_PORT")
            or os.environ.get("SIDECAR_PORT")
            or "0"
        )
        self.startup_timeout = max(
            1.0, float(os.environ.get("SIDECAR_STARTUP_TIMEOUT", "20"))
        )
        self.forwarded_allow_ips = (
            os.environ.get("SIDECAR_FORWARDED_ALLOW_IPS", "127.0.0.1").strip()
            or "127.0.0.1"
        )

        self.process: asyncio.subprocess.Process | None = None
        self.port: int | None = None
        self._log_task: asyncio.Task | None = None
        self._stopping = False

    @staticmethod
    def _parse_port(value: str) -> int:
        try:
            port = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid SIDECAR_INTERNAL_PORT/SIDECAR_PORT: {value!r}") from exc
        if not 0 <= port <= 65535:
            raise ValueError(f"Invalid sidecar port: {port}")
        return port

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.returncode is None and self.port is not None

    @property
    def base_url(self) -> str:
        if self.port is None:
            raise RuntimeError("Sidecar is not running")
        return f"http://{self.host}:{self.port}"

    def target_url(self, path: str, query_string: str = "") -> str:
        """Build an internal URL while keeping the original query encoding."""
        if not path.startswith("/"):
            path = f"/{path}"
        target = f"{self.base_url}{path}"
        return f"{target}?{query_string}" if query_string else target

    async def start(self) -> None:
        """Launch the sidecar and wait until its health endpoint responds."""
        if self.running:
            return
        if self.process is not None:
            await self.stop()
        if not self.sidecar_dir.is_dir() or not (self.sidecar_dir / "app.py").is_file():
            raise FileNotFoundError(f"Toastflix sidecar directory not found: {self.sidecar_dir}")

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.port = self.configured_port or self._find_free_port()
        child_env = os.environ.copy()
        child_env["SIDECAR_CACHE_DIR"] = str(self.cache_dir)

        command = [
            sys.executable,
            "-m",
            "uvicorn",
            "app:app",
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--proxy-headers",
            "--forwarded-allow-ips",
            self.forwarded_allow_ips,
        ]
        logger.info(
            "Starting Toastflix sidecar on %s:%s (cache: %s)",
            self.host,
            self.port,
            self.cache_dir,
        )
        try:
            self.process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(self.sidecar_dir),
                env=child_env,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            self._log_task = asyncio.create_task(self._read_output())
            await self._wait_until_ready()
        except Exception:
            await self.stop()
            raise

        logger.info("Toastflix sidecar is ready at %s", self.base_url)

    async def stop(self) -> None:
        """Stop the child process and its output reader without leaving orphans."""
        if self._stopping:
            return
        self._stopping = True
        try:
            process = self.process
            if process is not None and process.returncode is None:
                logger.info("Stopping Toastflix sidecar (pid=%s)", process.pid)
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    logger.warning("Toastflix sidecar did not stop cleanly; killing it")
                    process.kill()
                    await process.wait()

            if self._log_task is not None:
                self._log_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._log_task
        finally:
            self._log_task = None
            self.process = None
            self.port = None
            self._stopping = False

    def _find_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, 0))
            return int(sock.getsockname()[1])

    async def _wait_until_ready(self) -> None:
        deadline = asyncio.get_running_loop().time() + self.startup_timeout
        timeout = aiohttp.ClientTimeout(total=2)
        health_url = f"{self.base_url}/health"
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while asyncio.get_running_loop().time() < deadline:
                if self.process is None or self.process.returncode is not None:
                    code = None if self.process is None else self.process.returncode
                    raise RuntimeError(f"Toastflix sidecar exited before readiness (code={code})")
                try:
                    async with session.get(health_url) as response:
                        if response.status == 200:
                            return
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass
                await asyncio.sleep(0.1)
        raise TimeoutError(f"Toastflix sidecar did not become ready within {self.startup_timeout:g}s")

    async def _read_output(self) -> None:
        process = self.process
        if process is None or process.stdout is None:
            return
        try:
            async for raw_line in process.stdout:
                line = raw_line.decode(errors="replace").rstrip()
                if line:
                    logger.info("[sidecar] %s", line)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Unable to read Toastflix sidecar output: %s", exc)


__all__ = ["SidecarManager"]
