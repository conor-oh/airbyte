# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

import certifi
from connectors_blue_green import logger
from mitmproxy import http
from mitmproxy.addons import default_addons, script
from mitmproxy.master import Master
from mitmproxy.options import Options


class MitmProxy:
    proxy_host = "0.0.0.0"

    def __init__(self, proxy_port: int, har_dump_path: str, replay_session_path: str | None = None) -> None:
        self.proxy_port = proxy_port
        self.har_dump_path = har_dump_path
        self.proxy_process = None
        self.session_path = tempfile.NamedTemporaryFile(delete=False).name
        self.replay_session_path = replay_session_path

    @property
    def mitm_command(self):
        default_command = [
            "mitmdump",
            f"--listen-host={self.proxy_host}",
            f"--listen-port={self.proxy_port}",
            "--set",
            f"hardump={self.har_dump_path}",
            "--save-stream-file",
            self.session_path,
        ]
        if self.replay_session_path is not None:
            return default_command + ["--server-replay", self.replay_session_path]
        return default_command

    def _start_mitmdump(self):
        return subprocess.Popen(
            self.mitm_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def __enter__(self) -> MitmProxy:
        self.proxy_process = self._start_mitmdump()
        self._set_proxy_environment()
        logger.info(f"Started proxy on port {self.proxy_port}")
        return self

    def __exit__(self, *_) -> None:
        self.proxy_process.terminate()

    def _set_proxy_environment(self):
        extra_ca_certificates_dir = Path("/usr/local/share/ca-certificates/extra")
        extra_ca_certificates_dir.mkdir(exist_ok=True)
        self_signed_cert_path = extra_ca_certificates_dir / "mitm.crt"
        if not self_signed_cert_path.exists():
            mitmproxy_dir = Path.home() / ".mitmproxy"
            mitmproxy_pem_path = mitmproxy_dir / "mitmproxy-ca.pem"
            while not mitmproxy_pem_path.exists():
                time.sleep(0.1)
            subprocess.run(
                ["openssl", "x509", "-in", str(mitmproxy_pem_path), "-out", str(extra_ca_certificates_dir / "mitm.crt")],
                check=True,
                capture_output=True,
            )
            subprocess.run(["update-ca-certificates", "--fresh"], check=True, capture_output=True)
        os.environ["HTTP_PROXY"] = f"{self.proxy_host}:{self.proxy_port}"
        os.environ["HTTPS_PROXY"] = f"{self.proxy_host}:{self.proxy_port}"
