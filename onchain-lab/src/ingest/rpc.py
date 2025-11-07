from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx
from tenacity import RetryError, retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class RPCError(RuntimeError):
    """Raised when an RPC request fails."""


class BitcoinRPCClient:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        *,
        timeout: float = 30.0,
        max_attempts: int = 5,
    ) -> None:
        self._endpoint = f"http://{host}:{port}"
        self._auth = (user, password)
        self._client = httpx.Client(timeout=timeout)
        self._max_attempts = max_attempts

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "BitcoinRPCClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def get_block_hash(self, height: int) -> str:
        return self._call("getblockhash", [height])

    def get_block(self, block_hash: str, verbosity: int = 2) -> Dict[str, Any]:
        block = self._call("getblock", [block_hash, verbosity])
        if "time" in block:
            block["time"] = self._to_utc(block["time"])
        if "tx" in block:
            for tx in block["tx"]:
                if "time" in tx:
                    tx["time"] = self._to_utc(tx["time"])
        return block

    def get_block_count(self) -> int:
        return self._call("getblockcount", [])

    def get_raw_transaction(self, txid: str, verbose: bool = True) -> Dict[str, Any]:
        tx = self._call("getrawtransaction", [txid, int(verbose)])
        if "time" in tx:
            tx["time"] = self._to_utc(tx["time"])
        return tx

    def _to_utc(self, epoch_seconds: Any) -> datetime:
        if not isinstance(epoch_seconds, (int, float)):
            raise RPCError(f"Expected epoch seconds, received {epoch_seconds!r}")
        return datetime.fromtimestamp(int(epoch_seconds), tz=timezone.utc)

    def _call(self, method: str, params: Optional[list[Any]] = None) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": "onchain-ingest",
            "method": method,
            "params": params or [],
        }

        @retry(
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_exponential(multiplier=0.5, min=1, max=10),
            retry=retry_if_exception_type(httpx.HTTPError),
            reraise=True,
        )
        def _do_call() -> Any:
            response = self._client.post(self._endpoint, json=payload, auth=self._auth)
            response.raise_for_status()
            data = response.json()
            if "error" in data and data["error"]:
                raise RPCError(f"RPC error calling {method}: {json.dumps(data['error'])}")
            return data["result"]

        try:
            return _do_call()
        except RetryError as exc:
            raise RPCError(f"RPC call failed for {method}: {exc}") from exc
