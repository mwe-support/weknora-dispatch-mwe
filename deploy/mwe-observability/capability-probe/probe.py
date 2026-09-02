#!/usr/bin/env python3
"""Small Docker capability/log probe with a Prometheus endpoint."""

from __future__ import annotations

import json
import csv
import os
import re
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

# ponytail: v1 keeps only known blocking signatures here; move them to a
# mounted policy file when operators need to tune rules without rebuilding.
BLOCKING_SIGNATURES = {
    "cuda_unavailable": re.compile(
        r"(?i)(cuda[^\n]*(unavailable|not available|initiali[sz](e|ation) failed)|"
        r"no cuda gpus|can't initialize nvml|failed to initialize nvml|"
        r"fall(?:ing)? back to cpu)"
    ),
    "cuda_oom": re.compile(r"(?i)(cuda out of memory|outofmemoryerror|cublas.*alloc|gpu.*oom)"),
    "fatal_runtime": re.compile(r"(?i)(^|\b)(fatal|panic|segmentation fault|oom-kill)(\b|:)", re.MULTILINE),
}


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        return default
    return min(max(value, minimum), maximum)


def container_names(raw: str) -> list[str]:
    names = [item.strip() for item in raw.split(",") if item.strip()]
    if not names or any(not NAME_RE.fullmatch(item) for item in names):
        raise ValueError("EXPECTED_CONTAINERS contains an invalid container name")
    return list(dict.fromkeys(names))


def gpu_sources(raw: str) -> list[tuple[str, str]]:
    sources: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in raw.split(","):
        if not item.strip():
            continue
        gpu, separator, container = item.strip().partition(":")
        if separator != ":" or not gpu.isdigit() or not NAME_RE.fullmatch(container):
            raise ValueError("GPU_SOURCES must use physical_index:container entries")
        if gpu in seen:
            raise ValueError("GPU_SOURCES contains a duplicate physical index")
        seen.add(gpu)
        sources.append((gpu, container))
    if not sources:
        raise ValueError("GPU_SOURCES is empty")
    return sources


def numeric(value: str, multiplier: int = 1) -> str:
    try:
        return str(float(value.strip()) * multiplier)
    except ValueError:
        return "NaN"


def run(command: list[str], timeout: int = 20) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)


def escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


class Probe:
    def __init__(self) -> None:
        self.host = os.getenv("MONITORED_HOST", "marvel-kb")
        self.names = container_names(os.environ["EXPECTED_CONTAINERS"])
        self.mineru = os.getenv("MINERU_CONTAINER", "kb-mineru-api")
        self.mineru_python = os.getenv("MINERU_PYTHON", "python3")
        self.gpus = gpu_sources(os.getenv("GPU_SOURCES", "0:kb-q4-gpu0,1:kb-q4-gpu1"))
        if not NAME_RE.fullmatch(self.mineru) or not NAME_RE.fullmatch(self.mineru_python):
            raise ValueError("invalid MinerU probe command configuration")
        self.interval = env_int("PROBE_INTERVAL_SECONDS", 30, 10, 300)
        self.log_window = env_int("LOG_WINDOW_SECONDS", 120, 30, 900)
        self._metrics = ""
        self._lock = threading.Lock()

    def collect(self) -> str:
        host = escape_label(self.host)
        lines = [
            "# HELP kb_probe_last_success_timestamp_seconds Last completed capability probe.",
            "# TYPE kb_probe_last_success_timestamp_seconds gauge",
            "# HELP kb_container_running Whether an expected container is running.",
            "# TYPE kb_container_running gauge",
            "# HELP kb_container_health_status Docker health state as a one-hot gauge.",
            "# TYPE kb_container_health_status gauge",
            "# HELP kb_container_restart_count Docker restart count.",
            "# TYPE kb_container_restart_count gauge",
            "# HELP kb_container_oom_killed Whether Docker reports OOMKilled.",
            "# TYPE kb_container_oom_killed gauge",
            "# HELP kb_component_capability_ready Whether a component-specific capability probe passed.",
            "# TYPE kb_component_capability_ready gauge",
            "# HELP kb_component_blocking_log Whether a blocking signature exists in the recent log window.",
            "# TYPE kb_component_blocking_log gauge",
            "# HELP kb_gpu_probe_ready Whether nvidia-smi succeeded for the configured physical GPU.",
            "# TYPE kb_gpu_probe_ready gauge",
            "# HELP kb_gpu_utilization_percent Current GPU compute utilization.",
            "# TYPE kb_gpu_utilization_percent gauge",
            "# HELP kb_gpu_memory_used_bytes Current GPU framebuffer memory usage.",
            "# TYPE kb_gpu_memory_used_bytes gauge",
            "# HELP kb_gpu_memory_total_bytes Total GPU framebuffer memory.",
            "# TYPE kb_gpu_memory_total_bytes gauge",
            "# HELP kb_gpu_temperature_celsius Current GPU temperature.",
            "# TYPE kb_gpu_temperature_celsius gauge",
            "# HELP kb_gpu_power_watts Current GPU board power draw.",
            "# TYPE kb_gpu_power_watts gauge",
            "# HELP kb_gpu_power_limit_watts Configured GPU board power limit.",
            "# TYPE kb_gpu_power_limit_watts gauge",
        ]
        for name in self.names:
            label = escape_label(name)
            inspected = run(["docker", "inspect", name])
            running = 0
            health = "missing"
            restarts = 0
            oom = 0
            if inspected.returncode == 0:
                try:
                    state = json.loads(inspected.stdout)[0]["State"]
                    running = int(bool(state.get("Running")))
                    health = state.get("Health", {}).get("Status", "none")
                    restarts = int(state.get("RestartCount", 0))
                    oom = int(bool(state.get("OOMKilled")))
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    health = "invalid"
            lines.append(f'kb_container_running{{host="{host}",container="{label}"}} {running}')
            lines.append(
                f'kb_container_health_status{{host="{host}",container="{label}",status="{escape_label(health)}"}} 1'
            )
            lines.append(f'kb_container_restart_count{{host="{host}",container="{label}"}} {restarts}')
            lines.append(f'kb_container_oom_killed{{host="{host}",container="{label}"}} {oom}')

            logs = run(["docker", "logs", "--since", f"{self.log_window}s", "--tail", "2000", name])
            recent = (logs.stdout + "\n" + logs.stderr) if logs.returncode == 0 else ""
            for code, pattern in BLOCKING_SIGNATURES.items():
                value = int(bool(pattern.search(recent)))
                lines.append(
                    f'kb_component_blocking_log{{host="{host}",component="{label}",code="{code}"}} {value}'
                )

        for gpu, container in self.gpus:
            sampled = run(
                [
                    "docker",
                    "exec",
                    container,
                    "nvidia-smi",
                    "--query-gpu=uuid,name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,power.limit",
                    "--format=csv,noheader,nounits",
                ],
                timeout=15,
            )
            rows = list(csv.reader(sampled.stdout.splitlines(), skipinitialspace=True))
            ready = int(sampled.returncode == 0 and len(rows) == 1 and len(rows[0]) == 8)
            base = f'host="{host}",gpu="{gpu}",container="{escape_label(container)}"'
            lines.append(f"kb_gpu_probe_ready{{{base}}} {ready}")
            if not ready:
                continue
            uuid, name, utilization, memory_used, memory_total, temperature, power, power_limit = rows[0]
            labels = base + f',uuid="{escape_label(uuid)}",name="{escape_label(name)}"'
            lines.append(f"kb_gpu_utilization_percent{{{labels}}} {numeric(utilization)}")
            lines.append(f"kb_gpu_memory_used_bytes{{{labels}}} {numeric(memory_used, 1024 * 1024)}")
            lines.append(f"kb_gpu_memory_total_bytes{{{labels}}} {numeric(memory_total, 1024 * 1024)}")
            lines.append(f"kb_gpu_temperature_celsius{{{labels}}} {numeric(temperature)}")
            lines.append(f"kb_gpu_power_watts{{{labels}}} {numeric(power)}")
            lines.append(f"kb_gpu_power_limit_watts{{{labels}}} {numeric(power_limit)}")

        cuda = run(
            [
                "docker",
                "exec",
                self.mineru,
                self.mineru_python,
                "-c",
                "import torch,sys;sys.exit(0 if torch.cuda.is_available() and torch.cuda.device_count()>0 else 1)",
            ],
            timeout=30,
        )
        lines.append(
            f'kb_component_capability_ready{{host="{host}",component="mineru_cuda"}} {int(cuda.returncode == 0)}'
        )
        lines.append(f'kb_probe_last_success_timestamp_seconds{{host="{host}"}} {int(time.time())}')
        return "\n".join(lines) + "\n"

    def loop(self) -> None:
        while True:
            started = time.monotonic()
            try:
                value = self.collect()
            except Exception as exc:  # keep the endpoint alive and visibly stale
                print(f"probe collection failed: {type(exc).__name__}", flush=True)
            else:
                with self._lock:
                    self._metrics = value
            time.sleep(max(1, self.interval - (time.monotonic() - started)))

    def metrics(self) -> str:
        with self._lock:
            return self._metrics


class Handler(BaseHTTPRequestHandler):
    probe: Probe

    def do_GET(self) -> None:  # noqa: N802
        if self.path not in ("/metrics", "/healthz"):
            self.send_error(404)
            return
        body = (self.probe.metrics() if self.path == "/metrics" else "ok\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def self_test() -> None:
    assert container_names("a,b,a") == ["a", "b"]
    assert gpu_sources("0:gpu-a,1:gpu-b") == [("0", "gpu-a"), ("1", "gpu-b")]
    assert numeric("12.5", 2) == "25.0"
    assert numeric("N/A") == "NaN"
    assert escape_label('a"b\\c\n') == 'a\\"b\\\\c\\n'
    assert BLOCKING_SIGNATURES["cuda_unavailable"].search("Failed to initialize NVML: Unknown Error")
    assert not BLOCKING_SIGNATURES["cuda_unavailable"].search("CUDA initialized successfully")


if __name__ == "__main__":
    if os.getenv("PROBE_SELF_TEST") == "1":
        self_test()
        raise SystemExit(0)
    probe = Probe()
    Handler.probe = probe
    threading.Thread(target=probe.loop, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 9477), Handler).serve_forever()
