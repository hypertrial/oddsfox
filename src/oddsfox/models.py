"""Pinned local-model loading, chat-template verification, and serialized generation."""

import gc
import hashlib
import importlib.metadata
import json
import platform
import resource
import threading
import time
from importlib import import_module
from pathlib import Path

from oddsfox.ir import fingerprint

MODEL_LOCK = threading.Lock()
MEMORY_LIMIT = 8 * 1024**3
LINEAGE_SCHEMA = "oddsfox-model-lineage/1"
LINEAGE_FILE = "oddsfox-lineage.json"


def chat_template_text(path: Path) -> str:
    for name in ("chat_template.jinja", "tokenizer.jinja"):
        candidate = path / name
        if candidate.is_file():
            text = candidate.read_text(encoding="utf-8")
            if text.strip():
                return text
    config_path = path / "tokenizer_config.json"
    if config_path.is_file():
        data = json.loads(config_path.read_text(encoding="utf-8"))
        template = data.get("chat_template")
        if isinstance(template, str) and template.strip():
            return template
    raise ValueError("model directory has no pinned chat template")


def model_family(config: dict) -> str:
    family = config.get("model_type")
    if isinstance(family, str) and family.strip():
        return family.strip()
    architectures = config.get("architectures") or []
    if architectures and isinstance(architectures[0], str) and architectures[0].strip():
        return architectures[0].strip()
    raise ValueError("model directory does not declare a model family")


def model_manifest(path: Path) -> dict:
    if not path.is_dir():
        raise ValueError("model must be an explicitly downloaded local directory")
    root = path.resolve()
    files = sorted(p for p in path.rglob("*") if p.is_file() or p.is_symlink())
    for file in files:
        if file.is_symlink():
            raise ValueError("model directory must not contain symbolic links")
        resolved = file.resolve()
        if not resolved.is_relative_to(root):
            raise ValueError("model files must stay inside the model directory")
        if file.suffix.casefold() in {".py", ".pyc", ".pyw"}:
            raise ValueError("model directory must not contain Python files")
    relevant = [
        p for p in files if p.suffix in {".safetensors", ".json", ".model", ".txt", ".jinja"}
    ]
    weight_files = [p for p in relevant if p.suffix == ".safetensors"]
    if not weight_files:
        raise ValueError("model directory has no safetensors weights")
    hashes = {}
    for file in relevant:
        with file.open("rb") as handle:
            hashes[str(file.relative_to(path))] = hashlib.file_digest(handle, "sha256").hexdigest()
    config = json.loads((path / "config.json").read_text(encoding="utf-8"))
    if config.get("model_file") is not None:
        raise ValueError("custom model_file loaders are not allowed")
    quantization = config.get("quantization", config.get("quantization_config"))
    if not quantization:
        raise ValueError("V1 model evaluation requires explicitly quantized weights")
    weights_revision = fingerprint(
        sorted(hashes[str(file.relative_to(path))] for file in weight_files)
    )
    lineage_path = path.with_name(f"{path.name}.{LINEAGE_FILE}")
    if not lineage_path.is_file():
        raise ValueError(f"model requires adjacent operator-reviewed {LINEAGE_FILE}")
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
    if set(lineage) != {
        "schema",
        "architecture",
        "lineage",
        "weights_revision",
        "operator_reviewed",
    }:
        raise ValueError("model lineage record has unsupported fields")
    if (
        lineage["schema"] != LINEAGE_SCHEMA
        or lineage["operator_reviewed"] is not True
        or not isinstance(lineage["architecture"], str)
        or not lineage["architecture"].strip()
        or not isinstance(lineage["lineage"], str)
        or not lineage["lineage"].strip()
        or lineage["weights_revision"] != weights_revision
        or lineage["architecture"].strip() != model_family(config)
    ):
        raise ValueError("model lineage record is not reviewed or does not match the weights")
    template = chat_template_text(path)
    return {
        "identity": path.name,
        "family": lineage["lineage"].strip(),
        "architecture": lineage["architecture"].strip(),
        "lineage": lineage["lineage"].strip(),
        "lineage_schema": LINEAGE_SCHEMA,
        "weights_revision": weights_revision,
        "weight_revision": fingerprint(hashes),
        "files": hashes,
        "quantization": quantization,
        "chat_template_sha256": hashlib.sha256(template.encode("utf-8")).hexdigest(),
        "chat_template_chars": len(template),
        "runtime": importlib.metadata.version("mlx-lm"),
        "outlines": importlib.metadata.version("outlines"),
        "platform": platform.platform(),
    }


def verify_loaded_template(tokenizer, manifest: dict) -> None:
    loaded = getattr(tokenizer, "chat_template", None)
    if not isinstance(loaded, str) or not loaded.strip():
        raise ValueError("loaded tokenizer has no chat template")
    digest = hashlib.sha256(loaded.encode("utf-8")).hexdigest()
    if digest != manifest["chat_template_sha256"]:
        raise ValueError("loaded chat template does not match the pinned manifest")


def _unload(mx, model) -> None:
    del model
    clearer = getattr(getattr(mx, "metal", None), "clear_cache", None)
    if callable(clearer):
        clearer()
    gc.collect()


def generate_constrained(
    path: Path,
    prompt: str,
    schema: dict,
    *,
    max_tokens: int,
    timeout_seconds: int,
    memory_limit_bytes: int = MEMORY_LIMIT,
    constrained: bool = True,
) -> tuple[str, int]:
    """Load one local model, generate, then unload before the lock is released."""
    mx = import_module("mlx.core")
    mlx_lm = import_module("mlx_lm")
    make_sampler = import_module("mlx_lm.sample_utils").make_sampler
    previous_limit = mx.set_memory_limit(memory_limit_bytes)
    started = time.monotonic()
    model = None
    try:
        mx.reset_peak_memory()
        pinned = model_manifest(path)
        loaded = mlx_lm.load(str(path))
        model, tokenizer = loaded[0], loaded[1]
        verify_loaded_template(tokenizer, pinned)
        prepared = tokenizer.apply_chat_template(
            [{"role": "user", "content": prompt}],
            tokenize=False,
            add_generation_prompt=True,
        )
        greedy = make_sampler(temp=0)

        def check_budget(*_unused):
            if time.monotonic() - started > timeout_seconds:
                raise TimeoutError("local generation exceeded its recorded time budget")

        def sampler(logits):
            check_budget()
            return greedy(logits)

        if constrained:
            outlines = import_module("outlines")
            raw = outlines.from_mlxlm(model, tokenizer)(
                prepared,
                outlines.types.json_schema(schema),
                max_tokens=max_tokens,
                sampler=sampler,
                prompt_progress_callback=check_budget,
                verbose=False,
            )
        else:
            raw = mlx_lm.generate(
                model,
                tokenizer,
                prepared,
                max_tokens=max_tokens,
                sampler=sampler,
                prompt_progress_callback=check_budget,
                verbose=False,
            )
        if len(raw.encode()) > 128000:
            raise ValueError("model output too large")
        return raw, mx.get_peak_memory()
    finally:
        if model is not None:
            _unload(mx, model)
        mx.set_memory_limit(previous_limit)


def peak_rss() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
