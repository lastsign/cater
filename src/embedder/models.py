from functools import lru_cache
from typing import Iterable

from src.embedder.config import COLBERT_MODEL, DENSE_MODEL, SPARSE_MODEL, USE_GPU


def _preload_cuda_libs() -> None:
    # Preload NVIDIA libs shipped via pip (nvidia-cuda-runtime-cu12, nvidia-cublas-cu12,
    # nvidia-cudnn-cu12) so onnxruntime's CUDA provider finds them without LD_LIBRARY_PATH.
    # Linux ignores LD_LIBRARY_PATH changes after process start, so dlopen with RTLD_GLOBAL
    # is the reliable path. Order matters: cudart -> cublas -> cudnn.
    import ctypes
    import os

    for pkg, sonames in (
        ("nvidia.cuda_runtime", ("libcudart.so.12",)),
        ("nvidia.cublas", ("libcublasLt.so.12", "libcublas.so.12")),
        ("nvidia.cudnn", ("libcudnn.so.9",)),
    ):
        try:
            mod = __import__(pkg, fromlist=["*"])
        except ImportError:
            continue
        # nvidia.* are namespace packages without __file__; use __path__ instead.
        pkg_dir = next(iter(mod.__path__), None)
        if not pkg_dir:
            continue
        lib_dir = os.path.join(pkg_dir, "lib")
        for soname in sonames:
            path = os.path.join(lib_dir, soname)
            if os.path.exists(path):
                try:
                    ctypes.CDLL(path, mode=ctypes.RTLD_GLOBAL)
                except OSError:
                    pass


if USE_GPU:
    _preload_cuda_libs()

from fastembed import LateInteractionTextEmbedding, SparseTextEmbedding, TextEmbedding  # noqa: E402

# Models that require "passage: " / "query: " input prefixes (E5 family).
_E5_PREFIX_MODELS = {
    "intfloat/multilingual-e5-large",
    "intfloat/multilingual-e5-base",
    "intfloat/multilingual-e5-small",
    "intfloat/e5-large-v2",
    "intfloat/e5-base-v2",
    "intfloat/e5-small-v2",
}


def _providers() -> list[str]:
    if not USE_GPU:
        return ["CPUExecutionProvider"]
    import onnxruntime as ort

    available = set(ort.get_available_providers())
    preferred = ["CUDAExecutionProvider", "CPUExecutionProvider"]
    return [p for p in preferred if p in available] or ["CPUExecutionProvider"]


@lru_cache(maxsize=1)
def dense_model() -> TextEmbedding:
    return TextEmbedding(model_name=DENSE_MODEL, providers=_providers())


def _needs_e5_prefix() -> bool:
    return DENSE_MODEL in _E5_PREFIX_MODELS


def passage_texts(texts: Iterable[str]) -> list[str]:
    if _needs_e5_prefix():
        return [f"passage: {t}" for t in texts]
    return list(texts)


def query_text(text: str) -> str:
    if _needs_e5_prefix():
        return f"query: {text}"
    return text


@lru_cache(maxsize=1)
def sparse_model() -> SparseTextEmbedding:
    # BM25 is statistical — CPU is fine and avoids fighting dense model for VRAM.
    return SparseTextEmbedding(model_name=SPARSE_MODEL)


@lru_cache(maxsize=1)
def colbert_model() -> LateInteractionTextEmbedding:
    return LateInteractionTextEmbedding(model_name=COLBERT_MODEL, providers=_providers())
