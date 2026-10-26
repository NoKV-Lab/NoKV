"""Versioned, non-executable DCP shard values.

The DCP writer converts values from its trusted local planner into this format.
The reader accepts only this format and never unpickles checkpoint bytes.
"""

from __future__ import annotations

import base64
import io
import json
import struct
import sys
from collections import OrderedDict

import msgpack
import torch


_MAGIC = b"NOKV-DCP-PAYLOAD\x01"
_SCHEMA = "nokv.torch.dcp.payload.v1"
_MAX_HEADER_BYTES = 64 * 1024 * 1024
_MAX_DEPTH = 64
_MAX_DIMENSION = (1 << 63) - 1
_DTYPES = {
    name: getattr(torch, name)
    for name in (
        "bool",
        "uint8",
        "int8",
        "int16",
        "int32",
        "int64",
        "float16",
        "bfloat16",
        "float32",
        "float64",
        "complex64",
        "complex128",
    )
}
SUPPORTED_DTYPE_NAMES = frozenset(_DTYPES)


def _invalid(reason: str) -> ValueError:
    return ValueError(f"invalid NoKV DCP payload: {reason}")


def _object(value, fields: set[str], context: str) -> dict:
    if type(value) is not dict or set(value) != fields:
        raise _invalid(f"{context} must have exactly {sorted(fields)!r}")
    return value


def _list(value, context: str) -> list:
    if type(value) is not list:
        raise _invalid(f"{context} must be a list")
    return value


def _shape(value) -> tuple[int, ...]:
    dimensions = _list(value, "tensor shape")
    if len(dimensions) > 64 or any(
        type(dimension) is not int or not 0 <= dimension <= _MAX_DIMENSION
        for dimension in dimensions
    ):
        raise _invalid("tensor shape is unsupported")
    return tuple(dimensions)


def _dtype(name):
    if type(name) is not str or name not in _DTYPES:
        raise _invalid("tensor dtype is unsupported")
    return _DTYPES[name]


def _tensor_parts(value):
    if type(value) is not torch.Tensor or vars(value):
        raise _invalid("tensor must be a plain torch.Tensor")
    if value.layout is not torch.strided or value.dtype not in _DTYPES.values():
        raise _invalid("tensor must have a supported dense strided dtype")
    tensor = value.detach().to(device="cpu").contiguous().clone()
    name = next(name for name, dtype in _DTYPES.items() if dtype is tensor.dtype)
    shape = list(tensor.shape)
    raw_buffer = io.BytesIO()
    # UntypedStorage iteration is Python-level and prohibitively slow for model
    # shards. PyTorch's storage writer copies the raw bytes in native code.
    tensor.untyped_storage()._write_file(raw_buffer, False, False, 1)
    raw = raw_buffer.getvalue()
    if len(raw) != tensor.numel() * tensor.element_size():
        raise _invalid("tensor storage length differs from its shape")
    return {"dtype": name, "shape": shape, "byteorder": sys.byteorder}, raw


def _tensor_from_parts(document, raw: bytes):
    info = _object(document, {"dtype", "shape", "byteorder"}, "tensor")
    dtype = _dtype(info["dtype"])
    shape = _shape(info["shape"])
    if info["byteorder"] != sys.byteorder:
        raise _invalid("tensor byte order differs from this host")
    count = 1
    for dimension in shape:
        count *= dimension
        if count > _MAX_DIMENSION:
            raise _invalid("tensor shape is too large")
    element_size = torch.empty((), dtype=dtype).element_size()
    if len(raw) != count * element_size:
        raise _invalid("tensor byte count differs from its shape")
    if not raw:
        return torch.empty(shape, dtype=dtype)
    # frombuffer retains the private bytearray, so the tensor owns writable
    # storage without a second full-size clone.
    return torch.frombuffer(bytearray(raw), dtype=dtype).reshape(shape)


def _bytes(value: str) -> bytes:
    if type(value) is not str:
        raise _invalid("binary value must be base64 text")
    try:
        return base64.b64decode(value, validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise _invalid("binary value has invalid base64") from exc


def _encode_value(value, active: set[int], depth: int = 0):
    if depth > _MAX_DEPTH:
        raise _invalid("value is nested too deeply")
    if value is None:
        return {"type": "none"}
    if type(value) is bool:
        return {"type": "bool", "value": value}
    if type(value) is int:
        return {"type": "int", "value": str(value)}
    if type(value) is float:
        return {"type": "float", "value": value.hex()}
    if type(value) is complex:
        return {
            "type": "complex",
            "real": value.real.hex(),
            "imag": value.imag.hex(),
        }
    if type(value) is str:
        return {"type": "str", "value": value}
    if type(value) in (bytes, bytearray):
        return {
            "type": "bytes" if type(value) is bytes else "bytearray",
            "value": base64.b64encode(value).decode("ascii"),
        }
    if type(value) is torch.Tensor:
        info, raw = _tensor_parts(value)
        return {
            "type": "tensor",
            **info,
            "value": base64.b64encode(raw).decode("ascii"),
        }
    if type(value) not in (list, tuple, dict, OrderedDict):
        raise _invalid(f"value has unsupported type {type(value).__name__}")
    identity = id(value)
    if identity in active:
        raise _invalid("value contains a cycle")
    active.add(identity)
    try:
        if type(value) in (dict, OrderedDict):
            entries = []
            for key, item in value.items():
                if type(key) not in (bool, int, float, complex, str, bytes):
                    raise _invalid("dictionary key has an unsupported type")
                entries.append(
                    [
                        _encode_value(key, active, depth + 1),
                        _encode_value(item, active, depth + 1),
                    ]
                )
            return {
                "type": "ordered_dict" if type(value) is OrderedDict else "dict",
                "items": entries,
            }
        return {
            "type": "tuple" if type(value) is tuple else "list",
            "items": [_encode_value(item, active, depth + 1) for item in value],
        }
    finally:
        active.remove(identity)


def _float(value):
    if type(value) is not str or len(value) > 32:
        raise _invalid("floating-point value is unsupported")
    try:
        return float.fromhex(value)
    except ValueError as exc:
        raise _invalid("floating-point value is malformed") from exc


def _decode_value(raw, depth: int = 0):
    if depth > _MAX_DEPTH or type(raw) is not dict or type(raw.get("type")) is not str:
        raise _invalid("value is malformed or nested too deeply")
    kind = raw["type"]
    if kind == "none":
        _object(raw, {"type"}, "null value")
        return None
    if kind == "bool":
        value = _object(raw, {"type", "value"}, "boolean value")["value"]
        if type(value) is not bool:
            raise _invalid("boolean value is malformed")
        return value
    if kind == "int":
        value = _object(raw, {"type", "value"}, "integer value")["value"]
        if (
            type(value) is not str
            or len(value) > 256
            or value != "0"
            and (not value.lstrip("-").isdigit() or value.lstrip("-").startswith("0"))
        ):
            raise _invalid("integer value is malformed")
        return int(value)
    if kind == "float":
        return _float(_object(raw, {"type", "value"}, "float value")["value"])
    if kind == "complex":
        value = _object(raw, {"type", "real", "imag"}, "complex value")
        return complex(_float(value["real"]), _float(value["imag"]))
    if kind == "str":
        value = _object(raw, {"type", "value"}, "string value")["value"]
        if type(value) is not str:
            raise _invalid("string value is malformed")
        return value
    if kind in ("bytes", "bytearray"):
        value = _bytes(_object(raw, {"type", "value"}, "binary value")["value"])
        return value if kind == "bytes" else bytearray(value)
    if kind == "tensor":
        value = _object(
            raw, {"type", "dtype", "shape", "byteorder", "value"}, "tensor value"
        )
        return _tensor_from_parts(
            {key: value[key] for key in ("dtype", "shape", "byteorder")},
            _bytes(value["value"]),
        )
    if kind in ("list", "tuple", "dict", "ordered_dict"):
        items = _list(
            _object(raw, {"type", "items"}, "container value")["items"], "items"
        )
        if kind in ("list", "tuple"):
            result = [_decode_value(item, depth + 1) for item in items]
            return result if kind == "list" else tuple(result)
        result = OrderedDict() if kind == "ordered_dict" else {}
        for pair in items:
            if type(pair) is not list or len(pair) != 2:
                raise _invalid("dictionary entry is malformed")
            key = _decode_value(pair[0], depth + 1)
            if type(key) not in (bool, int, float, complex, str, bytes):
                raise _invalid("dictionary key has an unsupported type")
            if key in result:
                raise _invalid("dictionary contains a duplicate key")
            result[key] = _decode_value(pair[1], depth + 1)
        return result
    raise _invalid(f"value has unsupported type {kind!r}")


def _no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise _invalid(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_constant(value):
    raise _invalid(f"JSON constant {value!r} is unsupported")

def _plain_msgpack_value(value, active: set[int], depth: int = 0) -> bool:
    if depth > _MAX_DEPTH:
        return False
    if value is None or type(value) in (bool, float, str, bytes):
        return True
    if type(value) is int:
        return -(1 << 63) <= value < (1 << 64)
    if type(value) not in (list, dict):
        return False
    identity = id(value)
    if identity in active:
        return False
    active.add(identity)
    try:
        if type(value) is list:
            return all(_plain_msgpack_value(item, active, depth + 1) for item in value)
        return all(
            type(key) is str and _plain_msgpack_value(item, active, depth + 1)
            for key, item in value.items()
        )
    finally:
        active.remove(identity)


def _checked_msgpack_value(value, depth: int = 0):
    if depth > _MAX_DEPTH:
        raise _invalid("MessagePack value is nested too deeply")
    if value is None or type(value) in (bool, int, float, str, bytes):
        return value
    if type(value) is list:
        for item in value:
            _checked_msgpack_value(item, depth + 1)
        return value
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise _invalid("MessagePack map key is not a string")
            _checked_msgpack_value(item, depth + 1)
        return value
    raise _invalid("MessagePack value has an unsupported type")


def _reject_ext(code, data):
    raise _invalid(f"MessagePack extension {code} is unsupported")


def _msgpack_pairs(pairs):
    result = {}
    for key, value in pairs:
        if type(key) is not str or key in result:
            raise _invalid("MessagePack map has an invalid or duplicate key")
        result[key] = value
    return result


def encode_payload(value, kind: str) -> bytes:
    """Encode one checked local DCP value without emitting pickle."""
    if kind == "tensor":
        info, raw = _tensor_parts(value)
        document = {"schema": _SCHEMA, "kind": "tensor", **info}
    elif kind == "bytes":
        if _plain_msgpack_value(value, set()):
            raw = msgpack.packb(value, use_bin_type=True, strict_types=True)
            document = {"schema": _SCHEMA, "kind": "bytes", "codec": "msgpack"}
        else:
            raw = b""
            document = {
                "schema": _SCHEMA,
                "kind": "bytes",
                "value": _encode_value(value, set()),
            }
    else:
        raise _invalid("payload kind is unsupported")
    try:
        header = json.dumps(
            document, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise _invalid("payload contains unsupported values") from exc
    if len(header) > _MAX_HEADER_BYTES:
        raise _invalid("payload header exceeds the size limit")
    return _MAGIC + struct.pack("<I", len(header)) + header + raw


def decode_payload(data: bytes, expected_kind: str):
    """Decode only the NoKV payload envelope and its fixed value model."""
    if type(data) is not bytes or not data.startswith(_MAGIC):
        raise _invalid("unknown or legacy payload format")
    start = len(_MAGIC)
    if len(data) < start + 4:
        raise _invalid("payload header length is missing")
    header_length = struct.unpack_from("<I", data, start)[0]
    start += 4
    if header_length > _MAX_HEADER_BYTES or start + header_length > len(data):
        raise _invalid("payload header length is invalid")
    try:
        document = json.loads(
            data[start : start + header_length].decode("utf-8"),
            object_pairs_hook=_no_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise _invalid("payload header is not valid JSON") from exc
    raw = data[start + header_length :]
    if type(document) is not dict or document.get("schema") != _SCHEMA:
        raise _invalid("payload schema is unsupported")
    if document.get("kind") != expected_kind:
        raise _invalid("payload kind differs from the read plan")
    if expected_kind == "tensor":
        value = _object(
            document,
            {"schema", "kind", "dtype", "shape", "byteorder"},
            "tensor payload",
        )
        return _tensor_from_parts(
            {key: value[key] for key in ("dtype", "shape", "byteorder")}, raw
        )
    if expected_kind == "bytes":
        if document.get("codec") == "msgpack":
            _object(document, {"schema", "kind", "codec"}, "byte payload")
            try:
                value = msgpack.unpackb(
                    raw,
                    raw=False,
                    strict_map_key=True,
                    object_pairs_hook=_msgpack_pairs,
                    ext_hook=_reject_ext,
                )
            except (ValueError, TypeError, msgpack.exceptions.UnpackException) as exc:
                raise _invalid("MessagePack body is malformed") from exc
            return _checked_msgpack_value(value)
        value = _object(document, {"schema", "kind", "value"}, "byte payload")
        if raw:
            raise _invalid("byte payload has trailing data")
        return _decode_value(value["value"])
    raise _invalid("payload kind is unsupported")
