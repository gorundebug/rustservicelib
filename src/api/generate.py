#!/usr/bin/env python3
"""Generate Rust API models from servicelib/api/serviceapi.yaml.

The schema deliberately uses a small OpenAPI subset: enums and object models
whose properties are primitives, references, or arrays. Keeping this generator
in the framework repository makes generation reproducible without committing a
generic OpenAPI client runtime.
"""

from __future__ import annotations

import ast
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def schema_path() -> Path:
    positional = [value for value in sys.argv[1:] if not value.startswith("--")]
    if positional:
        return Path(positional[0]).resolve()
    configured = os.environ.get("SERVICEAPI_SCHEMA")
    if configured:
        return Path(configured).resolve()
    return HERE.parents[2] / "servicelib" / "api" / "serviceapi.yaml"


SCHEMA = schema_path()
OUTPUT = HERE / "serviceapi.rs"


def rust_name(value: str) -> str:
    parts = re.split(r"[^A-Za-z0-9]+", value)
    name = "".join(part[:1].upper() + part[1:] for part in parts if part)
    if name and name[0].isdigit():
        name = f"Value{name}"
    return name


def rust_field_name(value: str) -> str:
    words = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    name = re.sub(r"[^A-Za-z0-9]+", "_", words).strip("_").lower()
    if name in {
        "async",
        "crate",
        "dyn",
        "enum",
        "fn",
        "impl",
        "in",
        "loop",
        "match",
        "mod",
        "move",
        "ref",
        "self",
        "struct",
        "super",
        "trait",
        "type",
        "use",
        "where",
    }:
        return f"r#{name}"
    return name


def schema_blocks() -> list[tuple[str, list[str]]]:
    lines = SCHEMA.read_text().splitlines()
    result: list[tuple[str, list[str]]] = []
    index = 0
    while index < len(lines):
        match = re.match(r"^    ([A-Za-z][A-Za-z0-9]*):\s*$", lines[index])
        if not match:
            index += 1
            continue
        name = match.group(1)
        end = index + 1
        while end < len(lines) and not re.match(
            r"^    [A-Za-z][A-Za-z0-9]*:\s*$", lines[end]
        ):
            end += 1
        result.append((match.group(1), lines[index + 1 : end]))
        index = end
    return result


def enum_schemas() -> list[tuple[str, str, list[object], list[str]]]:
    result: list[tuple[str, str, list[object], list[str]]] = []
    for name, block in schema_blocks():
        schema_type = ""
        values: list[object] = []
        names: list[str] = []
        cursor = 0
        while cursor < len(block):
            line = block[cursor]
            if line.startswith("      type:"):
                schema_type = line.split(":", 1)[1].strip()
            elif line.startswith("      enum:"):
                literal = line.split(":", 1)[1].strip()
                while "]" not in literal and cursor + 1 < len(block):
                    cursor += 1
                    literal += " " + block[cursor].strip()
                values = list(ast.literal_eval(literal))
            elif line.startswith("      x-enum-varnames:"):
                cursor += 1
                while cursor < len(block):
                    item = re.match(r"^        -\s+(.*)\s*$", block[cursor])
                    if not item:
                        cursor -= 1
                        break
                    names.append(item.group(1).strip().strip("'\""))
                    cursor += 1
            cursor += 1
        if values and names:
            result.append((name, schema_type, values, names))
    return result


def referenced_type(line: str) -> str:
    return line.rsplit("/", 1)[-1].strip().strip("'\"")


def property_type(block: list[str]) -> str:
    for line in block:
        if line.startswith("          $ref:"):
            return referenced_type(line)
    direct_type = next(
        (
            line.split(":", 1)[1].strip()
            for line in block
            if line.startswith("          type:")
        ),
        None,
    )
    if direct_type == "array":
        for line in block:
            if line.startswith("            $ref:"):
                return f"Vec<{referenced_type(line)}>"
            if line.startswith("            type:"):
                primitive = line.split(":", 1)[1].strip()
                item_type = {
                    "boolean": "bool",
                    "integer": "i64",
                    "number": "f64",
                    "string": "String",
                }[primitive]
                return f"Vec<{item_type}>"
        raise ValueError(f"array property has no supported items: {block!r}")
    if direct_type is not None:
        return {
            "boolean": "bool",
            "integer": "i64",
            "number": "f64",
            "string": "String",
        }[direct_type]
    raise ValueError(f"unsupported property schema: {block!r}")


def object_schemas() -> list[tuple[str, list[tuple[str, str]], set[str]]]:
    result: list[tuple[str, list[tuple[str, str]], set[str]]] = []
    for name, block in schema_blocks():
        if "      type: object" not in block:
            continue
        required: set[str] = set()
        in_required = False
        for line in block:
            if line == "      required:":
                in_required = True
                continue
            if in_required:
                item = re.match(r"^        -\s+([A-Za-z][A-Za-z0-9]*)\s*$", line)
                if item:
                    required.add(item.group(1))
                    continue
                if line and not line.startswith("        "):
                    in_required = False

        try:
            properties_index = block.index("      properties:")
        except ValueError:
            properties_index = -1
        properties: list[tuple[str, str]] = []
        cursor = properties_index + 1
        while properties_index >= 0 and cursor < len(block):
            match = re.match(
                r"^        ([A-Za-z][A-Za-z0-9]*):\s*$", block[cursor]
            )
            if not match:
                if block[cursor] and not block[cursor].startswith("        "):
                    break
                cursor += 1
                continue
            property_name = match.group(1)
            end = cursor + 1
            while end < len(block) and not re.match(
                r"^        [A-Za-z][A-Za-z0-9]*:\s*$", block[end]
            ):
                if block[end] and not block[end].startswith("          "):
                    break
                end += 1
            properties.append((property_name, property_type(block[cursor + 1 : end])))
            cursor = end
        result.append((name, properties, required))
    return result


def generate() -> str:
    blocks = [
        "// Code generated by src/api/generate.py from "
        "servicelib/api/serviceapi.yaml. DO NOT EDIT.",
        "",
        "use serde::{Deserialize, Serialize};",
        "use serde_repr::{Deserialize_repr, Serialize_repr};",
        "",
    ]
    for schema_name, schema_type, values, names in enum_schemas():
        if schema_type == "integer":
            blocks.extend(
                [
                    "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, "
                    "Serialize_repr, Deserialize_repr)]",
                    "#[repr(i32)]",
                    f"pub enum {schema_name} {{",
                ]
            )
            for name, value in zip(names, values, strict=True):
                blocks.append(f"    {rust_name(name)} = {value},")
        else:
            blocks.extend(
                [
                    "#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, "
                    "Serialize, Deserialize)]",
                    f"pub enum {schema_name} {{",
                ]
            )
            for name, value in zip(names, values, strict=True):
                escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
                blocks.append(f'    #[serde(rename = "{escaped}")]')
                blocks.append(f"    {rust_name(name)},")
        blocks.extend(["}", ""])
    for schema_name, properties, required in object_schemas():
        blocks.extend(
            [
                "#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]",
                "#[serde(deny_unknown_fields)]",
                f"pub struct {schema_name} {{",
            ]
        )
        for property_name, property_type_name in properties:
            field_name = rust_field_name(property_name)
            if property_name in required:
                blocks.append(f'    #[serde(rename = "{property_name}")]')
                blocks.append(f"    pub {field_name}: {property_type_name},")
            else:
                attribute = (
                    f'    #[serde(rename = "{property_name}", '
                    'skip_serializing_if = "Option::is_none")]'
                )
                if len(attribute) <= 84:
                    blocks.append(attribute)
                else:
                    blocks.extend(
                        [
                            "    #[serde(",
                            f'        rename = "{property_name}",',
                            '        skip_serializing_if = "Option::is_none"',
                            "    )]",
                        ]
                    )
                blocks.append(f"    pub {field_name}: Option<{property_type_name}>,")
        blocks.extend(["}", ""])
    return "\n".join(blocks)


if __name__ == "__main__":
    if not SCHEMA.exists():
        raise SystemExit(f"schema not found: {SCHEMA}")
    generated = generate()
    if "--check" in sys.argv:
        if not OUTPUT.exists() or OUTPUT.read_text() != generated:
            raise SystemExit(
                "generated Rust API is stale; run ./scripts/sync_api.sh"
            )
        print(f"checked {OUTPUT}")
        raise SystemExit(0)
    OUTPUT.write_text(generated)
    print(f"generated {OUTPUT}")
