"""config_loader.py: load config and build table names / file paths."""
import copy
import uuid
from datetime import datetime, timezone

import yaml

REQUIRED_SECTIONS = ["project", "api", "storage", "resources", "meta_tables"]


def _deep_merge(base: dict, extra: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in extra.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(config_path: str, overrides: dict | None = None) -> dict:
    """Read YAML config; optionally override values (e.g. from pipeline parameters)."""
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    if overrides:
        cfg = _deep_merge(cfg, overrides)
    missing = [s for s in REQUIRED_SECTIONS if s not in cfg]
    if missing:
        raise ValueError(f"Config is missing sections: {missing}")
    return cfg


# ---------- resources ----------
def get_resources(cfg: dict, only: list | None = None) -> list:
    """All resources in pipeline order, optionally filtered by name."""
    resources = cfg["resources"]
    if only:
        wanted = [n.strip().lower() for n in only]
        resources = [r for r in resources if r["name"].lower() in wanted]
    return resources


def get_resource(cfg: dict, name: str) -> dict:
    for r in cfg["resources"]:
        if r["name"].lower() == name.lower():
            return r
    raise KeyError(f"Resource '{name}' not found in config")


# ---------- table names ----------
def table_fqn(cfg: dict, layer: str, table: str) -> str:
    catalog = cfg["project"]["catalog"]
    schema = cfg["storage"]["layers"][layer]
    return f"{catalog}.{schema}.{table}"


def resource_table(cfg: dict, layer: str, resource_name: str) -> str:
    return table_fqn(cfg, layer, get_resource(cfg, resource_name)["table"])


def meta_table(cfg: dict, key: str) -> str:
    return table_fqn(cfg, "meta", cfg["meta_tables"][key])


# ---------- raw file paths ----------
def raw_root(cfg: dict, fmt: str | None = None) -> str:
    fmt = fmt or cfg["api"]["format"]
    vol = cfg["storage"]["raw_volume"]
    return f"/Volumes/{cfg['project']['catalog']}/{vol['schema']}/{vol['name']}/{fmt}"


def raw_dir(cfg: dict, resource_name: str, load_date: str, fmt: str | None = None) -> str:
    folder = get_resource(cfg, resource_name)["folder"]
    return f"{raw_root(cfg, fmt)}/{folder}/load_date={load_date}"


def raw_file_path(cfg: dict, resource_name: str, load_date: str,
                  page_number: int, fmt: str | None = None) -> str:
    fmt = fmt or cfg["api"]["format"]
    return f"{raw_dir(cfg, resource_name, load_date, fmt)}/page_{page_number:04d}.{fmt}"


# ---------- small helpers ----------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_batch_id() -> str:
    """Unique id for one run, e.g. 20261126T101500_a1b2c3d4"""
    return utc_now().strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]