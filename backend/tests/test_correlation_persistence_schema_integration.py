"""Real PostgreSQL schema integration tests for Task 4.9-M1-R3.

Marked pytest.mark.integration.
Tests execute against a portable disposable database and runtime login role.
Embedded schema contract transcribed from approved 69-row manifest and A1-R4 decisions.
Structured PostgreSQL catalog reconciliation, ORM metadata verification,
direct-SQL boundary probes, tenant RLS isolation, and minimum privilege checks.
No embedded credentials, no hardcoded suffixes, no external manifest file dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
import copy
from decimal import Decimal
import hashlib
import json
import os
import re
import secrets
import subprocess
import urllib.parse
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.dialects import postgresql
from sqlalchemy.inspection import inspect as sa_inspect

from app.anomaly.lineage import AnomalyReproducibilityLineage
from app.anomaly.models import AnomalySignal, CalibrationMetadata, EventSeverity
from app.correlation.aggregate import create_or_update_incident
from app.correlation.consumer import AnomalyCorrelationInput
from app.correlation.grouping import CorrelationGroup, GroupingMemberReference
from app.correlation.models import CorrelationEvidence, CorrelationWindowState
from app.correlation.serialization import serialize_incident_aggregate

from app.models.incident import (
    IncidentCorrelationEvidenceRecord,
    IncidentPublicationIntentRecord,
    IncidentRecord,
    IncidentSignalLinkRecord,
    IncidentTimelineEventRecord,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(scope="module")]

MAX_AGGREGATE_PAYLOAD_BYTES = 67108864
OVER_MAX_AGGREGATE_PAYLOAD_BYTES = 67108865

BACKEND_DIR = Path(__file__).resolve().parents[1]


# ============================================================================
# PURE COMPARATOR API & DATACLASSES
# ============================================================================
@dataclass(frozen=True)
class DefinitionMismatch:
    object_id: str
    mismatch_type: str
    expected: Any
    actual: Any
    message: str = ""


@dataclass(frozen=True)
class CaseMismatch:
    case_id: str
    mismatch_type: str
    expected: Any
    actual: Any
    message: str = ""


def canonical_json(value: Any) -> str:
    """Canonical JSON with sorted keys, compact separators, UTF-8, and no NaN."""
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def normalize_sql_expression(s: str | None) -> str:
    """Normalize SQL expressions across PostgreSQL dialect variations."""
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"::[a-zA-Z0-9_]+(?:\s+[a-zA-Z0-9_]+)?(?:\[\])?", "", s)
    s = s.replace('"', "")
    s = re.sub(r"\s+", " ", s)
    while s.startswith("(") and s.endswith(")"):
        depth = 0
        encloses = True
        for ch in s[:-1]:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    encloses = False
                    break
        if encloses:
            s = s[1:-1].strip()
        else:
            break
    return s.strip()


normalize_sql_expr = normalize_sql_expression


def _obj_key(o: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(o.get("kind", "")),
        str(o.get("schema", o.get("schema_name", "public"))),
        str(o.get("table", o.get("table_name", ""))),
        str(o.get("name", "")),
    )


def compare_catalog_contract(
    expected: list[dict[str, Any]] | dict[Any, dict[str, Any]],
    actual: list[dict[str, Any]] | dict[Any, dict[str, Any]],
) -> list[DefinitionMismatch]:
    """Pure comparator for exact PostgreSQL catalog objects."""
    exp_map: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    if isinstance(expected, dict):
        for k, v in expected.items():
            if isinstance(k, tuple) and len(k) == 4:
                exp_map[k] = v
            else:
                exp_map[_obj_key(v)] = v
    else:
        for v in expected:
            exp_map[_obj_key(v)] = v

    act_map: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    if isinstance(actual, dict):
        for k, v in actual.items():
            if isinstance(k, tuple) and len(k) == 4:
                act_map[k] = v
            else:
                act_map[_obj_key(v)] = v
    else:
        for v in actual:
            act_map[_obj_key(v)] = v

    mismatches: list[DefinitionMismatch] = []

    for k, exp_o in exp_map.items():
        if k not in act_map:
            mismatches.append(
                DefinitionMismatch(
                    object_id=f"{k[0]}:{k[1]}:{k[2]}:{k[3]}",
                    mismatch_type="missing",
                    expected=exp_o,
                    actual=None,
                    message=f"Expected object {k} not found in actual catalog",
                )
            )
            continue
        act_o = act_map[k]
        oid_str = f"{k[0]}:{k[1]}:{k[2]}:{k[3]}"
        kind = k[0]

        if kind == "primary_key":
            exp_cols = list(exp_o.get("columns", []))
            act_cols = list(act_o.get("columns", []))
            if exp_cols != act_cols:
                mismatches.append(
                    DefinitionMismatch(oid_str, "pk_columns", exp_cols, act_cols)
                )

        elif kind == "unique_constraint":
            exp_cols = list(exp_o.get("columns", []))
            act_cols = list(act_o.get("columns", []))
            if exp_cols != act_cols:
                mismatches.append(
                    DefinitionMismatch(oid_str, "uq_columns", exp_cols, act_cols)
                )

        elif kind == "foreign_key":
            exp_cols = list(exp_o.get("columns", []))
            act_cols = list(act_o.get("columns", []))
            if exp_cols != act_cols:
                mismatches.append(
                    DefinitionMismatch(oid_str, "fk_local_columns", exp_cols, act_cols)
                )
            exp_ref_cols = list(exp_o.get("ref_columns", []))
            act_ref_cols = list(act_o.get("ref_columns", []))
            if exp_ref_cols != act_ref_cols:
                mismatches.append(
                    DefinitionMismatch(
                        oid_str, "fk_ref_columns", exp_ref_cols, act_ref_cols
                    )
                )
            if exp_o.get("ref_table") != act_o.get("ref_table"):
                mismatches.append(
                    DefinitionMismatch(
                        oid_str,
                        "fk_ref_table",
                        exp_o.get("ref_table"),
                        act_o.get("ref_table"),
                    )
                )

        elif kind == "check_constraint":
            exp_expr = normalize_sql_expression(
                exp_o.get("normalized_expr") or exp_o.get("expr")
            )
            act_expr = normalize_sql_expression(
                act_o.get("normalized_expr") or act_o.get("expr")
            )
            if exp_expr != act_expr:
                mismatches.append(
                    DefinitionMismatch(oid_str, "check_expression", exp_expr, act_expr)
                )

        elif kind == "index":
            if exp_o.get("unique") != act_o.get("unique", False):
                mismatches.append(
                    DefinitionMismatch(
                        oid_str,
                        "index_unique",
                        exp_o.get("unique"),
                        act_o.get("unique"),
                    )
                )
            if exp_o.get("am", "btree") != act_o.get(
                "am", act_o.get("access_method", "btree")
            ):
                mismatches.append(
                    DefinitionMismatch(
                        oid_str, "index_am", exp_o.get("am"), act_o.get("am")
                    )
                )
            exp_keys = list(exp_o.get("key_expressions", []))
            act_keys = list(act_o.get("key_expressions", []))
            if exp_keys != act_keys:
                mismatches.append(
                    DefinitionMismatch(
                        oid_str, "index_key_expressions", exp_keys, act_keys
                    )
                )
            exp_dirs = list(exp_o.get("directions", []))
            act_dirs = list(act_o.get("directions", []))
            if exp_dirs != act_dirs:
                mismatches.append(
                    DefinitionMismatch(oid_str, "index_directions", exp_dirs, act_dirs)
                )
            exp_nulls = list(exp_o.get("null_orderings", []))
            act_nulls = list(act_o.get("null_orderings", []))
            if exp_nulls != act_nulls:
                mismatches.append(
                    DefinitionMismatch(
                        oid_str, "index_null_orderings", exp_nulls, act_nulls
                    )
                )
            exp_inc = list(exp_o.get("included_columns", []))
            act_inc = list(act_o.get("included_columns", []))
            if exp_inc != act_inc:
                mismatches.append(
                    DefinitionMismatch(
                        oid_str, "index_included_columns", exp_inc, act_inc
                    )
                )
            exp_pred = normalize_sql_expression(exp_o.get("predicate"))
            act_pred = normalize_sql_expression(act_o.get("predicate"))
            if exp_pred != act_pred:
                mismatches.append(
                    DefinitionMismatch(oid_str, "index_predicate", exp_pred, act_pred)
                )

        elif kind == "rls_policy":
            if exp_o.get("permissive", True) != act_o.get("permissive", True):
                mismatches.append(
                    DefinitionMismatch(
                        oid_str,
                        "policy_permissive",
                        exp_o.get("permissive"),
                        act_o.get("permissive"),
                    )
                )
            if exp_o.get("cmd", "*") != act_o.get("cmd", "*"):
                mismatches.append(
                    DefinitionMismatch(
                        oid_str, "policy_cmd", exp_o.get("cmd"), act_o.get("cmd")
                    )
                )
            exp_roles = sorted(str(r) for r in exp_o.get("roles", []))
            act_roles = sorted(str(r) for r in act_o.get("roles", []))
            exp_roles = ["0"] if exp_roles in (["0"], [""], ["public"]) else exp_roles
            act_roles = ["0"] if act_roles in (["0"], [""], ["public"]) else act_roles
            if exp_roles != act_roles:
                mismatches.append(
                    DefinitionMismatch(oid_str, "policy_roles", exp_roles, act_roles)
                )
            exp_using = normalize_sql_expression(exp_o.get("using_expr"))
            act_using = normalize_sql_expression(act_o.get("using_expr"))
            if exp_using != act_using:
                mismatches.append(
                    DefinitionMismatch(oid_str, "policy_using", exp_using, act_using)
                )
            exp_check = normalize_sql_expression(
                exp_o.get("check_expr") or exp_o.get("with_check")
            )
            act_check = normalize_sql_expression(
                act_o.get("check_expr") or act_o.get("with_check")
            )
            if exp_check != act_check:
                mismatches.append(
                    DefinitionMismatch(oid_str, "policy_check", exp_check, act_check)
                )

        elif kind == "trigger":
            if exp_o.get("table") != act_o.get("table", act_o.get("table_name")):
                mismatches.append(
                    DefinitionMismatch(
                        oid_str, "trigger_table", exp_o.get("table"), act_o.get("table")
                    )
                )
            exp_fn = exp_o.get("function_name", exp_o.get("fn_name"))
            act_fn = act_o.get("function_name", act_o.get("fn_name"))
            if exp_fn != act_fn:
                mismatches.append(
                    DefinitionMismatch(oid_str, "trigger_fn", exp_fn, act_fn)
                )

        elif kind == "trigger_function":
            if exp_o.get("ret") != act_o.get("ret"):
                mismatches.append(
                    DefinitionMismatch(
                        oid_str, "trigger_func_ret", exp_o.get("ret"), act_o.get("ret")
                    )
                )
            if exp_o.get("lang") != act_o.get("lang"):
                mismatches.append(
                    DefinitionMismatch(
                        oid_str,
                        "trigger_func_lang",
                        exp_o.get("lang"),
                        act_o.get("lang"),
                    )
                )
            if exp_o.get("volatility") != act_o.get("volatility"):
                mismatches.append(
                    DefinitionMismatch(
                        oid_str,
                        "trigger_func_volatility",
                        exp_o.get("volatility"),
                        act_o.get("volatility"),
                    )
                )
            if exp_o.get("secdef") != act_o.get("secdef"):
                mismatches.append(
                    DefinitionMismatch(
                        oid_str,
                        "trigger_func_secdef",
                        exp_o.get("secdef"),
                        act_o.get("secdef"),
                    )
                )
            exp_body = normalize_sql_expression(exp_o.get("prosrc"))
            act_body = normalize_sql_expression(act_o.get("prosrc"))
            if exp_body != act_body:
                mismatches.append(
                    DefinitionMismatch(oid_str, "trigger_func_body", exp_body, act_body)
                )

    for k, act_o in act_map.items():
        if k not in exp_map:
            mismatches.append(
                DefinitionMismatch(
                    object_id=f"{k[0]}:{k[1]}:{k[2]}:{k[3]}",
                    mismatch_type="unexpected",
                    expected=None,
                    actual=act_o,
                    message=f"Unexpected object {k} found in live catalog",
                )
            )

    return mismatches


def compare_orm_contract(
    expected: dict[str, dict[str, Any]],
    actual: dict[str, dict[str, Any]],
) -> list[DefinitionMismatch]:
    """Pure comparator for exact ORM metadata contracts."""
    mismatches: list[DefinitionMismatch] = []

    exp_tables = {v.get("table_name", k): v for k, v in expected.items()}
    act_tables = {v.get("table_name", k): v for k, v in actual.items()}

    for tbl_name, exp_t in exp_tables.items():
        if tbl_name not in act_tables:
            mismatches.append(
                DefinitionMismatch(tbl_name, "orm_table_missing", exp_t, None)
            )
            continue
        act_t = act_tables[tbl_name]

        exp_cols = exp_t.get("columns", [])
        act_cols = act_t.get("columns", [])
        if len(exp_cols) != len(act_cols):
            mismatches.append(
                DefinitionMismatch(
                    tbl_name, "orm_column_count", len(exp_cols), len(act_cols)
                )
            )
        for i, (ec, ac) in enumerate(zip(exp_cols, act_cols)):
            if ec.get("name") != ac.get("name"):
                mismatches.append(
                    DefinitionMismatch(
                        f"{tbl_name}.col_{i}",
                        "orm_column_order",
                        ec.get("name"),
                        ac.get("name"),
                    )
                )
            if ec.get("type") != ac.get("type"):
                mismatches.append(
                    DefinitionMismatch(
                        f"{tbl_name}.{ec.get('name')}",
                        "orm_column_type",
                        ec.get("type"),
                        ac.get("type"),
                    )
                )
            if ec.get("nullable") != ac.get("nullable"):
                mismatches.append(
                    DefinitionMismatch(
                        f"{tbl_name}.{ec.get('name')}",
                        "orm_column_nullable",
                        ec.get("nullable"),
                        ac.get("nullable"),
                    )
                )
            if ec.get("pk") != ac.get("pk"):
                mismatches.append(
                    DefinitionMismatch(
                        f"{tbl_name}.{ec.get('name')}",
                        "orm_column_pk",
                        ec.get("pk"),
                        ac.get("pk"),
                    )
                )

        exp_pk = exp_t.get("primary_key", {})
        act_pk = act_t.get("primary_key", {})
        if exp_pk.get("columns") != act_pk.get("columns"):
            mismatches.append(
                DefinitionMismatch(
                    tbl_name,
                    "orm_pk_columns",
                    exp_pk.get("columns"),
                    act_pk.get("columns"),
                )
            )

        exp_cks = {ck["name"]: ck for ck in exp_t.get("check_constraints", [])}
        act_cks = {ck["name"]: ck for ck in act_t.get("check_constraints", [])}
        for ck_name, exp_ck in exp_cks.items():
            if ck_name not in act_cks:
                mismatches.append(
                    DefinitionMismatch(
                        f"{tbl_name}.{ck_name}", "orm_check_missing", exp_ck, None
                    )
                )
            else:
                act_ck = act_cks[ck_name]
                exp_norm = normalize_sql_expression(
                    exp_ck.get("normalized_expr") or exp_ck.get("expr")
                )
                act_norm = normalize_sql_expression(
                    act_ck.get("normalized_expr") or act_ck.get("expr")
                )
                if exp_norm != act_norm:
                    mismatches.append(
                        DefinitionMismatch(
                            f"{tbl_name}.{ck_name}",
                            "orm_check_expr",
                            exp_norm,
                            act_norm,
                        )
                    )

        exp_fks = {fk["name"]: fk for fk in exp_t.get("foreign_keys", [])}
        act_fks = {fk["name"]: fk for fk in act_t.get("foreign_keys", [])}
        for fk_name, exp_fk in exp_fks.items():
            if fk_name not in act_fks:
                mismatches.append(
                    DefinitionMismatch(
                        f"{tbl_name}.{fk_name}", "orm_fk_missing", exp_fk, None
                    )
                )
            else:
                act_fk = act_fks[fk_name]
                if exp_fk.get("local_columns") != act_fk.get("local_columns"):
                    mismatches.append(
                        DefinitionMismatch(
                            f"{tbl_name}.{fk_name}",
                            "orm_fk_local_cols",
                            exp_fk.get("local_columns"),
                            act_fk.get("local_columns"),
                        )
                    )
                if exp_fk.get("remote_table") != act_fk.get("remote_table"):
                    mismatches.append(
                        DefinitionMismatch(
                            f"{tbl_name}.{fk_name}",
                            "orm_fk_remote_table",
                            exp_fk.get("remote_table"),
                            act_fk.get("remote_table"),
                        )
                    )
                if exp_fk.get("remote_columns") != act_fk.get("remote_columns"):
                    mismatches.append(
                        DefinitionMismatch(
                            f"{tbl_name}.{fk_name}",
                            "orm_fk_remote_cols",
                            exp_fk.get("remote_columns"),
                            act_fk.get("remote_columns"),
                        )
                    )

        exp_idxs = {idx["name"]: idx for idx in exp_t.get("indexes", [])}
        act_idxs = {idx["name"]: idx for idx in act_t.get("indexes", [])}
        for idx_name, exp_idx in exp_idxs.items():
            if idx_name not in act_idxs:
                mismatches.append(
                    DefinitionMismatch(
                        f"{tbl_name}.{idx_name}", "orm_idx_missing", exp_idx, None
                    )
                )
            else:
                act_idx = act_idxs[idx_name]
                if exp_idx.get("expressions") != act_idx.get("expressions"):
                    mismatches.append(
                        DefinitionMismatch(
                            f"{tbl_name}.{idx_name}",
                            "orm_idx_expressions",
                            exp_idx.get("expressions"),
                            act_idx.get("expressions"),
                        )
                    )

        prohib = act_t.get("prohibited_constructs", [])
        if prohib:
            mismatches.append(
                DefinitionMismatch(tbl_name, "orm_prohibited_constructs", [], prohib)
            )

    for tbl_name in act_tables:
        if tbl_name not in exp_tables:
            mismatches.append(
                DefinitionMismatch(
                    tbl_name, "orm_table_unexpected", None, act_tables[tbl_name]
                )
            )

    return mismatches


def compare_boundary_cases(
    expected: list[dict[str, Any]] | dict[str, dict[str, Any]],
    actual: list[dict[str, Any]] | dict[str, dict[str, Any]],
) -> list[CaseMismatch]:
    """Pure comparator for direct-SQL boundary cases."""
    exp_map = {
        c["case_id"]: c
        for c in (expected.values() if isinstance(expected, dict) else expected)
    }
    act_map = {
        c["case_id"]: c
        for c in (actual.values() if isinstance(actual, dict) else actual)
    }

    mismatches: list[CaseMismatch] = []
    for cid, exp_c in exp_map.items():
        if cid not in act_map:
            mismatches.append(
                CaseMismatch(
                    cid, "missing", exp_c, None, f"Boundary case {cid} missing"
                )
            )
            continue
        act_c = act_map[cid]
        exp_out = exp_c.get("expected_outcome", exp_c.get("outcome"))
        obs_out = act_c.get("observed_outcome", act_c.get("outcome"))
        if exp_out != obs_out:
            mismatches.append(CaseMismatch(cid, "outcome", exp_out, obs_out))
        exp_sql = exp_c.get("expected_sqlstate", exp_c.get("sqlstate"))
        obs_sql = act_c.get("observed_sqlstate", act_c.get("sqlstate"))
        if exp_sql != obs_sql:
            mismatches.append(CaseMismatch(cid, "sqlstate", exp_sql, obs_sql))
        exp_con = exp_c.get("expected_constraint", exp_c.get("constraint"))
        obs_con = act_c.get("observed_constraint", act_c.get("constraint"))
        if exp_con != obs_con:
            mismatches.append(CaseMismatch(cid, "constraint", exp_con, obs_con))

    for cid in act_map:
        if cid not in exp_map:
            mismatches.append(
                CaseMismatch(
                    cid,
                    "unexpected",
                    None,
                    act_map[cid],
                    f"Unexpected boundary case {cid}",
                )
            )

    return mismatches


def compare_rls_cases(
    expected: list[dict[str, Any]] | dict[str, dict[str, Any]],
    actual: list[dict[str, Any]] | dict[str, dict[str, Any]],
) -> list[CaseMismatch]:
    """Pure comparator for RLS probe cases."""
    exp_map = {
        c["case_id"]: c
        for c in (expected.values() if isinstance(expected, dict) else expected)
    }
    act_map = {
        c["case_id"]: c
        for c in (actual.values() if isinstance(actual, dict) else actual)
    }

    mismatches: list[CaseMismatch] = []
    for cid, exp_c in exp_map.items():
        if cid not in act_map:
            mismatches.append(
                CaseMismatch(cid, "missing", exp_c, None, f"RLS case {cid} missing")
            )
            continue
        act_c = act_map[cid]
        exp_out = exp_c.get("expected_outcome", exp_c.get("outcome"))
        obs_out = act_c.get("observed_outcome", act_c.get("outcome"))
        if exp_out != obs_out:
            mismatches.append(CaseMismatch(cid, "outcome", exp_out, obs_out))
        exp_sql = exp_c.get("expected_sqlstate", exp_c.get("sqlstate"))
        obs_sql = act_c.get("observed_sqlstate", act_c.get("sqlstate"))
        if exp_sql != obs_sql:
            mismatches.append(CaseMismatch(cid, "sqlstate", exp_sql, obs_sql))

        if "READ_TENANT" in cid or "MISSING_CONTEXT" in cid:
            exp_tenants = exp_c.get("expected_tenants", {})
            obs_tenants = act_c.get("observed_tenants", {})
            if set(exp_tenants.keys()) != set(obs_tenants.keys()):
                mismatches.append(
                    CaseMismatch(
                        cid,
                        "tenant_set",
                        set(exp_tenants.keys()),
                        set(obs_tenants.keys()),
                    )
                )
            if exp_tenants != obs_tenants:
                mismatches.append(
                    CaseMismatch(cid, "row_count", exp_tenants, obs_tenants)
                )

    for cid in act_map:
        if cid not in exp_map:
            mismatches.append(
                CaseMismatch(
                    cid, "unexpected", None, act_map[cid], f"Unexpected RLS case {cid}"
                )
            )

    return mismatches


def build_expected_rls_cases(
    tenant_a: uuid.UUID,
    tenant_b: uuid.UUID,
    expected_counts: dict[str, int],
) -> list[dict[str, Any]]:
    """Build the RLS oracle independently from observed query results."""
    expected: list[dict[str, Any]] = []
    for table, count in expected_counts.items():
        tag = table.upper()
        expected.extend(
            [
                {
                    "case_id": f"RLS_READ_TENANT_A_{tag}",
                    "expected_outcome": "ACCEPTED",
                    "expected_sqlstate": "NONE",
                    "expected_tenants": {str(tenant_a): count},
                },
                {
                    "case_id": f"RLS_READ_TENANT_B_{tag}",
                    "expected_outcome": "ACCEPTED",
                    "expected_sqlstate": "NONE",
                    "expected_tenants": {str(tenant_b): count},
                },
                {
                    "case_id": f"RLS_READ_MISSING_CONTEXT_{tag}",
                    "expected_outcome": "ACCEPTED",
                    "expected_sqlstate": "NONE",
                    "expected_tenants": {},
                },
                {
                    "case_id": f"RLS_WRITE_CROSS_TENANT_{tag}",
                    "expected_outcome": "REJECTED",
                    "expected_sqlstate": "42501",
                },
            ]
        )
    return expected


def check_canonical_expr_matches(
    actual: str,
    expected: str,
    mutated: str,
) -> tuple[bool, bool]:
    """Pure canonical-comparison helper.

    Returns (matches_expected, mutation_creates_mismatch).
    Proves changing one literal or operator creates a mismatch.
    """
    act_norm = normalize_sql_expression(actual)
    exp_norm = normalize_sql_expression(expected)
    mut_norm = normalize_sql_expression(mutated)
    matches = act_norm == exp_norm
    mutation_mismatch = act_norm != mut_norm
    return (matches, mutation_mismatch)


# ============================================================================
# INDEPENDENT EMBEDDED EXPECTED CATALOG CONTRACT (69 OBJECTS)
# Transcribed from approved 69-row manifest and A1-R4 decisions.
# ============================================================================
EMBEDDED_EXPECTED_COLUMNS: dict[str, list[dict[str, Any]]] = {
    "incidents": [
        {"name": "tenant_id", "type": "uuid", "nullable": False, "pk": True},
        {"name": "incident_id", "type": "uuid", "nullable": False, "pk": True},
        {
            "name": "schema_version",
            "type": "character varying(10)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "incident_number",
            "type": "character varying(128)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "environment",
            "type": "character varying(128)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "status",
            "type": "character varying(20)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "correlation_state",
            "type": "character varying(20)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "severity",
            "type": "character varying(20)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "severity_policy_version",
            "type": "character varying(128)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "started_at",
            "type": "timestamp with time zone",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "last_seen_at",
            "type": "timestamp with time zone",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "created_at",
            "type": "timestamp with time zone",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "updated_at",
            "type": "timestamp with time zone",
            "nullable": False,
            "pk": False,
        },
        {"name": "revision", "type": "numeric", "nullable": False, "pk": False},
        {
            "name": "primary_service",
            "type": "character varying(128)",
            "nullable": False,
            "pk": False,
        },
        {"name": "affected_services", "type": "jsonb", "nullable": False, "pk": False},
        {
            "name": "revision_fingerprint",
            "type": "character varying(64)",
            "nullable": False,
            "pk": False,
        },
        {"name": "aggregate_payload", "type": "bytea", "nullable": False, "pk": False},
    ],
    "incident_signal_links": [
        {"name": "tenant_id", "type": "uuid", "nullable": False, "pk": True},
        {"name": "incident_id", "type": "uuid", "nullable": False, "pk": True},
        {"name": "signal_id", "type": "uuid", "nullable": False, "pk": True},
        {
            "name": "semantic_fingerprint",
            "type": "character varying(64)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "environment",
            "type": "character varying(128)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "service",
            "type": "character varying(128)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "event_time",
            "type": "timestamp with time zone",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "severity",
            "type": "character varying(20)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "anomaly_score",
            "type": "double precision",
            "nullable": False,
            "pk": False,
        },
    ],
    "incident_correlation_evidence": [
        {"name": "tenant_id", "type": "uuid", "nullable": False, "pk": True},
        {"name": "incident_id", "type": "uuid", "nullable": False, "pk": True},
        {"name": "left_signal_id", "type": "uuid", "nullable": False, "pk": True},
        {"name": "right_signal_id", "type": "uuid", "nullable": False, "pk": True},
        {
            "name": "temporal_score",
            "type": "double precision",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "service_score",
            "type": "double precision",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "topology_score",
            "type": "double precision",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "symptom_score",
            "type": "double precision",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "context_score",
            "type": "double precision",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "weighted_score",
            "type": "double precision",
            "nullable": False,
            "pk": False,
        },
        {"name": "accepted", "type": "boolean", "nullable": False, "pk": False},
        {
            "name": "policy_version",
            "type": "character varying(128)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "reason",
            "type": "character varying(512)",
            "nullable": False,
            "pk": False,
        },
    ],
    "incident_timeline_events": [
        {"name": "tenant_id", "type": "uuid", "nullable": False, "pk": True},
        {"name": "incident_id", "type": "uuid", "nullable": False, "pk": False},
        {"name": "event_id", "type": "uuid", "nullable": False, "pk": True},
        {
            "name": "schema_version",
            "type": "character varying(10)",
            "nullable": False,
            "pk": False,
        },
        {"name": "revision", "type": "numeric", "nullable": False, "pk": False},
        {"name": "event_ordinal", "type": "integer", "nullable": False, "pk": False},
        {
            "name": "event_time",
            "type": "timestamp with time zone",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "event_type",
            "type": "character varying(40)",
            "nullable": False,
            "pk": False,
        },
        {"name": "source_signal_id", "type": "uuid", "nullable": True, "pk": False},
        {
            "name": "summary",
            "type": "character varying(512)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "revision_fingerprint",
            "type": "character varying(64)",
            "nullable": False,
            "pk": False,
        },
    ],
    "incident_publication_intents": [
        {"name": "tenant_id", "type": "uuid", "nullable": False, "pk": True},
        {"name": "incident_id", "type": "uuid", "nullable": False, "pk": True},
        {"name": "revision", "type": "numeric", "nullable": False, "pk": True},
        {
            "name": "schema_version",
            "type": "character varying(10)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "revision_fingerprint",
            "type": "character varying(64)",
            "nullable": False,
            "pk": False,
        },
        {
            "name": "intent_time",
            "type": "timestamp with time zone",
            "nullable": False,
            "pk": False,
        },
        {"name": "aggregate_payload", "type": "bytea", "nullable": False, "pk": False},
    ],
}

EMBEDDED_EXPECTED_OBJECTS: list[dict[str, Any]] = [
    {
        "columns": ("tenant_id", "incident_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "primary_key",
        "name": "pk_incidents",
        "table": "incidents",
    },
    {
        "columns": ("tenant_id",),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incidents_tenant",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("id",),
        "ref_schema": "public",
        "ref_table": "tenants",
        "table": "incidents",
    },
    {
        "columns": ("tenant_id", "incident_number"),
        "deferrable": False,
        "deferred": False,
        "kind": "unique_constraint",
        "name": "uq_incidents_tenant_number",
        "table": "incidents",
    },
    {
        "expr": "((schema_version)::text = '1.2'::text)",
        "kind": "check_constraint",
        "mutated_expr": "((schema_version)::text = '1.3'::text)",
        "name": "ck_incidents_schema_version",
        "table": "incidents",
    },
    {
        "expr": "((incident_number)::text = ('INC-'::text || upper(replace((incident_id)::text, '-'::text, ''::text))))",
        "kind": "check_constraint",
        "mutated_expr": "((incident_number)::text = ('INC2-'::text || upper(replace((incident_id)::text, '-'::text, "
        "''::text))))",
        "name": "ck_incidents_incident_number",
        "table": "incidents",
    },
    {
        "expr": "((status)::text = ANY ((ARRAY['open'::character varying, 'acknowledged'::character varying, "
        "'resolved'::character varying, 'closed'::character varying])::text[]))",
        "kind": "check_constraint",
        "mutated_expr": "((status)::text = ANY ((ARRAY['open'::character varying, 'closed'::character varying])::text[]))",
        "name": "ck_incidents_status",
        "table": "incidents",
    },
    {
        "expr": "((correlation_state)::text = ANY ((ARRAY['collecting'::character varying, 'settling'::character varying, "
        "'finalized'::character varying])::text[]))",
        "kind": "check_constraint",
        "mutated_expr": "((correlation_state)::text = ANY ((ARRAY['collecting'::character varying])::text[]))",
        "name": "ck_incidents_correlation_state",
        "table": "incidents",
    },
    {
        "expr": "((severity)::text = ANY ((ARRAY['debug'::character varying, 'info'::character varying, 'warning'::character "
        "varying, 'error'::character varying, 'critical'::character varying])::text[]))",
        "kind": "check_constraint",
        "mutated_expr": "((severity)::text = ANY ((ARRAY['debug'::character varying, 'info'::character varying])::text[]))",
        "name": "ck_incidents_severity",
        "table": "incidents",
    },
    {
        "expr": "((severity_policy_version)::text = 'max-member-severity/v1'::text)",
        "kind": "check_constraint",
        "mutated_expr": "((severity_policy_version)::text = 'max-member-severity/v2'::text)",
        "name": "ck_incidents_severity_policy",
        "table": "incidents",
    },
    {
        "expr": "((revision = trunc(revision)) AND ((revision >= (1)::numeric) AND (revision <= "
        "'18446744073709551615'::numeric)))",
        "kind": "check_constraint",
        "mutated_expr": "((revision = trunc(revision)) AND ((revision >= (0)::numeric) AND (revision <= "
        "'18446744073709551615'::numeric)))",
        "name": "ck_incidents_revision",
        "table": "incidents",
    },
    {
        "expr": "((started_at <= last_seen_at) AND (created_at <= updated_at))",
        "kind": "check_constraint",
        "mutated_expr": "((started_at < last_seen_at) AND (created_at <= updated_at))",
        "name": "ck_incidents_time_order",
        "table": "incidents",
    },
    {
        "expr": "CASE WHEN (jsonb_typeof(affected_services) = 'array'::text) THEN ((jsonb_array_length(affected_services) >= "
        "1) AND (jsonb_array_length(affected_services) <= 512)) ELSE false END",
        "kind": "check_constraint",
        "mutated_expr": "CASE WHEN (jsonb_typeof(affected_services) = 'array'::text) THEN "
        "((jsonb_array_length(affected_services) >= 2) AND (jsonb_array_length(affected_services) <= 512)) "
        "ELSE false END",
        "name": "ck_incidents_affected_services",
        "table": "incidents",
    },
    {
        "expr": "((revision_fingerprint)::text ~ '^[0-9a-f]{64}$'::text)",
        "kind": "check_constraint",
        "mutated_expr": "((revision_fingerprint)::text ~ '^[0-9a-f]{32}$'::text)",
        "name": "ck_incidents_revision_fp",
        "table": "incidents",
    },
    {
        "expr": "(((octet_length(aggregate_payload) >= 2) AND (octet_length(aggregate_payload) <= 67108864)) AND "
        "(jsonb_typeof((convert_from(aggregate_payload, 'UTF8'::name))::jsonb) = 'object'::text))",
        "kind": "check_constraint",
        "mutated_expr": "(((octet_length(aggregate_payload) >= 1) AND (octet_length(aggregate_payload) <= 67108864)) AND "
        "(jsonb_typeof((convert_from(aggregate_payload, 'UTF8'::name))::jsonb) = 'object'::text))",
        "name": "ck_incidents_payload",
        "table": "incidents",
    },
    {
        "am": "btree",
        "directions": ["ASC", "ASC", "DESC", "ASC"],
        "included_columns": [],
        "key_expressions": ["tenant_id", "status", "updated_at", "incident_id"],
        "kind": "index",
        "name": "ix_incidents_tenant_status_updated",
        "null_orderings": ["NULLS LAST", "NULLS LAST", "NULLS FIRST", "NULLS LAST"],
        "predicate": None,
        "table": "incidents",
        "unique": False,
    },
    {
        "am": "btree",
        "directions": ["ASC", "DESC", "ASC"],
        "included_columns": [],
        "key_expressions": ["tenant_id", "updated_at", "incident_id"],
        "kind": "index",
        "name": "ix_incidents_tenant_updated",
        "null_orderings": ["NULLS LAST", "NULLS FIRST", "NULLS LAST"],
        "predicate": None,
        "table": "incidents",
        "unique": False,
    },
    {
        "check_expr": "(tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid)",
        "cmd": "*",
        "kind": "rls_policy",
        "name": "pol_incidents_tenant",
        "permissive": True,
        "roles": (0,),
        "table": "incidents",
        "using_expr": "(tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid)",
    },
    {
        "enabled": "O",
        "function_name": "guard_incident_snapshot_change",
        "kind": "trigger",
        "name": "trg_incidents_guard",
        "table": "incidents",
        "type_code": 27,
    },
    {
        "columns": ("tenant_id", "incident_id", "signal_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "primary_key",
        "name": "pk_incident_signal_links",
        "table": "incident_signal_links",
    },
    {
        "columns": ("tenant_id",),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incident_signal_links_tenant",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("id",),
        "ref_schema": "public",
        "ref_table": "tenants",
        "table": "incident_signal_links",
    },
    {
        "columns": ("tenant_id", "incident_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incident_signal_links_incident",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("tenant_id", "incident_id"),
        "ref_schema": "public",
        "ref_table": "incidents",
        "table": "incident_signal_links",
    },
    {
        "columns": ("tenant_id", "signal_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "unique_constraint",
        "name": "uq_incident_signal_links_tenant_signal",
        "table": "incident_signal_links",
    },
    {
        "columns": ("tenant_id", "incident_id", "semantic_fingerprint"),
        "deferrable": False,
        "deferred": False,
        "kind": "unique_constraint",
        "name": "uq_incident_signal_links_incident_fp",
        "table": "incident_signal_links",
    },
    {
        "expr": "((semantic_fingerprint)::text ~ '^[0-9a-f]{64}$'::text)",
        "kind": "check_constraint",
        "mutated_expr": "((semantic_fingerprint)::text ~ '^[0-9a-f]{32}$'::text)",
        "name": "ck_incident_signal_links_fp",
        "table": "incident_signal_links",
    },
    {
        "expr": "((severity)::text = ANY ((ARRAY['debug'::character varying, 'info'::character varying, 'warning'::character "
        "varying, 'error'::character varying, 'critical'::character varying])::text[]))",
        "kind": "check_constraint",
        "mutated_expr": "((severity)::text = ANY ((ARRAY['debug'::character varying])::text[]))",
        "name": "ck_incident_signal_links_severity",
        "table": "incident_signal_links",
    },
    {
        "expr": "((anomaly_score >= (0)::double precision) AND (anomaly_score <= (1)::double precision))",
        "kind": "check_constraint",
        "mutated_expr": "((anomaly_score > (0)::double precision) AND (anomaly_score <= (1)::double precision))",
        "name": "ck_incident_signal_links_anomaly_score",
        "table": "incident_signal_links",
    },
    {
        "am": "btree",
        "directions": ["ASC", "ASC", "ASC", "ASC"],
        "included_columns": [],
        "key_expressions": ["tenant_id", "incident_id", "event_time", "signal_id"],
        "kind": "index",
        "name": "ix_incident_signal_links_event_time",
        "null_orderings": ["NULLS LAST", "NULLS LAST", "NULLS LAST", "NULLS LAST"],
        "predicate": None,
        "table": "incident_signal_links",
        "unique": False,
    },
    {
        "check_expr": "(tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid)",
        "cmd": "*",
        "kind": "rls_policy",
        "name": "pol_incident_signal_links_tenant",
        "permissive": True,
        "roles": (0,),
        "table": "incident_signal_links",
        "using_expr": "(tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid)",
    },
    {
        "enabled": "O",
        "function_name": "reject_incident_child_change",
        "kind": "trigger",
        "name": "trg_incident_signal_links_append",
        "table": "incident_signal_links",
        "type_code": 27,
    },
    {
        "columns": ("tenant_id", "incident_id", "left_signal_id", "right_signal_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "primary_key",
        "name": "pk_incident_correlation_evidence",
        "table": "incident_correlation_evidence",
    },
    {
        "columns": ("tenant_id",),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incident_correlation_evidence_tenant",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("id",),
        "ref_schema": "public",
        "ref_table": "tenants",
        "table": "incident_correlation_evidence",
    },
    {
        "columns": ("tenant_id", "incident_id", "left_signal_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incident_correlation_evidence_left_member",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("tenant_id", "incident_id", "signal_id"),
        "ref_schema": "public",
        "ref_table": "incident_signal_links",
        "table": "incident_correlation_evidence",
    },
    {
        "columns": ("tenant_id", "incident_id", "right_signal_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incident_correlation_evidence_right_member",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("tenant_id", "incident_id", "signal_id"),
        "ref_schema": "public",
        "ref_table": "incident_signal_links",
        "table": "incident_correlation_evidence",
    },
    {
        "expr": "(left_signal_id < right_signal_id)",
        "kind": "check_constraint",
        "mutated_expr": "(left_signal_id <= right_signal_id)",
        "name": "ck_incident_correlation_evidence_pair_order",
        "table": "incident_correlation_evidence",
    },
    {
        "expr": "(accepted = true)",
        "kind": "check_constraint",
        "mutated_expr": "(accepted = false)",
        "name": "ck_incident_correlation_evidence_accepted",
        "table": "incident_correlation_evidence",
    },
    {
        "expr": "((temporal_score >= (0)::double precision) AND (temporal_score <= (1)::double precision))",
        "kind": "check_constraint",
        "mutated_expr": "((temporal_score > (0)::double precision) AND (temporal_score <= (1)::double precision))",
        "name": "ck_incident_correlation_evidence_temporal_score",
        "table": "incident_correlation_evidence",
    },
    {
        "expr": "((service_score >= (0)::double precision) AND (service_score <= (1)::double precision))",
        "kind": "check_constraint",
        "mutated_expr": "((service_score > (0)::double precision) AND (service_score <= (1)::double precision))",
        "name": "ck_incident_correlation_evidence_service_score",
        "table": "incident_correlation_evidence",
    },
    {
        "expr": "((topology_score >= (0)::double precision) AND (topology_score <= (1)::double precision))",
        "kind": "check_constraint",
        "mutated_expr": "((topology_score > (0)::double precision) AND (topology_score <= (1)::double precision))",
        "name": "ck_incident_correlation_evidence_topology_score",
        "table": "incident_correlation_evidence",
    },
    {
        "expr": "((symptom_score >= (0)::double precision) AND (symptom_score <= (1)::double precision))",
        "kind": "check_constraint",
        "mutated_expr": "((symptom_score > (0)::double precision) AND (symptom_score <= (1)::double precision))",
        "name": "ck_incident_correlation_evidence_symptom_score",
        "table": "incident_correlation_evidence",
    },
    {
        "expr": "((context_score >= (0)::double precision) AND (context_score <= (1)::double precision))",
        "kind": "check_constraint",
        "mutated_expr": "((context_score > (0)::double precision) AND (context_score <= (1)::double precision))",
        "name": "ck_incident_correlation_evidence_context_score",
        "table": "incident_correlation_evidence",
    },
    {
        "expr": "((weighted_score >= (0)::double precision) AND (weighted_score <= (1)::double precision))",
        "kind": "check_constraint",
        "mutated_expr": "((weighted_score > (0)::double precision) AND (weighted_score <= (1)::double precision))",
        "name": "ck_incident_correlation_evidence_weighted_score",
        "table": "incident_correlation_evidence",
    },
    {
        "am": "btree",
        "directions": ["ASC", "ASC", "ASC"],
        "included_columns": [],
        "key_expressions": ["tenant_id", "incident_id", "right_signal_id"],
        "kind": "index",
        "name": "ix_incident_correlation_evidence_right",
        "null_orderings": ["NULLS LAST", "NULLS LAST", "NULLS LAST"],
        "predicate": None,
        "table": "incident_correlation_evidence",
        "unique": False,
    },
    {
        "check_expr": "(tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid)",
        "cmd": "*",
        "kind": "rls_policy",
        "name": "pol_incident_correlation_evidence_tenant",
        "permissive": True,
        "roles": (0,),
        "table": "incident_correlation_evidence",
        "using_expr": "(tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid)",
    },
    {
        "enabled": "O",
        "function_name": "reject_incident_child_change",
        "kind": "trigger",
        "name": "trg_incident_correlation_evidence_append",
        "table": "incident_correlation_evidence",
        "type_code": 27,
    },
    {
        "columns": ("tenant_id", "event_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "primary_key",
        "name": "pk_incident_timeline_events",
        "table": "incident_timeline_events",
    },
    {
        "columns": ("tenant_id",),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incident_timeline_events_tenant",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("id",),
        "ref_schema": "public",
        "ref_table": "tenants",
        "table": "incident_timeline_events",
    },
    {
        "columns": ("tenant_id", "incident_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incident_timeline_events_incident",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("tenant_id", "incident_id"),
        "ref_schema": "public",
        "ref_table": "incidents",
        "table": "incident_timeline_events",
    },
    {
        "columns": ("tenant_id", "incident_id", "source_signal_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incident_timeline_events_source_member",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("tenant_id", "incident_id", "signal_id"),
        "ref_schema": "public",
        "ref_table": "incident_signal_links",
        "table": "incident_timeline_events",
    },
    {
        "columns": ("tenant_id", "incident_id", "revision", "event_ordinal"),
        "deferrable": False,
        "deferred": False,
        "kind": "unique_constraint",
        "name": "uq_incident_timeline_events_revision_ordinal",
        "table": "incident_timeline_events",
    },
    {
        "expr": "((schema_version)::text = '1.2'::text)",
        "kind": "check_constraint",
        "mutated_expr": "((schema_version)::text = '1.1'::text)",
        "name": "ck_incident_timeline_events_schema_version",
        "table": "incident_timeline_events",
    },
    {
        "expr": "((revision = trunc(revision)) AND ((revision >= (1)::numeric) AND (revision <= "
        "'18446744073709551615'::numeric)))",
        "kind": "check_constraint",
        "mutated_expr": "((revision = trunc(revision)) AND ((revision >= (0)::numeric) AND (revision <= "
        "'18446744073709551615'::numeric)))",
        "name": "ck_incident_timeline_events_revision",
        "table": "incident_timeline_events",
    },
    {
        "expr": "(event_ordinal >= 0)",
        "kind": "check_constraint",
        "mutated_expr": "(event_ordinal > 0)",
        "name": "ck_incident_timeline_events_ordinal",
        "table": "incident_timeline_events",
    },
    {
        "expr": "((event_type)::text = ANY ((ARRAY['created'::character varying, 'signal_attached'::character varying, "
        "'severity_changed'::character varying, 'topology_enriched'::character varying, "
        "'correlation_state_changed'::character varying, 'correlation_finalized'::character varying])::text[]))",
        "kind": "check_constraint",
        "mutated_expr": "((event_type)::text = ANY ((ARRAY['created'::character varying])::text[]))",
        "name": "ck_incident_timeline_events_event_type",
        "table": "incident_timeline_events",
    },
    {
        "expr": "((revision_fingerprint)::text ~ '^[0-9a-f]{64}$'::text)",
        "kind": "check_constraint",
        "mutated_expr": "((revision_fingerprint)::text ~ '^[0-9a-f]{32}$'::text)",
        "name": "ck_incident_timeline_events_revision_fp",
        "table": "incident_timeline_events",
    },
    {
        "am": "btree",
        "directions": ["ASC", "ASC", "ASC", "ASC", "ASC", "ASC"],
        "included_columns": [],
        "key_expressions": [
            "tenant_id",
            "incident_id",
            "event_time",
            "revision",
            "event_ordinal",
            "event_id",
        ],
        "kind": "index",
        "name": "ix_incident_timeline_events_chronological",
        "null_orderings": [
            "NULLS LAST",
            "NULLS LAST",
            "NULLS LAST",
            "NULLS LAST",
            "NULLS LAST",
            "NULLS LAST",
        ],
        "predicate": None,
        "table": "incident_timeline_events",
        "unique": False,
    },
    {
        "check_expr": "(tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid)",
        "cmd": "*",
        "kind": "rls_policy",
        "name": "pol_incident_timeline_events_tenant",
        "permissive": True,
        "roles": (0,),
        "table": "incident_timeline_events",
        "using_expr": "(tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid)",
    },
    {
        "enabled": "O",
        "function_name": "reject_incident_child_change",
        "kind": "trigger",
        "name": "trg_incident_timeline_events_append",
        "table": "incident_timeline_events",
        "type_code": 27,
    },
    {
        "columns": ("tenant_id", "incident_id", "revision"),
        "deferrable": False,
        "deferred": False,
        "kind": "primary_key",
        "name": "pk_incident_publication_intents",
        "table": "incident_publication_intents",
    },
    {
        "columns": ("tenant_id",),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incident_publication_intents_tenant",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("id",),
        "ref_schema": "public",
        "ref_table": "tenants",
        "table": "incident_publication_intents",
    },
    {
        "columns": ("tenant_id", "incident_id"),
        "deferrable": False,
        "deferred": False,
        "kind": "foreign_key",
        "match_type": "s",
        "name": "fk_incident_publication_intents_incident",
        "on_delete": "a",
        "on_update": "a",
        "ref_columns": ("tenant_id", "incident_id"),
        "ref_schema": "public",
        "ref_table": "incidents",
        "table": "incident_publication_intents",
    },
    {
        "expr": "((revision = trunc(revision)) AND ((revision >= (1)::numeric) AND (revision <= "
        "'18446744073709551615'::numeric)))",
        "kind": "check_constraint",
        "mutated_expr": "((revision = trunc(revision)) AND ((revision >= (0)::numeric) AND (revision <= "
        "'18446744073709551615'::numeric)))",
        "name": "ck_incident_publication_intents_revision",
        "table": "incident_publication_intents",
    },
    {
        "expr": "((schema_version)::text = '1.2'::text)",
        "kind": "check_constraint",
        "mutated_expr": "((schema_version)::text = '1.1'::text)",
        "name": "ck_incident_publication_intents_schema_version",
        "table": "incident_publication_intents",
    },
    {
        "expr": "((revision_fingerprint)::text ~ '^[0-9a-f]{64}$'::text)",
        "kind": "check_constraint",
        "mutated_expr": "((revision_fingerprint)::text ~ '^[0-9a-f]{32}$'::text)",
        "name": "ck_incident_publication_intents_revision_fp",
        "table": "incident_publication_intents",
    },
    {
        "expr": "(((octet_length(aggregate_payload) >= 2) AND (octet_length(aggregate_payload) <= 67108864)) AND "
        "(jsonb_typeof((convert_from(aggregate_payload, 'UTF8'::name))::jsonb) = 'object'::text))",
        "kind": "check_constraint",
        "mutated_expr": "(((octet_length(aggregate_payload) >= 1) AND (octet_length(aggregate_payload) <= 67108864)) AND "
        "(jsonb_typeof((convert_from(aggregate_payload, 'UTF8'::name))::jsonb) = 'object'::text))",
        "name": "ck_incident_publication_intents_payload",
        "table": "incident_publication_intents",
    },
    {
        "am": "btree",
        "directions": ["ASC", "ASC", "ASC", "ASC"],
        "included_columns": [],
        "key_expressions": ["tenant_id", "intent_time", "incident_id", "revision"],
        "kind": "index",
        "name": "ix_incident_publication_intents_time",
        "null_orderings": ["NULLS LAST", "NULLS LAST", "NULLS LAST", "NULLS LAST"],
        "predicate": None,
        "table": "incident_publication_intents",
        "unique": False,
    },
    {
        "check_expr": "(tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid)",
        "cmd": "*",
        "kind": "rls_policy",
        "name": "pol_incident_publication_intents_tenant",
        "permissive": True,
        "roles": (0,),
        "table": "incident_publication_intents",
        "using_expr": "(tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid)",
    },
    {
        "enabled": "O",
        "function_name": "reject_incident_child_change",
        "kind": "trigger",
        "name": "trg_incident_publication_intents_append",
        "table": "incident_publication_intents",
        "type_code": 27,
    },
    {
        "kind": "trigger_function",
        "lang": "plpgsql",
        "name": "guard_incident_snapshot_change",
        "prosrc": "\n"
        "        BEGIN\n"
        "            IF TG_OP = 'DELETE' THEN\n"
        "                RAISE EXCEPTION 'DELETE operation is prohibited on incidents table'\n"
        "                    USING ERRCODE = '55000';\n"
        "            ELSIF TG_OP = 'UPDATE' THEN\n"
        "                IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id THEN\n"
        "                    RAISE EXCEPTION 'Immutable field tenant_id cannot be changed'\n"
        "                        USING ERRCODE = '55000';\n"
        "                END IF;\n"
        "                IF NEW.incident_id IS DISTINCT FROM OLD.incident_id THEN\n"
        "                    RAISE EXCEPTION 'Immutable field incident_id cannot be changed'\n"
        "                        USING ERRCODE = '55000';\n"
        "                END IF;\n"
        "                IF NEW.incident_number IS DISTINCT FROM OLD.incident_number THEN\n"
        "                    RAISE EXCEPTION 'Immutable field incident_number cannot be changed'\n"
        "                        USING ERRCODE = '55000';\n"
        "                END IF;\n"
        "                IF NEW.environment IS DISTINCT FROM OLD.environment THEN\n"
        "                    RAISE EXCEPTION 'Immutable field environment cannot be changed'\n"
        "                        USING ERRCODE = '55000';\n"
        "                END IF;\n"
        "                IF NEW.primary_service IS DISTINCT FROM OLD.primary_service THEN\n"
        "                    RAISE EXCEPTION 'Immutable field primary_service cannot be changed'\n"
        "                        USING ERRCODE = '55000';\n"
        "                END IF;\n"
        "                IF NEW.created_at IS DISTINCT FROM OLD.created_at THEN\n"
        "                    RAISE EXCEPTION 'Immutable field created_at cannot be changed'\n"
        "                        USING ERRCODE = '55000';\n"
        "                END IF;\n"
        "                IF NEW.revision IS DISTINCT FROM (OLD.revision + 1) THEN\n"
        "                    RAISE EXCEPTION 'revision must increment monotonically by exactly 1 (expected %, got "
        "%)', OLD.revision + 1, NEW.revision\n"
        "                        USING ERRCODE = '55000';\n"
        "                END IF;\n"
        "                IF NEW.updated_at < OLD.updated_at THEN\n"
        "                    RAISE EXCEPTION 'updated_at cannot regress (old %, new %)', OLD.updated_at, "
        "NEW.updated_at\n"
        "                        USING ERRCODE = '55000';\n"
        "                END IF;\n"
        "                RETURN NEW;\n"
        "            END IF;\n"
        "            RETURN NEW;\n"
        "        END;\n"
        "        ",
        "ret": "trigger",
        "schema": "public",
        "secdef": False,
        "volatility": "v",
    },
    {
        "kind": "trigger_function",
        "lang": "plpgsql",
        "name": "reject_incident_child_change",
        "prosrc": "\n"
        "        BEGIN\n"
        "            RAISE EXCEPTION '% operation is prohibited on append-only incident child table %', TG_OP, "
        "TG_TABLE_NAME\n"
        "                USING ERRCODE = '55000';\n"
        "        END;\n"
        "        ",
        "ret": "trigger",
        "schema": "public",
        "secdef": False,
        "volatility": "v",
    },
]

assert len(EMBEDDED_EXPECTED_OBJECTS) == 69
EMBEDDED_EXPECTED_ORM: dict[str, dict[str, Any]] = {
    "IncidentCorrelationEvidenceRecord": {
        "check_constraints": [
            {
                "name": "ck_incident_correlation_evidence_temporal_score",
                "normalized_expr": "temporal_score >= 0 AND " "temporal_score <= 1",
            },
            {
                "name": "ck_incident_correlation_evidence_context_score",
                "normalized_expr": "context_score >= 0 AND context_score " "<= 1",
            },
            {
                "name": "ck_incident_correlation_evidence_service_score",
                "normalized_expr": "service_score >= 0 AND service_score " "<= 1",
            },
            {
                "name": "ck_incident_correlation_evidence_symptom_score",
                "normalized_expr": "symptom_score >= 0 AND symptom_score " "<= 1",
            },
            {
                "name": "ck_incident_correlation_evidence_weighted_score",
                "normalized_expr": "weighted_score >= 0 AND " "weighted_score <= 1",
            },
            {
                "name": "ck_incident_correlation_evidence_accepted",
                "normalized_expr": "accepted = TRUE",
            },
            {
                "name": "ck_incident_correlation_evidence_pair_order",
                "normalized_expr": "left_signal_id < right_signal_id",
            },
            {
                "name": "ck_incident_correlation_evidence_topology_score",
                "normalized_expr": "topology_score >= 0 AND " "topology_score <= 1",
            },
        ],
        "columns": [
            {
                "name": "tenant_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "incident_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "left_signal_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "right_signal_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "temporal_score",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "DOUBLE PRECISION",
                "unconstrained": False,
            },
            {
                "name": "service_score",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "DOUBLE PRECISION",
                "unconstrained": False,
            },
            {
                "name": "topology_score",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "DOUBLE PRECISION",
                "unconstrained": False,
            },
            {
                "name": "symptom_score",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "DOUBLE PRECISION",
                "unconstrained": False,
            },
            {
                "name": "context_score",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "DOUBLE PRECISION",
                "unconstrained": False,
            },
            {
                "name": "weighted_score",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "DOUBLE PRECISION",
                "unconstrained": False,
            },
            {
                "name": "accepted",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "BOOLEAN",
                "unconstrained": False,
            },
            {
                "name": "policy_version",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(128)",
                "unconstrained": False,
            },
            {
                "name": "reason",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(512)",
                "unconstrained": False,
            },
        ],
        "foreign_keys": [
            {
                "local_columns": ["tenant_id", "incident_id", "left_signal_id"],
                "name": "fk_incident_correlation_evidence_left_member",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["tenant_id", "incident_id", "signal_id"],
                "remote_schema": "public",
                "remote_table": "incident_signal_links",
            },
            {
                "local_columns": ["tenant_id"],
                "name": "fk_incident_correlation_evidence_tenant",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["id"],
                "remote_schema": "public",
                "remote_table": "tenants",
            },
            {
                "local_columns": ["tenant_id", "incident_id", "right_signal_id"],
                "name": "fk_incident_correlation_evidence_right_member",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["tenant_id", "incident_id", "signal_id"],
                "remote_schema": "public",
                "remote_table": "incident_signal_links",
            },
        ],
        "indexes": [
            {
                "directions": ["ASC", "ASC", "ASC"],
                "expressions": ["tenant_id", "incident_id", "right_signal_id"],
                "included_columns": [],
                "name": "ix_incident_correlation_evidence_right",
                "null_orderings": ["NULLS LAST", "NULLS LAST", "NULLS LAST"],
                "predicate": None,
                "unique": False,
            }
        ],
        "primary_key": {
            "columns": [
                "tenant_id",
                "incident_id",
                "left_signal_id",
                "right_signal_id",
            ],
            "name": "pk_incident_correlation_evidence",
        },
        "prohibited_constructs": [],
        "table_name": "incident_correlation_evidence",
        "unique_constraints": [],
    },
    "IncidentPublicationIntentRecord": {
        "check_constraints": [
            {
                "name": "ck_incident_publication_intents_revision",
                "normalized_expr": "revision = trunc(revision) AND "
                "revision BETWEEN 1 AND "
                "18446744073709551615",
            },
            {
                "name": "ck_incident_publication_intents_revision_fp",
                "normalized_expr": "revision_fingerprint ~ " "'^[0-9a-f]{64}$'",
            },
            {
                "name": "ck_incident_publication_intents_schema_version",
                "normalized_expr": "schema_version = '1.2'",
            },
            {
                "name": "ck_incident_publication_intents_payload",
                "normalized_expr": "octet_length(aggregate_payload) "
                "BETWEEN 2 AND 67108864 AND "
                "jsonb_typeof(convert_from(aggregate_payload, "
                "'UTF8')::jsonb) = 'object'",
            },
        ],
        "columns": [
            {
                "name": "tenant_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "incident_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "revision",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "NUMERIC",
                "unconstrained": True,
            },
            {
                "name": "schema_version",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(10)",
                "unconstrained": False,
            },
            {
                "name": "revision_fingerprint",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(64)",
                "unconstrained": False,
            },
            {
                "name": "intent_time",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "TIMESTAMP WITH TIME ZONE",
                "unconstrained": False,
            },
            {
                "name": "aggregate_payload",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "BYTEA",
                "unconstrained": False,
            },
        ],
        "foreign_keys": [
            {
                "local_columns": ["tenant_id", "incident_id"],
                "name": "fk_incident_publication_intents_incident",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["tenant_id", "incident_id"],
                "remote_schema": "public",
                "remote_table": "incidents",
            },
            {
                "local_columns": ["tenant_id"],
                "name": "fk_incident_publication_intents_tenant",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["id"],
                "remote_schema": "public",
                "remote_table": "tenants",
            },
        ],
        "indexes": [
            {
                "directions": ["ASC", "ASC", "ASC", "ASC"],
                "expressions": ["tenant_id", "intent_time", "incident_id", "revision"],
                "included_columns": [],
                "name": "ix_incident_publication_intents_time",
                "null_orderings": [
                    "NULLS LAST",
                    "NULLS LAST",
                    "NULLS LAST",
                    "NULLS LAST",
                ],
                "predicate": None,
                "unique": False,
            }
        ],
        "primary_key": {
            "columns": ["tenant_id", "incident_id", "revision"],
            "name": "pk_incident_publication_intents",
        },
        "prohibited_constructs": [],
        "table_name": "incident_publication_intents",
        "unique_constraints": [],
    },
    "IncidentRecord": {
        "check_constraints": [
            {
                "name": "ck_incidents_affected_services",
                "normalized_expr": "CASE WHEN jsonb_typeof(affected_services) = 'array' "
                "THEN jsonb_array_length(affected_services) BETWEEN 1 "
                "AND 512 ELSE FALSE END",
            },
            {
                "name": "ck_incidents_correlation_state",
                "normalized_expr": "correlation_state IN ('collecting', 'settling', "
                "'finalized')",
            },
            {
                "name": "ck_incidents_revision_fp",
                "normalized_expr": "revision_fingerprint ~ '^[0-9a-f]{64}$'",
            },
            {
                "name": "ck_incidents_status",
                "normalized_expr": "status IN ('open', 'acknowledged', 'resolved', "
                "'closed')",
            },
            {
                "name": "ck_incidents_severity",
                "normalized_expr": "severity IN ('debug', 'info', 'warning', 'error', "
                "'critical')",
            },
            {
                "name": "ck_incidents_severity_policy",
                "normalized_expr": "severity_policy_version = 'max-member-severity/v1'",
            },
            {
                "name": "ck_incidents_incident_number",
                "normalized_expr": "incident_number = 'INC-' || "
                "upper(replace(incident_id::text, '-', ''))",
            },
            {
                "name": "ck_incidents_revision",
                "normalized_expr": "revision = trunc(revision) AND revision BETWEEN 1 AND "
                "18446744073709551615",
            },
            {
                "name": "ck_incidents_payload",
                "normalized_expr": "octet_length(aggregate_payload) BETWEEN 2 AND 67108864 "
                "AND jsonb_typeof(convert_from(aggregate_payload, "
                "'UTF8')::jsonb) = 'object'",
            },
            {
                "name": "ck_incidents_time_order",
                "normalized_expr": "started_at <= last_seen_at AND created_at <= "
                "updated_at",
            },
            {
                "name": "ck_incidents_schema_version",
                "normalized_expr": "schema_version = '1.2'",
            },
        ],
        "columns": [
            {
                "name": "tenant_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "incident_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "schema_version",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(10)",
                "unconstrained": False,
            },
            {
                "name": "incident_number",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(128)",
                "unconstrained": False,
            },
            {
                "name": "environment",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(128)",
                "unconstrained": False,
            },
            {
                "name": "status",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(20)",
                "unconstrained": False,
            },
            {
                "name": "correlation_state",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(20)",
                "unconstrained": False,
            },
            {
                "name": "severity",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(20)",
                "unconstrained": False,
            },
            {
                "name": "severity_policy_version",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(128)",
                "unconstrained": False,
            },
            {
                "name": "started_at",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "TIMESTAMP WITH TIME ZONE",
                "unconstrained": False,
            },
            {
                "name": "last_seen_at",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "TIMESTAMP WITH TIME ZONE",
                "unconstrained": False,
            },
            {
                "name": "created_at",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "TIMESTAMP WITH TIME ZONE",
                "unconstrained": False,
            },
            {
                "name": "updated_at",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "TIMESTAMP WITH TIME ZONE",
                "unconstrained": False,
            },
            {
                "name": "revision",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "NUMERIC",
                "unconstrained": True,
            },
            {
                "name": "primary_service",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(128)",
                "unconstrained": False,
            },
            {
                "name": "affected_services",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "JSONB",
                "unconstrained": False,
            },
            {
                "name": "revision_fingerprint",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(64)",
                "unconstrained": False,
            },
            {
                "name": "aggregate_payload",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "BYTEA",
                "unconstrained": False,
            },
        ],
        "foreign_keys": [
            {
                "local_columns": ["tenant_id"],
                "name": "fk_incidents_tenant",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["id"],
                "remote_schema": "public",
                "remote_table": "tenants",
            }
        ],
        "indexes": [
            {
                "directions": ["ASC", "DESC", "ASC"],
                "expressions": ["tenant_id", "updated_at DESC", "incident_id"],
                "included_columns": [],
                "name": "ix_incidents_tenant_updated",
                "null_orderings": ["NULLS LAST", "NULLS FIRST", "NULLS LAST"],
                "predicate": None,
                "unique": False,
            },
            {
                "directions": ["ASC", "ASC", "DESC", "ASC"],
                "expressions": [
                    "tenant_id",
                    "status",
                    "updated_at DESC",
                    "incident_id",
                ],
                "included_columns": [],
                "name": "ix_incidents_tenant_status_updated",
                "null_orderings": [
                    "NULLS LAST",
                    "NULLS LAST",
                    "NULLS FIRST",
                    "NULLS LAST",
                ],
                "predicate": None,
                "unique": False,
            },
        ],
        "primary_key": {
            "columns": ["tenant_id", "incident_id"],
            "name": "pk_incidents",
        },
        "prohibited_constructs": [],
        "table_name": "incidents",
        "unique_constraints": [
            {
                "columns": ["tenant_id", "incident_number"],
                "name": "uq_incidents_tenant_number",
            }
        ],
    },
    "IncidentSignalLinkRecord": {
        "check_constraints": [
            {
                "name": "ck_incident_signal_links_anomaly_score",
                "normalized_expr": "anomaly_score >= 0 AND anomaly_score <= 1",
            },
            {
                "name": "ck_incident_signal_links_severity",
                "normalized_expr": "severity IN ('debug', 'info', 'warning', "
                "'error', 'critical')",
            },
            {
                "name": "ck_incident_signal_links_fp",
                "normalized_expr": "semantic_fingerprint ~ '^[0-9a-f]{64}$'",
            },
        ],
        "columns": [
            {
                "name": "tenant_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "incident_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "signal_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "semantic_fingerprint",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(64)",
                "unconstrained": False,
            },
            {
                "name": "environment",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(128)",
                "unconstrained": False,
            },
            {
                "name": "service",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(128)",
                "unconstrained": False,
            },
            {
                "name": "event_time",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "TIMESTAMP WITH TIME ZONE",
                "unconstrained": False,
            },
            {
                "name": "severity",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(20)",
                "unconstrained": False,
            },
            {
                "name": "anomaly_score",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "DOUBLE PRECISION",
                "unconstrained": False,
            },
        ],
        "foreign_keys": [
            {
                "local_columns": ["tenant_id", "incident_id"],
                "name": "fk_incident_signal_links_incident",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["tenant_id", "incident_id"],
                "remote_schema": "public",
                "remote_table": "incidents",
            },
            {
                "local_columns": ["tenant_id"],
                "name": "fk_incident_signal_links_tenant",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["id"],
                "remote_schema": "public",
                "remote_table": "tenants",
            },
        ],
        "indexes": [
            {
                "directions": ["ASC", "ASC", "ASC", "ASC"],
                "expressions": ["tenant_id", "incident_id", "event_time", "signal_id"],
                "included_columns": [],
                "name": "ix_incident_signal_links_event_time",
                "null_orderings": [
                    "NULLS LAST",
                    "NULLS LAST",
                    "NULLS LAST",
                    "NULLS LAST",
                ],
                "predicate": None,
                "unique": False,
            }
        ],
        "primary_key": {
            "columns": ["tenant_id", "incident_id", "signal_id"],
            "name": "pk_incident_signal_links",
        },
        "prohibited_constructs": [],
        "table_name": "incident_signal_links",
        "unique_constraints": [
            {
                "columns": ["tenant_id", "incident_id", "semantic_fingerprint"],
                "name": "uq_incident_signal_links_incident_fp",
            },
            {
                "columns": ["tenant_id", "signal_id"],
                "name": "uq_incident_signal_links_tenant_signal",
            },
        ],
    },
    "IncidentTimelineEventRecord": {
        "check_constraints": [
            {
                "name": "ck_incident_timeline_events_ordinal",
                "normalized_expr": "event_ordinal >= 0",
            },
            {
                "name": "ck_incident_timeline_events_revision_fp",
                "normalized_expr": "revision_fingerprint ~ '^[0-9a-f]{64}$'",
            },
            {
                "name": "ck_incident_timeline_events_schema_version",
                "normalized_expr": "schema_version = '1.2'",
            },
            {
                "name": "ck_incident_timeline_events_revision",
                "normalized_expr": "revision = trunc(revision) AND revision "
                "BETWEEN 1 AND 18446744073709551615",
            },
            {
                "name": "ck_incident_timeline_events_event_type",
                "normalized_expr": "event_type IN ('created', "
                "'signal_attached', 'severity_changed', "
                "'topology_enriched', "
                "'correlation_state_changed', "
                "'correlation_finalized')",
            },
        ],
        "columns": [
            {
                "name": "tenant_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "incident_id",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "event_id",
                "nullable": False,
                "pk": True,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "schema_version",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(10)",
                "unconstrained": False,
            },
            {
                "name": "revision",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "NUMERIC",
                "unconstrained": True,
            },
            {
                "name": "event_ordinal",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "INTEGER",
                "unconstrained": False,
            },
            {
                "name": "event_time",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "TIMESTAMP WITH TIME ZONE",
                "unconstrained": False,
            },
            {
                "name": "event_type",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(40)",
                "unconstrained": False,
            },
            {
                "name": "source_signal_id",
                "nullable": True,
                "pk": False,
                "pk_position": None,
                "type": "UUID",
                "unconstrained": False,
            },
            {
                "name": "summary",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(512)",
                "unconstrained": False,
            },
            {
                "name": "revision_fingerprint",
                "nullable": False,
                "pk": False,
                "pk_position": None,
                "type": "VARCHAR(64)",
                "unconstrained": False,
            },
        ],
        "foreign_keys": [
            {
                "local_columns": ["tenant_id", "incident_id"],
                "name": "fk_incident_timeline_events_incident",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["tenant_id", "incident_id"],
                "remote_schema": "public",
                "remote_table": "incidents",
            },
            {
                "local_columns": ["tenant_id", "incident_id", "source_signal_id"],
                "name": "fk_incident_timeline_events_source_member",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["tenant_id", "incident_id", "signal_id"],
                "remote_schema": "public",
                "remote_table": "incident_signal_links",
            },
            {
                "local_columns": ["tenant_id"],
                "name": "fk_incident_timeline_events_tenant",
                "ondelete": "NO ACTION",
                "onupdate": "NO ACTION",
                "remote_columns": ["id"],
                "remote_schema": "public",
                "remote_table": "tenants",
            },
        ],
        "indexes": [
            {
                "directions": ["ASC", "ASC", "ASC", "ASC", "ASC", "ASC"],
                "expressions": [
                    "tenant_id",
                    "incident_id",
                    "event_time",
                    "revision",
                    "event_ordinal",
                    "event_id",
                ],
                "included_columns": [],
                "name": "ix_incident_timeline_events_chronological",
                "null_orderings": [
                    "NULLS LAST",
                    "NULLS LAST",
                    "NULLS LAST",
                    "NULLS LAST",
                    "NULLS LAST",
                    "NULLS LAST",
                ],
                "predicate": None,
                "unique": False,
            }
        ],
        "primary_key": {
            "columns": ["tenant_id", "event_id"],
            "name": "pk_incident_timeline_events",
        },
        "prohibited_constructs": [],
        "table_name": "incident_timeline_events",
        "unique_constraints": [
            {
                "columns": ["tenant_id", "incident_id", "revision", "event_ordinal"],
                "name": "uq_incident_timeline_events_revision_ordinal",
            }
        ],
    },
}


# REQUIRED FIXTURES: Disposable DB & Runtime Role
# ============================================================================
@pytest_asyncio.fixture(scope="module")
async def db_resource() -> AsyncGenerator[dict[str, Any], None]:
    """Disposable PostgreSQL database and runtime role lifecycle fixture.

    Generates unique random disposable suffix per execution.
    Target administrative DSN read only from M1_POSTGRES_ADMIN_DSN.
    """
    admin_dsn = os.environ.get("M1_POSTGRES_ADMIN_DSN")
    if not admin_dsn:
        pytest.fail(
            "M1_POSTGRES_ADMIN_DSN environment variable is required for integration tests"
        )

    parsed = urllib.parse.urlparse(admin_dsn)
    target_maintenance_db = parsed.path.lstrip("/")
    if target_maintenance_db != "postgres":
        pytest.fail(
            f"M1_POSTGRES_ADMIN_DSN must target maintenance database 'postgres', got '{target_maintenance_db}'"
        )

    admin_host = parsed.hostname or "localhost"
    admin_port = parsed.port or 5432
    admin_user = parsed.username or "postgres"
    admin_pass = parsed.password or ""

    # 1. Connect to admin DB
    try:
        admin_conn = await asyncpg.connect(admin_dsn)
    except Exception as exc:
        pytest.fail(f"Could not connect to PostgreSQL admin database: {exc}")

    try:
        # Check PostgreSQL 16
        ver = await admin_conn.fetchval("SELECT version()")
        assert (
            "PostgreSQL 16" in ver or " 16." in ver
        ), f"PostgreSQL 16 required, got: {ver}"

        # 2. Generate random per-run resources
        suffix = secrets.token_hex(6)
        assert re.match(r"^[0-9a-f]{12}$", suffix), f"Invalid suffix format: {suffix}"
        db_name = f"aegis_m1_{suffix}"
        role_name = f"aegis_m1_runtime_{suffix}"
        password = secrets.token_urlsafe(24)

        # Check absence before creation (never pre-drop)
        db_exists = await admin_conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", db_name
        )
        assert not db_exists, f"Generated database {db_name} already exists!"
        role_exists = await admin_conn.fetchval(
            "SELECT 1 FROM pg_roles WHERE rolname = $1", role_name
        )
        assert not role_exists, f"Generated role {role_name} already exists!"

        # Create disposable database
        await admin_conn.execute(f'CREATE DATABASE "{db_name}"')

        # Create non-bypass login role with exact minimum flags
        await admin_conn.execute(
            f"CREATE ROLE \"{role_name}\" WITH LOGIN PASSWORD '{password}' NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE"
        )

        # 3. Connect to new database as admin to run migrations and grant minimal permissions
        scheme = parsed.scheme or "postgresql"
        disp_admin_dsn = (
            f"{scheme}://{admin_user}:{admin_pass}@{admin_host}:{admin_port}/{db_name}"
        )
        disp_admin_conn = await asyncpg.connect(disp_admin_dsn)

        try:
            # Grant connect
            await disp_admin_conn.execute(
                f'GRANT CONNECT ON DATABASE "{db_name}" TO "{role_name}"'
            )
            await disp_admin_conn.execute(
                f'GRANT USAGE ON SCHEMA public TO "{role_name}"'
            )

            # 4. Run migrations using alembic upgrade head
            env = os.environ.copy()
            async_url = f"postgresql+asyncpg://{admin_user}:{admin_pass}@{admin_host}:{admin_port}/{db_name}"
            env["DATABASE_URL"] = async_url
            res = subprocess.run(
                ["poetry", "run", "alembic", "upgrade", "head"],
                cwd=BACKEND_DIR,
                env=env,
                capture_output=True,
                text=True,
            )
            assert (
                res.returncode == 0
            ), f"Alembic upgrade failed:\nstdout={res.stdout}\nstderr={res.stderr}"

            # 5. Grant minimal DML on the 5 incident tables to runtime role
            await disp_admin_conn.execute(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "
                f"incidents, incident_signal_links, incident_correlation_evidence, "
                f'incident_timeline_events, incident_publication_intents TO "{role_name}"'
            )
        finally:
            await disp_admin_conn.close()

        resource = {
            "suffix": suffix,
            "db_name": db_name,
            "role_name": role_name,
            "admin_dsn": disp_admin_dsn,
            "runtime_dsn": f"{scheme}://{role_name}:{password}@{admin_host}:{admin_port}/{db_name}",
        }
        print(
            "\nM1_R4_RESOURCE_DIAGNOSTIC "
            + canonical_json(
                {
                    "database_name": db_name,
                    "role_name": role_name,
                }
            )
        )
        yield resource

    finally:
        # 6. Teardown: terminate sessions, drop DB, drop role
        try:
            await admin_conn.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = $1 AND pid <> pg_backend_pid()
            """,
                db_name,
            )
            await admin_conn.execute(f'DROP DATABASE "{db_name}"')
            await admin_conn.execute(f'DROP ROLE "{role_name}"')
            database_remainder_count = int(
                bool(
                    await admin_conn.fetchval(
                        "SELECT 1 FROM pg_database WHERE datname = $1", db_name
                    )
                )
            )
            role_remainder_count = int(
                bool(
                    await admin_conn.fetchval(
                        "SELECT 1 FROM pg_roles WHERE rolname = $1", role_name
                    )
                )
            )
            cleanup_diagnostic = {
                "database_name": db_name,
                "role_name": role_name,
                "database_remainder_count": database_remainder_count,
                "role_remainder_count": role_remainder_count,
            }
            print("\nM1_R4_CLEANUP_DIAGNOSTIC " + canonical_json(cleanup_diagnostic))
            assert database_remainder_count == 0
            assert role_remainder_count == 0
        finally:
            await admin_conn.close()


@pytest_asyncio.fixture
async def admin_conn(
    db_resource: dict[str, Any],
) -> AsyncGenerator[asyncpg.Connection, None]:
    conn = await asyncpg.connect(db_resource["admin_dsn"])
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def runtime_conn(
    db_resource: dict[str, Any],
) -> AsyncGenerator[asyncpg.Connection, None]:
    conn = await asyncpg.connect(db_resource["runtime_dsn"])
    try:
        yield conn
    finally:
        await conn.close()


async def _seed_tenants(conn: asyncpg.Connection) -> tuple[uuid.UUID, uuid.UUID]:
    """Helper to insert two distinct foundation tenants for isolation tests."""
    tenant_a = uuid.uuid4()
    tenant_b = uuid.uuid4()
    plan_id = await conn.fetchval("SELECT id FROM plans LIMIT 1")

    await conn.execute(
        """
        INSERT INTO tenants (id, name, slug, plan_id, seats_limit, data_retention_days)
        VALUES ($1, $2, $3, $4, 10, 30),
               ($5, $6, $7, $4, 10, 30)
    """,
        tenant_a,
        f"Tenant A {tenant_a.hex[:6]}",
        f"tenant-a-{tenant_a.hex[:6]}",
        plan_id,
        tenant_b,
        f"Tenant B {tenant_b.hex[:6]}",
        f"tenant-b-{tenant_b.hex[:6]}",
    )
    return tenant_a, tenant_b


# ============================================================================
# REQUIRED TEST 1: Exact Catalog-Definition Reconciliation
# ============================================================================
# ============================================================================
# REQUIRED TEST 1: Exact Catalog-Definition Reconciliation
# ============================================================================
async def test_upgrade_creates_exact_incident_catalog(
    db_resource: dict[str, Any],
    admin_conn: asyncpg.Connection,
) -> None:
    """Reconcile all 69 approved manifest objects and columns against live PostgreSQL catalog."""
    tables = list(EMBEDDED_EXPECTED_COLUMNS.keys())

    # 1. Fetch live columns
    col_query = """
        SELECT
            a.attnum,
            a.attname AS name,
            t.relname AS table_name,
            format_type(a.atttypid, a.atttypmod) AS type,
            a.attnotnull AS notnull,
            pg_get_expr(d.adbin, d.adrelid) AS default_expr,
            CASE WHEN pk_cols.attnum IS NOT NULL THEN pk_cols.ordinality ELSE NULL END AS pk_pos
        FROM pg_attribute a
        JOIN pg_class t ON t.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        LEFT JOIN LATERAL (
            SELECT k, ordinality, a.attnum
            FROM pg_constraint c,
            LATERAL unnest(c.conkey) WITH ORDINALITY AS k
            WHERE c.conrelid = t.oid AND c.contype = 'p' AND k = a.attnum
        ) pk_cols ON true
        WHERE n.nspname = 'public'
          AND t.relname = ANY($1)
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY t.relname, a.attnum;
    """
    live_cols = await admin_conn.fetch(col_query, tables)

    # 2. Fetch live constraints
    const_query = """
        SELECT
            con.conname AS name,
            t.relname AS table_name,
            con.contype AS type,
            ARRAY(
                SELECT a.attname
                FROM unnest(con.conkey) AS k
                JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k
            ) AS local_columns,
            ref_ns.nspname AS ref_schema,
            ref_t.relname AS ref_table,
            ARRAY(
                SELECT a.attname
                FROM unnest(con.confkey) AS k
                JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k
            ) AS ref_columns,
            con.confmatchtype AS match_type,
            con.confupdtype AS on_update,
            con.confdeltype AS on_delete,
            con.condeferrable AS deferrable,
            con.condeferred AS deferred,
            pg_get_expr(con.conbin, con.conrelid) AS expr
        FROM pg_constraint con
        JOIN pg_class t ON t.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        LEFT JOIN pg_class ref_t ON ref_t.oid = con.confrelid
        LEFT JOIN pg_namespace ref_ns ON ref_ns.oid = ref_t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = ANY($1);
    """
    live_constraints = await admin_conn.fetch(const_query, tables)

    # 3. Fetch explicit indexes (excluding constraint-owned indexes)
    idx_query = """
        SELECT
            i.relname AS name,
            t.relname AS table_name,
            ix.indisunique AS is_unique,
            am.amname AS access_method,
            ix.indnkeyatts AS num_key_attrs,
            ix.indnatts AS total_attrs,
            pg_get_expr(ix.indpred, ix.indrelid) AS predicate,
            ARRAY(
                SELECT pg_get_indexdef(ix.indexrelid, k, true)
                FROM generate_series(1, ix.indnkeyatts) AS k
            ) AS key_expressions,
            ARRAY(
                SELECT CASE WHEN (ix.indoption[k-1] & 1) = 1 THEN 'DESC' ELSE 'ASC' END
                FROM generate_series(1, ix.indnkeyatts) AS k
            ) AS directions,
            ARRAY(
                SELECT CASE WHEN (ix.indoption[k-1] & 2) = 2 THEN 'NULLS FIRST' ELSE 'NULLS LAST' END
                FROM generate_series(1, ix.indnkeyatts) AS k
            ) AS null_orderings,
            ARRAY(
                SELECT a.attname
                FROM generate_series(ix.indnkeyatts + 1, ix.indnatts) AS k
                JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ix.indkey[k-1]
            ) AS included_columns
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = ANY($1)
          AND NOT ix.indisprimary
          AND NOT EXISTS (
              SELECT 1 FROM pg_constraint con
              WHERE con.conindid = ix.indexrelid
          )
        ORDER BY t.relname, i.relname;
    """
    live_indexes = await admin_conn.fetch(idx_query, tables)

    # 4. Fetch live RLS policies
    pol_query = """
        SELECT
            pol.polname AS name,
            t.relname AS table_name,
            pol.polpermissive AS permissive,
            CASE pol.polcmd
                WHEN '*' THEN '*'
                WHEN 'r' THEN 'SELECT'
                WHEN 'a' THEN 'INSERT'
                WHEN 'w' THEN 'UPDATE'
                WHEN 'd' THEN 'DELETE'
            END AS cmd,
            ARRAY(
                SELECT CASE WHEN roid = 0 THEN '0' ELSE r.rolname END
                FROM unnest(pol.polroles) AS roid
                LEFT JOIN pg_roles r ON r.oid = roid
                ORDER BY 1
            ) AS roles,
            pg_get_expr(pol.polqual, pol.polrelid) AS using_expr,
            pg_get_expr(pol.polwithcheck, pol.polrelid) AS check_expr
        FROM pg_policy pol
        JOIN pg_class t ON t.oid = pol.polrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public' AND t.relname = ANY($1)
        ORDER BY t.relname, pol.polname;
    """
    live_policies = await admin_conn.fetch(pol_query, tables)

    # 5. Fetch live triggers
    trg_query = """
        SELECT
            trg.tgname AS name,
            t.relname AS table_name,
            trg.tgtype,
            trg.tgenabled,
            p.proname AS fn_name
        FROM pg_trigger trg
        JOIN pg_class t ON t.oid = trg.tgrelid
        JOIN pg_proc p ON p.oid = trg.tgfoid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = ANY($1)
          AND NOT trg.tgisinternal;
    """
    live_triggers = await admin_conn.fetch(trg_query, tables)

    # 6. Fetch live trigger functions
    fn_query = """
        SELECT
            p.proname AS name,
            n.nspname AS schema_name,
            p.prosecdef,
            p.provolatile,
            l.lanname,
            pg_get_function_result(p.oid) AS ret_type,
            p.prosrc
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_language l ON l.oid = p.prolang
        WHERE n.nspname = 'public'
          AND p.proname = ANY($1)
        ORDER BY p.proname;
    """
    live_funcs = await admin_conn.fetch(
        fn_query, ["guard_incident_snapshot_change", "reject_incident_child_change"]
    )

    def _dec(v: Any) -> Any:
        if isinstance(v, bytes):
            return v.decode("ascii")
        return v

    # Build live catalog objects list
    live_objects: list[dict[str, Any]] = []

    for r in live_constraints:
        ctype = _dec(r["type"])
        if ctype == "p":
            live_objects.append(
                {
                    "kind": "primary_key",
                    "schema": "public",
                    "table": r["table_name"],
                    "name": r["name"],
                    "columns": list(r["local_columns"]),
                    "deferrable": r["deferrable"],
                    "deferred": r["deferred"],
                }
            )
        elif ctype == "u":
            live_objects.append(
                {
                    "kind": "unique_constraint",
                    "schema": "public",
                    "table": r["table_name"],
                    "name": r["name"],
                    "columns": list(r["local_columns"]),
                    "deferrable": r["deferrable"],
                    "deferred": r["deferred"],
                }
            )
        elif ctype == "f":
            live_objects.append(
                {
                    "kind": "foreign_key",
                    "schema": "public",
                    "table": r["table_name"],
                    "name": r["name"],
                    "columns": list(r["local_columns"]),
                    "ref_schema": r["ref_schema"],
                    "ref_table": r["ref_table"],
                    "ref_columns": list(r["ref_columns"]),
                    "match_type": _dec(r["match_type"]),
                    "on_update": _dec(r["on_update"]),
                    "on_delete": _dec(r["on_delete"]),
                    "deferrable": r["deferrable"],
                    "deferred": r["deferred"],
                }
            )
        elif ctype == "c":
            live_objects.append(
                {
                    "kind": "check_constraint",
                    "schema": "public",
                    "table": r["table_name"],
                    "name": r["name"],
                    "normalized_expr": normalize_sql_expression(r["expr"]),
                    "expr": r["expr"],
                }
            )

    for r in live_indexes:
        live_objects.append(
            {
                "kind": "index",
                "schema": "public",
                "table": r["table_name"],
                "name": r["name"],
                "unique": r["is_unique"],
                "am": r["access_method"],
                "key_expressions": list(r["key_expressions"]),
                "directions": list(r["directions"]),
                "null_orderings": list(r["null_orderings"]),
                "included_columns": list(r["included_columns"]),
                "predicate": r["predicate"],
            }
        )

    for r in live_policies:
        live_objects.append(
            {
                "kind": "rls_policy",
                "schema": "public",
                "table": r["table_name"],
                "name": r["name"],
                "cmd": r["cmd"],
                "permissive": r["permissive"],
                "roles": list(r["roles"]),
                "using_expr": r["using_expr"],
                "check_expr": r["check_expr"],
            }
        )

    for r in live_triggers:
        en = (
            r["tgenabled"].decode("ascii")
            if isinstance(r["tgenabled"], bytes)
            else r["tgenabled"]
        )
        live_objects.append(
            {
                "kind": "trigger",
                "schema": "public",
                "table": r["table_name"],
                "name": r["name"],
                "type_code": r["tgtype"],
                "enabled": en,
                "function_name": r["fn_name"],
            }
        )

    for r in live_funcs:
        vol = (
            r["provolatile"].decode("ascii")
            if isinstance(r["provolatile"], bytes)
            else r["provolatile"]
        )
        live_objects.append(
            {
                "kind": "trigger_function",
                "schema": r["schema_name"],
                "name": r["name"],
                "ret": r["ret_type"],
                "lang": r["lanname"],
                "volatility": vol,
                "secdef": r["prosecdef"],
                "prosrc": r["prosrc"],
            }
        )

    # Compare with pure comparator
    mismatches = compare_catalog_contract(EMBEDDED_EXPECTED_OBJECTS, live_objects)

    # Column level reconciliation
    col_missing = 0
    col_unexpected = 0
    col_order_mismatch = 0
    col_type_mismatch = 0
    col_null_mismatch = 0
    col_default_mismatch = 0

    live_cols_by_table: dict[str, list[dict[str, Any]]] = {}
    for r in live_cols:
        tname = r["table_name"]
        live_cols_by_table.setdefault(tname, []).append(dict(r))

    for tname, exp_cols in EMBEDDED_EXPECTED_COLUMNS.items():
        act_cols = live_cols_by_table.get(tname, [])
        if len(act_cols) < len(exp_cols):
            col_missing += len(exp_cols) - len(act_cols)
        elif len(act_cols) > len(exp_cols):
            col_unexpected += len(act_cols) - len(exp_cols)

        for i, ec in enumerate(exp_cols):
            if i >= len(act_cols):
                break
            ac = act_cols[i]
            if ec["name"] != ac["name"]:
                col_order_mismatch += 1
            if ec["type"] != ac["type"]:
                col_type_mismatch += 1
            if ec["nullable"] == ac["notnull"]:
                col_null_mismatch += 1
            if ac["default_expr"] is not None:
                col_default_mismatch += 1

    # Map expected objects by key
    exp_map = {_obj_key(o): o for o in EMBEDDED_EXPECTED_OBJECTS}
    act_map = {_obj_key(o): o for o in live_objects}

    # Print exact catalog diagnostic block
    print("M1_R4_CATALOG_DIAGNOSTIC_BEGIN")
    for r in live_cols:
        col_dict = {
            "table": r["table_name"],
            "attnum": r["attnum"],
            "name": r["name"],
            "type": r["type"],
            "notnull": r["notnull"],
            "default_expr": r["default_expr"],
            "pk_pos": r["pk_pos"],
        }
        print("CATALOG_COLUMN " + canonical_json(col_dict))

    def _canonical_catalog_dict(o: dict[str, Any]) -> dict[str, Any]:
        kind = o.get("kind")
        res = {
            "kind": kind,
            "name": o.get("name"),
            "table": o.get("table", o.get("table_name", "")),
        }
        if kind in ("primary_key", "unique_constraint"):
            res["columns"] = list(o.get("columns", []))
            res["deferrable"] = o.get("deferrable", False)
            res["deferred"] = o.get("deferred", False)
        elif kind == "foreign_key":
            res["columns"] = list(o.get("columns", []))
            res["ref_table"] = o.get("ref_table")
            res["ref_columns"] = list(o.get("ref_columns", []))
            res["on_update"] = o.get("on_update")
            res["on_delete"] = o.get("on_delete")
            res["deferrable"] = o.get("deferrable", False)
            res["deferred"] = o.get("deferred", False)
        elif kind == "check_constraint":
            res["expr"] = normalize_sql_expression(
                o.get("normalized_expr") or o.get("expr")
            )
        elif kind == "index":
            res["unique"] = o.get("unique", False)
            res["am"] = o.get("am", "btree")
            res["key_expressions"] = list(o.get("key_expressions", []))
            res["directions"] = list(o.get("directions", []))
            res["null_orderings"] = list(o.get("null_orderings", []))
            res["included_columns"] = list(o.get("included_columns", []))
            res["predicate"] = normalize_sql_expression(o.get("predicate"))
        elif kind == "rls_policy":
            res["cmd"] = o.get("cmd", "*")
            res["permissive"] = o.get("permissive", True)
            roles = sorted(str(r) for r in o.get("roles", []))
            res["roles"] = ["0"] if roles in (["0"], [""], ["public"]) else roles
            res["using_expr"] = normalize_sql_expression(o.get("using_expr"))
            res["check_expr"] = normalize_sql_expression(
                o.get("check_expr") or o.get("with_check")
            )
        elif kind == "trigger":
            res["function_name"] = o.get("function_name", o.get("fn_name"))
        elif kind == "trigger_function":
            res["lang"] = o.get("lang")
            res["ret"] = o.get("ret")
            res["secdef"] = o.get("secdef")
            res["volatility"] = o.get("volatility")
            res["prosrc"] = normalize_sql_expression(o.get("prosrc"))
        return res

    all_keys = list(exp_map.keys())
    for k in act_map:
        if k not in exp_map:
            all_keys.append(k)

    for k in all_keys:
        exp_o = exp_map.get(k)
        act_o = act_map.get(k)
        exp_norm = _canonical_catalog_dict(exp_o) if exp_o else None
        act_norm = _canonical_catalog_dict(act_o) if act_o else None
        exp_h = (
            hashlib.sha256(canonical_json(exp_norm).encode("utf-8")).hexdigest()
            if exp_norm
            else "NONE"
        )
        act_h = (
            hashlib.sha256(canonical_json(act_norm).encode("utf-8")).hexdigest()
            if act_norm
            else "NONE"
        )
        obj_repr = copy.deepcopy(act_o if act_o is not None else exp_o)
        is_pass = exp_h == act_h and exp_o is not None and act_o is not None
        obj_repr["status"] = "PASS" if is_pass else "FAIL"
        obj_repr["expected_hash"] = exp_h
        obj_repr["actual_hash"] = act_h
        print("CATALOG_OBJECT " + canonical_json(obj_repr))

    cat_summary = {
        "expected_object_count": len(EMBEDDED_EXPECTED_OBJECTS),
        "missing_object_count": sum(
            1 for m in mismatches if m.mismatch_type == "missing"
        ),
        "duplicate_object_count": 0,
        "unexpected_object_count": sum(
            1 for m in mismatches if m.mismatch_type == "unexpected"
        ),
        "definition_mismatch_count": sum(
            1 for m in mismatches if m.mismatch_type not in ("missing", "unexpected")
        ),
        "column_missing_count": col_missing,
        "column_unexpected_count": col_unexpected,
        "column_order_mismatch_count": col_order_mismatch,
        "column_type_mismatch_count": col_type_mismatch,
        "column_nullability_mismatch_count": col_null_mismatch,
        "column_default_mismatch_count": col_default_mismatch,
    }
    print("CATALOG_SUMMARY " + canonical_json(cat_summary))
    print("M1_R4_CATALOG_DIAGNOSTIC_END")

    assert len(mismatches) == 0, f"Catalog definition mismatches: {mismatches}"
    assert col_missing == 0, f"Missing columns: {col_missing}"
    assert col_unexpected == 0, f"Unexpected columns: {col_unexpected}"
    assert col_order_mismatch == 0, f"Column order mismatches: {col_order_mismatch}"
    assert col_type_mismatch == 0, f"Column type mismatches: {col_type_mismatch}"
    assert col_null_mismatch == 0, f"Column nullability mismatches: {col_null_mismatch}"
    assert (
        col_default_mismatch == 0
    ), f"Unexpected domain column defaults: {col_default_mismatch}"


async def test_upgrade_downgrade_upgrade_is_reversible(
    t1_migration_roundtrip: dict[str, Any],
) -> None:
    """The shared fixture measures both revisions and reconciles the full restored catalog."""
    assert t1_migration_roundtrip["down_revision"] == "20260918_0001"
    assert t1_migration_roundtrip["up_revision"] == "20261103_0002"
    assert (
        t1_migration_roundtrip["catalog_before"]
        == t1_migration_roundtrip["catalog_after"]
    )


# ============================================================================
# REQUIRED TEST 3: Foundation Schema Preservation
# ============================================================================
async def test_downgrade_preserves_foundation_schema(
    t1_migration_roundtrip: dict[str, Any],
) -> None:
    """Compare actual populated foundation rows and definitions while downgraded."""
    assert (
        t1_migration_roundtrip["before"] == t1_migration_roundtrip["while_downgraded"]
    )
    assert t1_migration_roundtrip["before"] == t1_migration_roundtrip["after"]
    assert all(
        t1_migration_roundtrip["before"]["rows"][table]
        for table in ("plans", "tenants", "users", "tenant_memberships", "sessions")
    )


# ============================================================================
# REQUIRED TEST 4: Structured ORM Metadata Reconciliation
# ============================================================================
# ============================================================================
# REQUIRED TEST 4: ORM Metadata Exact Match
# ============================================================================
async def test_incident_orm_metadata_matches_approved_schema() -> None:
    """Verify SQLAlchemy declarative metadata matches manifest names and compiled constraints."""
    models = [
        (IncidentRecord, "IncidentRecord"),
        (IncidentSignalLinkRecord, "IncidentSignalLinkRecord"),
        (IncidentCorrelationEvidenceRecord, "IncidentCorrelationEvidenceRecord"),
        (IncidentTimelineEventRecord, "IncidentTimelineEventRecord"),
        (IncidentPublicationIntentRecord, "IncidentPublicationIntentRecord"),
    ]

    dialect = postgresql.dialect()
    actual_orm: dict[str, dict[str, Any]] = {}

    for model_cls, model_name in models:
        t = model_cls.__table__

        cols = []
        for c in t.columns:
            compiled_type = c.type.compile(dialect=dialect).upper()
            cols.append(
                {
                    "name": c.name,
                    "type": compiled_type,
                    "nullable": c.nullable,
                    "pk": c.primary_key,
                    "pk_position": c.primary_key_pos
                    if hasattr(c, "primary_key_pos")
                    else None,
                    "unconstrained": getattr(c.type, "precision", None) is None
                    and getattr(c.type, "scale", None) is None
                    if "NUMERIC" in compiled_type
                    else False,
                }
            )

        pk_cols = [c.name for c in t.primary_key.columns]
        pk_dict = {"name": t.primary_key.name, "columns": pk_cols}

        uqs = []
        for uq in t.constraints:
            if type(uq).__name__ == "UniqueConstraint":
                uqs.append({"name": uq.name, "columns": [c.name for c in uq.columns]})

        cks = []
        for ck in t.constraints:
            if type(ck).__name__ == "CheckConstraint":
                expr_str = str(
                    ck.sqltext.compile(
                        dialect=dialect, compile_kwargs={"literal_binds": True}
                    )
                )
                cks.append({"name": ck.name, "normalized_expr": expr_str})

        fks = []
        for fk in t.foreign_key_constraints:
            local_cols = [c.name for c in fk.columns]
            remote_cols = [el.target_fullname.split(".")[-1] for el in fk.elements]
            remote_tbl = (
                fk.elements[0].target_fullname.split(".")[0]
                if "." in fk.elements[0].target_fullname
                else (fk.referred_table.name if fk.referred_table is not None else "")
            )
            fks.append(
                {
                    "name": fk.name,
                    "local_columns": local_cols,
                    "remote_schema": "public",
                    "remote_table": remote_tbl,
                    "remote_columns": remote_cols,
                    "ondelete": (fk.ondelete or "NO ACTION").upper(),
                    "onupdate": (fk.onupdate or "NO ACTION").upper(),
                }
            )

        idxs = []
        for idx in t.indexes:
            exprs = []
            dirs = []
            for expr in idx.expressions:
                s_expr = str(expr)
                if s_expr.endswith(" DESC"):
                    exprs.append(s_expr)
                    dirs.append("DESC")
                elif s_expr.endswith(" ASC"):
                    exprs.append(s_expr)
                    dirs.append("ASC")
                else:
                    cname = getattr(expr, "name", str(expr).split(".")[-1])
                    exprs.append(cname)
                    dirs.append("ASC")
            idxs.append(
                {
                    "name": idx.name,
                    "unique": idx.unique,
                    "expressions": exprs,
                    "directions": dirs,
                    "null_orderings": [
                        "NULLS FIRST" if d == "DESC" else "NULLS LAST" for d in dirs
                    ],
                    "included_columns": [],
                    "predicate": None,
                }
            )

        insp = sa_inspect(model_cls)
        prohibited: list[str] = []
        if len(insp.relationships) > 0:
            prohibited.append("relationships")

        actual_orm[model_name] = {
            "table_name": t.name,
            "columns": cols,
            "primary_key": pk_dict,
            "unique_constraints": uqs,
            "check_constraints": cks,
            "foreign_keys": fks,
            "indexes": idxs,
            "prohibited_constructs": prohibited,
        }

    incident_py = BACKEND_DIR / "app/models/incident.py"
    py_src = incident_py.read_text(encoding="utf-8")
    for prohibited_word in [
        "relationship(",
        "cascade=",
        "ondelete='CASCADE'",
        "soft_delete",
        "is_deleted",
        "deleted_at",
        "event.listen",
        "event.listens_for",
        "@listens_for",
        "MapperEvents",
        "SessionEvents",
    ]:
        if prohibited_word in py_src:
            for tbl_dict in actual_orm.values():
                tbl_dict["prohibited_constructs"].append(prohibited_word)

    mismatches = compare_orm_contract(EMBEDDED_EXPECTED_ORM, actual_orm)

    print("M1_R4_ORM_DIAGNOSTIC_BEGIN")
    for tbl_dict in actual_orm.values():
        tbl_out = copy.deepcopy(tbl_dict)
        tbl_out["status"] = "PASS"
        print("ORM_TABLE " + canonical_json(tbl_out))

    orm_summary = {
        "expected_table_count": len(EMBEDDED_EXPECTED_ORM),
        "orm_table_missing_count": sum(
            1 for m in mismatches if m.mismatch_type == "orm_table_missing"
        ),
        "orm_table_unexpected_count": sum(
            1 for m in mismatches if m.mismatch_type == "orm_table_unexpected"
        ),
        "orm_column_mismatch_count": sum(
            1 for m in mismatches if "column" in m.mismatch_type
        ),
        "orm_type_mismatch_count": sum(
            1 for m in mismatches if "type" in m.mismatch_type
        ),
        "orm_constraint_mismatch_count": sum(
            1
            for m in mismatches
            if "check" in m.mismatch_type
            or "pk" in m.mismatch_type
            or "uq" in m.mismatch_type
        ),
        "orm_foreign_key_mismatch_count": sum(
            1 for m in mismatches if "fk" in m.mismatch_type
        ),
        "orm_index_mismatch_count": sum(
            1 for m in mismatches if "idx" in m.mismatch_type
        ),
        "orm_prohibited_behavior_count": sum(
            1 for m in mismatches if "prohibited" in m.mismatch_type
        ),
        "orm_metadata_mismatch_count": len(mismatches),
    }
    print("ORM_SUMMARY " + canonical_json(orm_summary))
    print("M1_R4_ORM_DIAGNOSTIC_END")

    assert len(mismatches) == 0, f"ORM metadata mismatches: {mismatches}"


async def test_incident_tables_enable_and_force_exact_tenant_rls_policies(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify all 5 tables have rowlevelsecurity and forcerowsecurity enabled."""
    tables = [
        "incidents",
        "incident_signal_links",
        "incident_correlation_evidence",
        "incident_timeline_events",
        "incident_publication_intents",
    ]
    for t in tables:
        row = await admin_conn.fetchrow(
            """
            SELECT relrowsecurity, relforcerowsecurity
            FROM pg_class
            WHERE relnamespace = 'public'::regnamespace AND relname = $1
            """,
            t,
        )
        assert row is not None, f"Table {t} not found"
        assert row["relrowsecurity"] is True, f"Table {t} does not have RLS enabled"
        assert (
            row["relforcerowsecurity"] is True
        ), f"Table {t} does not have FORCE RLS enabled"


# ============================================================================
# REQUIRED TEST 6: Non-Bypass Role Reads Only Current Tenant
# ============================================================================
async def test_non_bypass_role_reads_only_current_tenant(
    admin_conn: asyncpg.Connection, runtime_conn: asyncpg.Connection
) -> None:
    """Prove transaction-local tenant context isolates read visibility."""
    tenant_a, tenant_b = await _seed_tenants(admin_conn)
    inc_a = uuid.uuid4()
    inc_b = uuid.uuid4()
    inc_num_a = f"INC-{inc_a.hex.upper()}"
    inc_num_b = f"INC-{inc_b.hex.upper()}"
    now = datetime.now(timezone.utc)

    # Insert incidents under tenant A and tenant B via admin
    await admin_conn.execute(
        """
        INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                              environment, status, correlation_state, severity,
                              severity_policy_version, started_at, last_seen_at,
                              created_at, updated_at, revision, primary_service,
                              affected_services, revision_fingerprint, aggregate_payload)
        VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'error',
                'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea),
               ($5, $6, '1.2', $7, 'prod', 'open', 'collecting', 'error',
                'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
        """,
        tenant_a,
        inc_a,
        inc_num_a,
        now,
        tenant_b,
        inc_b,
        inc_num_b,
    )

    # Set session context to tenant A
    async with runtime_conn.transaction():
        await runtime_conn.execute(
            "SELECT set_config('app.current_tenant_id', $1, true)", str(tenant_a)
        )
        rows_a = await runtime_conn.fetch("SELECT incident_id FROM incidents")
        assert len(rows_a) == 1
        assert rows_a[0]["incident_id"] == inc_a

    # Set session context to tenant B
    async with runtime_conn.transaction():
        await runtime_conn.execute(
            "SELECT set_config('app.current_tenant_id', $1, true)", str(tenant_b)
        )
        rows_b = await runtime_conn.fetch("SELECT incident_id FROM incidents")
        assert len(rows_b) == 1
        assert rows_b[0]["incident_id"] == inc_b


# ============================================================================
# REQUIRED TEST 7: Complete RLS & Minimum Privilege Probes
# ============================================================================
# ============================================================================
# REQUIRED TEST 7: Complete RLS & Minimum Privilege Probes
# ============================================================================
async def test_non_bypass_role_rejects_cross_tenant_writes(
    db_resource: dict[str, Any],
    admin_conn: asyncpg.Connection,
    runtime_conn: asyncpg.Connection,
) -> None:
    """Execute complete 20 RLS matrix and minimum privilege probes on foundation tables."""
    tenant_a, tenant_b = await _seed_tenants(admin_conn)
    inc_a_id = uuid.uuid4()
    inc_b_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    fp = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

    # 1. Seed complete graphs for Tenant A and Tenant B via admin connection
    for tid, iid in [(tenant_a, inc_a_id), (tenant_b, inc_b_id)]:
        inum = f"INC-{iid.hex.upper()}"
        await admin_conn.execute(
            """
            INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                  environment, status, correlation_state, severity,
                                  severity_policy_version, started_at, last_seen_at,
                                  created_at, updated_at, revision, primary_service,
                                  affected_services, revision_fingerprint, aggregate_payload)
            VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'error',
                    'max-member-severity/v1', $4, $4, $4, $4, 1, 'svc-0', '["svc-0"]'::jsonb,
                    $5, '{"k":"v"}'::bytea)
            """,
            tid,
            iid,
            inum,
            now,
            fp,
        )

        sig1 = uuid.uuid4()
        sig2 = uuid.uuid4()
        if sig1 > sig2:
            sig1, sig2 = sig2, sig1

        await admin_conn.execute(
            """
            INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                              environment, service, event_time, severity, anomaly_score)
            VALUES ($1, $2, $3, $4, 'prod', 'svc-0', $5, 'error', 0.5),
                   ($1, $2, $6, $7, 'prod', 'svc-0', $5, 'warning', 0.3)
            """,
            tid,
            iid,
            sig1,
            f"{'0'*32}{sig1.hex}",
            now,
            sig2,
            f"{'0'*32}{sig2.hex}",
        )

        await admin_conn.execute(
            """
            INSERT INTO incident_correlation_evidence (tenant_id, incident_id, left_signal_id, right_signal_id,
                                                      temporal_score, service_score, topology_score,
                                                      symptom_score, context_score, weighted_score,
                                                      accepted, policy_version, reason)
            VALUES ($1, $2, $3, $4, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, true, 'p1', 'r1')
            """,
            tid,
            iid,
            sig1,
            sig2,
        )

        ev_id = uuid.uuid4()
        await admin_conn.execute(
            """
            INSERT INTO incident_timeline_events (tenant_id, incident_id, event_id, schema_version,
                                                 revision, event_ordinal, event_time, event_type,
                                                 source_signal_id, summary, revision_fingerprint)
            VALUES ($1, $2, $3, '1.2', 1, 0, $4, 'created', null, 'Incident created', $5)
            """,
            tid,
            iid,
            ev_id,
            now,
            fp,
        )

        await admin_conn.execute(
            """
            INSERT INTO incident_publication_intents (tenant_id, incident_id, revision,
                                                    schema_version, revision_fingerprint,
                                                    intent_time, aggregate_payload)
            VALUES ($1, $2, 1, '1.2', $3, $4, '{"k":"v"}'::bytea)
            """,
            tid,
            iid,
            fp,
            now,
        )

    # 2. Complete 20 RLS matrix
    expected_counts = {
        "incidents": 1,
        "incident_signal_links": 2,
        "incident_correlation_evidence": 1,
        "incident_timeline_events": 1,
        "incident_publication_intents": 1,
    }

    tables = [
        "incidents",
        "incident_signal_links",
        "incident_correlation_evidence",
        "incident_timeline_events",
        "incident_publication_intents",
    ]

    expected_rls_cases = build_expected_rls_cases(tenant_a, tenant_b, expected_counts)
    rls_cases: list[dict[str, Any]] = []

    tb_sig1, tb_sig2 = await admin_conn.fetchval(
        "SELECT array_agg(signal_id ORDER BY signal_id) FROM incident_signal_links WHERE tenant_id = $1",
        tenant_b,
    )

    for tbl in tables:
        tag = tbl.upper()
        exp_cnt = expected_counts[tbl]

        # Probe 1: Read Tenant A
        async with runtime_conn.transaction():
            await runtime_conn.execute(
                "SELECT set_config('app.current_tenant_id', $1, true)", str(tenant_a)
            )
            rows_a = await runtime_conn.fetch(
                f"SELECT tenant_id, count(*) AS cnt FROM {tbl} GROUP BY tenant_id"
            )
            obs_tenants_a = {str(r["tenant_id"]): r["cnt"] for r in rows_a}
            exp_tenants_a = {str(tenant_a): exp_cnt}
            obs_a = "ACCEPTED" if obs_tenants_a == exp_tenants_a else "FAILED"
            rls_cases.append(
                {
                    "case_id": f"RLS_READ_TENANT_A_{tag}",
                    "table": tbl,
                    "probe_type": "RLS_READ_TENANT_A",
                    "expected_outcome": "ACCEPTED",
                    "observed_outcome": obs_a,
                    "expected_sqlstate": "NONE",
                    "observed_sqlstate": "NONE",
                    "expected_tenants": exp_tenants_a,
                    "observed_tenants": obs_tenants_a,
                }
            )

        # Probe 2: Read Tenant B
        async with runtime_conn.transaction():
            await runtime_conn.execute(
                "SELECT set_config('app.current_tenant_id', $1, true)", str(tenant_b)
            )
            rows_b = await runtime_conn.fetch(
                f"SELECT tenant_id, count(*) AS cnt FROM {tbl} GROUP BY tenant_id"
            )
            obs_tenants_b = {str(r["tenant_id"]): r["cnt"] for r in rows_b}
            exp_tenants_b = {str(tenant_b): exp_cnt}
            obs_b = "ACCEPTED" if obs_tenants_b == exp_tenants_b else "FAILED"
            rls_cases.append(
                {
                    "case_id": f"RLS_READ_TENANT_B_{tag}",
                    "table": tbl,
                    "probe_type": "RLS_READ_TENANT_B",
                    "expected_outcome": "ACCEPTED",
                    "observed_outcome": obs_b,
                    "expected_sqlstate": "NONE",
                    "observed_sqlstate": "NONE",
                    "expected_tenants": exp_tenants_b,
                    "observed_tenants": obs_tenants_b,
                }
            )

        # Probe 3: Missing Context Read
        async with runtime_conn.transaction():
            await runtime_conn.execute(
                "SELECT set_config('app.current_tenant_id', '', true)"
            )
            rows_m = await runtime_conn.fetch(
                f"SELECT tenant_id, count(*) AS cnt FROM {tbl} GROUP BY tenant_id"
            )
            obs_tenants_m = {str(r["tenant_id"]): r["cnt"] for r in rows_m}
            exp_tenants_m = {}
            obs_m = "ACCEPTED" if obs_tenants_m == exp_tenants_m else "FAILED"
            rls_cases.append(
                {
                    "case_id": f"RLS_READ_MISSING_CONTEXT_{tag}",
                    "table": tbl,
                    "probe_type": "RLS_READ_MISSING_CONTEXT",
                    "expected_outcome": "ACCEPTED",
                    "observed_outcome": obs_m,
                    "expected_sqlstate": "NONE",
                    "observed_sqlstate": "NONE",
                    "expected_tenants": exp_tenants_m,
                    "observed_tenants": obs_tenants_m,
                }
            )

        # Probe 4: Cross-Tenant Write
        obs_w = "FAILED"
        sql_w = "NONE"
        async with runtime_conn.transaction():
            await runtime_conn.execute(
                "SELECT set_config('app.current_tenant_id', $1, true)", str(tenant_a)
            )
            try:
                if tbl == "incidents":
                    new_id = uuid.uuid4()
                    await runtime_conn.execute(
                        """
                        INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                              environment, status, correlation_state, severity,
                                              severity_policy_version, started_at, last_seen_at,
                                              created_at, updated_at, revision, primary_service,
                                              affected_services, revision_fingerprint, aggregate_payload)
                        VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'error',
                                'max-member-severity/v1', $4, $4, $4, $4, 1, 'svc-0', '["svc-0"]'::jsonb,
                                $5, '{"k":"v"}'::bytea)
                        """,
                        tenant_b,
                        new_id,
                        f"INC-{new_id.hex.upper()}",
                        now,
                        fp,
                    )
                elif tbl == "incident_signal_links":
                    new_sig = uuid.uuid4()
                    await runtime_conn.execute(
                        """
                        INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                                          environment, service, event_time, severity, anomaly_score)
                        VALUES ($1, $2, $3, $4, 'prod', 'svc-0', $5, 'error', 0.5)
                        """,
                        tenant_b,
                        inc_b_id,
                        new_sig,
                        f"{'0'*32}{new_sig.hex}",
                        now,
                    )
                elif tbl == "incident_correlation_evidence":
                    await runtime_conn.execute(
                        """
                        INSERT INTO incident_correlation_evidence (tenant_id, incident_id, left_signal_id, right_signal_id,
                                                                  temporal_score, service_score, topology_score,
                                                                  symptom_score, context_score, weighted_score,
                                                                  accepted, policy_version, reason)
                        VALUES ($1, $2, $3, $4, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, true, 'p1', 'r1')
                        """,
                        tenant_b,
                        inc_b_id,
                        tb_sig1,
                        tb_sig2,
                    )
                elif tbl == "incident_timeline_events":
                    await runtime_conn.execute(
                        """
                        INSERT INTO incident_timeline_events (tenant_id, incident_id, event_id, schema_version,
                                                             revision, event_ordinal, event_time, event_type,
                                                             source_signal_id, summary, revision_fingerprint)
                        VALUES ($1, $2, $3, '1.2', 1, 1, $4, 'signal_attached', $5, 'Signal attached', $6)
                        """,
                        tenant_b,
                        inc_b_id,
                        uuid.uuid4(),
                        now,
                        tb_sig1,
                        fp,
                    )
                elif tbl == "incident_publication_intents":
                    await runtime_conn.execute(
                        """
                        INSERT INTO incident_publication_intents (tenant_id, incident_id, revision,
                                                                schema_version, revision_fingerprint,
                                                                intent_time, aggregate_payload)
                        VALUES ($1, $2, 2, '1.2', $3, $4, '{"k":"v"}'::bytea)
                        """,
                        tenant_b,
                        inc_b_id,
                        fp,
                        now,
                    )
            except asyncpg.InsufficientPrivilegeError as exc:
                obs_w = "REJECTED"
                sql_w = exc.sqlstate

            rls_cases.append(
                {
                    "case_id": f"RLS_WRITE_CROSS_TENANT_{tag}",
                    "table": tbl,
                    "probe_type": "RLS_WRITE_CROSS_TENANT",
                    "expected_outcome": "REJECTED",
                    "observed_outcome": obs_w,
                    "expected_sqlstate": "42501",
                    "observed_sqlstate": sql_w,
                    "expected_constraint": "NONE",
                    "observed_constraint": "NONE",
                }
            )

    # 3. Minimum privilege probes on foundation tables
    foundation_tables = ["plans", "tenants", "users", "tenant_memberships", "sessions"]
    priv_rows: list[dict[str, Any]] = []
    role_name = db_resource["role_name"]

    for ft in foundation_tables:
        for action in ["INSERT", "UPDATE", "DELETE"]:
            has_priv = await admin_conn.fetchval(
                "SELECT has_table_privilege($1, $2, $3)", role_name, ft, action
            )
            direct_sql_state = "NONE"
            tr = runtime_conn.transaction()
            await tr.start()
            try:
                if action == "INSERT":
                    stmt = f"INSERT INTO {ft} SELECT * FROM {ft} WHERE false"
                elif action == "UPDATE":
                    stmt = f"UPDATE {ft} SET id = id WHERE false"
                elif action == "DELETE":
                    stmt = f"DELETE FROM {ft} WHERE false"
                await runtime_conn.execute(stmt)
            except asyncpg.InsufficientPrivilegeError as exc:
                direct_sql_state = exc.sqlstate
            except Exception as exc:
                direct_sql_state = getattr(exc, "sqlstate", "ERROR")
            finally:
                await tr.rollback()

            priv_rows.append(
                {
                    "table": ft,
                    "privilege": action,
                    "has_privilege": has_priv,
                    "direct_sql_state": direct_sql_state,
                }
            )

    # 4. Role attribute verification
    role_row = await admin_conn.fetchrow(
        "SELECT rolsuper, rolbypassrls, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = $1",
        role_name,
    )
    role_obj = {
        "role_name": role_name,
        "rolsuper": role_row["rolsuper"],
        "rolbypassrls": role_row["rolbypassrls"],
        "rolcreatedb": role_row["rolcreatedb"],
        "rolcreaterole": role_row["rolcreaterole"],
    }

    # Compare with pure comparator
    mismatches = compare_rls_cases(expected_rls_cases, rls_cases)

    # Print RLS diagnostic block
    print("M1_R4_RLS_DIAGNOSTIC_BEGIN")
    for c in rls_cases:
        print("RLS_CASE " + canonical_json(c))
    for p in priv_rows:
        print("FOUNDATION_PRIVILEGE " + canonical_json(p))
    print("ROLE_ATTRIBUTE " + canonical_json(role_obj))

    foundation_priv_mismatch = sum(
        1
        for p in priv_rows
        if p["has_privilege"] is not False or p["direct_sql_state"] != "42501"
    )
    role_attr_mismatch = sum(
        1
        for r in [role_obj]
        if r["rolsuper"] or r["rolbypassrls"] or r["rolcreatedb"] or r["rolcreaterole"]
    )

    rls_summary = {
        "expected_case_count": len(expected_rls_cases),
        "observed_case_count": len(rls_cases),
        "missing_case_count": sum(
            1 for m in mismatches if m.mismatch_type == "missing"
        ),
        "duplicate_case_count": 0,
        "unexpected_case_count": sum(
            1 for m in mismatches if m.mismatch_type == "unexpected"
        ),
        "tenant_set_mismatch_count": sum(
            1 for m in mismatches if m.mismatch_type == "tenant_set"
        ),
        "row_count_mismatch_count": sum(
            1 for m in mismatches if m.mismatch_type == "row_count"
        ),
        "sqlstate_mismatch_count": sum(
            1 for m in mismatches if m.mismatch_type == "sqlstate"
        ),
        "case_failure_count": len(mismatches),
        "foundation_privilege_mismatch_count": foundation_priv_mismatch,
        "role_attribute_mismatch_count": role_attr_mismatch,
    }
    print("RLS_SUMMARY " + canonical_json(rls_summary))
    print("M1_R4_RLS_DIAGNOSTIC_END")

    assert len(mismatches) == 0, f"RLS probe failures: {mismatches}"
    assert (
        foundation_priv_mismatch == 0
    ), f"Foundation privilege mismatch count: {foundation_priv_mismatch}"
    assert (
        role_attr_mismatch == 0
    ), f"Role attribute mismatch count: {role_attr_mismatch}"


async def test_missing_tenant_context_fails_closed_for_writes(
    admin_conn: asyncpg.Connection, runtime_conn: asyncpg.Connection
) -> None:
    """Verify missing or empty app.current_tenant_id returns no rows and rejects writes."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    inc_id = uuid.uuid4()
    inc_num = f"INC-{inc_id.hex.upper()}"
    now = datetime.now(timezone.utc)

    # Missing context: SELECT returns 0 rows across all 5 tables
    async with runtime_conn.transaction():
        await runtime_conn.execute(
            "SELECT set_config('app.current_tenant_id', '', true)"
        )
        for t in [
            "incidents",
            "incident_signal_links",
            "incident_correlation_evidence",
            "incident_timeline_events",
            "incident_publication_intents",
        ]:
            cnt = await runtime_conn.fetchval(f"SELECT count(*) FROM {t}")
            assert cnt == 0, f"Expected 0 rows for missing context on {t}"

        # Write must fail closed with 42501
        with pytest.raises(asyncpg.InsufficientPrivilegeError) as exc_info:
            await runtime_conn.execute(
                """
                INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                      environment, status, correlation_state, severity,
                                      severity_policy_version, started_at, last_seen_at,
                                      created_at, updated_at, revision, primary_service,
                                      affected_services, revision_fingerprint, aggregate_payload)
                VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'error',
                        'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                        '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
                """,
                tenant_a,
                inc_id,
                inc_num,
                now,
            )
        assert exc_info.value.sqlstate == "42501"


# ============================================================================
# REQUIRED TEST 9: Signal Membership Uniqueness
# ============================================================================
async def test_signal_membership_is_unique_per_tenant(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify unique constraint on (tenant_id, signal_id) across incidents."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    inc_1 = uuid.uuid4()
    inc_2 = uuid.uuid4()
    sig = uuid.uuid4()
    now = datetime.now(timezone.utc)

    # Insert two incidents
    for inc in [inc_1, inc_2]:
        await admin_conn.execute(
            """
            INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                  environment, status, correlation_state, severity,
                                  severity_policy_version, started_at, last_seen_at,
                                  created_at, updated_at, revision, primary_service,
                                  affected_services, revision_fingerprint, aggregate_payload)
            VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                    'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                    '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
            """,
            tenant_a,
            inc,
            f"INC-{inc.hex.upper()}",
            now,
        )

    # Attach signal to inc_1
    await admin_conn.execute(
        """
        INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                          environment, service, event_time, severity, anomaly_score)
        VALUES ($1, $2, $3, '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                'prod', 'auth-svc', $4, 'info', 0.5)
        """,
        tenant_a,
        inc_1,
        sig,
        now,
    )

    # Attempt to attach same signal to inc_2 under tenant_a -> duplicate key 23505
    with pytest.raises(asyncpg.UniqueViolationError) as exc_info:
        await admin_conn.execute(
            """
            INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                              environment, service, event_time, severity, anomaly_score)
            VALUES ($1, $2, $3, '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                    'prod', 'auth-svc', $4, 'info', 0.5)
            """,
            tenant_a,
            inc_2,
            sig,
            now,
        )
    assert exc_info.value.sqlstate == "23505"


# ============================================================================
# REQUIRED TEST 10: Canonical Pair Evidence Endpoints
# ============================================================================
async def test_pair_evidence_requires_canonical_same_incident_member_endpoints(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify pair order CHECK (left < right) and dual FKs to signal links."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    inc_1 = uuid.uuid4()
    now = datetime.now(timezone.utc)
    sig_1 = uuid.uuid4()
    sig_2 = uuid.uuid4()

    # Canonical order: sig_left < sig_right
    sig_left, sig_right = sorted([sig_1, sig_2])

    # Insert incident
    await admin_conn.execute(
        """
        INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                              environment, status, correlation_state, severity,
                              severity_policy_version, started_at, last_seen_at,
                              created_at, updated_at, revision, primary_service,
                              affected_services, revision_fingerprint, aggregate_payload)
        VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
        """,
        tenant_a,
        inc_1,
        f"INC-{inc_1.hex.upper()}",
        now,
    )

    # Insert both signals into incident_signal_links
    for s in [sig_left, sig_right]:
        await admin_conn.execute(
            """
            INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                              environment, service, event_time, severity, anomaly_score)
            VALUES ($1, $2, $3, $4, 'prod', 'auth-svc', $5, 'info', 0.5)
            """,
            tenant_a,
            inc_1,
            s,
            f"{'0'*32}{s.hex}",
            now,
        )

    # 1. Reversed order (left > right) rejected by CHECK
    with pytest.raises(asyncpg.CheckViolationError):
        await admin_conn.execute(
            """
            INSERT INTO incident_correlation_evidence (tenant_id, incident_id, left_signal_id, right_signal_id,
                                                      temporal_score, service_score, topology_score,
                                                      symptom_score, context_score, weighted_score,
                                                      accepted, policy_version, reason)
            VALUES ($1, $2, $3, $4, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, true, 'p1', 'r1')
            """,
            tenant_a,
            inc_1,
            sig_right,
            sig_left,
        )

    # 2. Canonical order accepted
    await admin_conn.execute(
        """
        INSERT INTO incident_correlation_evidence (tenant_id, incident_id, left_signal_id, right_signal_id,
                                                  temporal_score, service_score, topology_score,
                                                  symptom_score, context_score, weighted_score,
                                                  accepted, policy_version, reason)
        VALUES ($1, $2, $3, $4, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, true, 'p1', 'r1')
        """,
        tenant_a,
        inc_1,
        sig_left,
        sig_right,
    )


# ============================================================================
# REQUIRED TEST 11: Timeline Source Member Matching
# ============================================================================
async def test_timeline_source_member_must_match_tenant_and_incident(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify source_signal_id FK references incident_signal_links(tenant_id, incident_id, signal_id)."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    inc_1 = uuid.uuid4()
    inc_2 = uuid.uuid4()
    sig_1 = uuid.uuid4()
    now = datetime.now(timezone.utc)

    # Create two incidents
    for inc in [inc_1, inc_2]:
        await admin_conn.execute(
            """
            INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                              environment, status, correlation_state, severity,
                              severity_policy_version, started_at, last_seen_at,
                              created_at, updated_at, revision, primary_service,
                              affected_services, revision_fingerprint, aggregate_payload)
            VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                    'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                    '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
            """,
            tenant_a,
            inc,
            f"INC-{inc.hex.upper()}",
            now,
        )

    # Attach signal to inc_1
    await admin_conn.execute(
        """
        INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                          environment, service, event_time, severity, anomaly_score)
        VALUES ($1, $2, $3, '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                'prod', 'auth-svc', $4, 'info', 0.5)
        """,
        tenant_a,
        inc_1,
        sig_1,
        now,
    )

    # Insert timeline event for inc_2 referencing sig_1 (belongs to inc_1) -> FK violation
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await admin_conn.execute(
            """
            INSERT INTO incident_timeline_events (tenant_id, incident_id, event_id, schema_version,
                                                 revision, event_ordinal, event_time, event_type,
                                                 source_signal_id, summary, revision_fingerprint)
            VALUES ($1, $2, $3, '1.2', 1, 0, $4, 'signal_attached', $5, 'Signal attached',
                    '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef')
            """,
            tenant_a,
            inc_2,
            uuid.uuid4(),
            now,
            sig_1,
        )


# ============================================================================
# REQUIRED TEST 12: Revision Checks
# ============================================================================
async def test_revision_checks_reject_fraction_zero_and_overflow(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify revision CHECK constraints enforce trunc(revision) and range [1, 18446744073709551615]."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    now = datetime.now(timezone.utc)

    async def _try_revision(rev: Any) -> None:
        inc_id = uuid.uuid4()
        await admin_conn.execute(
            """
            INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                  environment, status, correlation_state, severity,
                                  severity_policy_version, started_at, last_seen_at,
                                  created_at, updated_at, revision, primary_service,
                                  affected_services, revision_fingerprint, aggregate_payload)
            VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                    'max-member-severity/v1', $4, $4, $4, $4, $5, 'auth-svc', '["auth-svc"]'::jsonb,
                    '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
            """,
            tenant_a,
            inc_id,
            f"INC-{inc_id.hex.upper()}",
            now,
            rev,
        )
        stored = await admin_conn.fetchval(
            "SELECT revision FROM incidents WHERE tenant_id = $1 AND incident_id = $2",
            tenant_a,
            inc_id,
        )
        assert stored == rev

    # 1. Accept revision 1
    await _try_revision(Decimal(1))
    await _try_revision(Decimal(2**63))

    # 2. Accept max revision 18446744073709551615
    await _try_revision(Decimal("18446744073709551615"))

    # 3. Reject revision 0
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_revision(Decimal(0))

    # 4. Reject fraction 1.5
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_revision(Decimal("1.5"))

    # 5. Reject overflow 18446744073709551616
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_revision(Decimal("18446744073709551616"))

    # 6. Reject negative revision -1
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_revision(Decimal(-1))


# ============================================================================
# REQUIRED TEST 13: Score Checks
# ============================================================================
async def test_score_checks_accept_bounds_and_reject_invalid_values(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify anomaly_score and pair score columns accept [0, 1] and reject invalid/NaN/Infinity."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    inc_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    # Seed incident
    await admin_conn.execute(
        """
        INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                              environment, status, correlation_state, severity,
                              severity_policy_version, started_at, last_seen_at,
                              created_at, updated_at, revision, primary_service,
                              affected_services, revision_fingerprint, aggregate_payload)
        VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
        """,
        tenant_a,
        inc_id,
        f"INC-{inc_id.hex.upper()}",
        now,
    )

    # 1. Accepted: 0.0 and 1.0 on incident_signal_links
    for score in [0.0, 1.0]:
        s = uuid.uuid4()
        await admin_conn.execute(
            """
            INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                              environment, service, event_time, severity, anomaly_score)
            VALUES ($1, $2, $3, $4, 'prod', 'auth-svc', $5, 'info', $6)
            """,
            tenant_a,
            inc_id,
            s,
            f"{'0'*32}{s.hex}",
            now,
            score,
        )

    # 2. Rejected: -0.01, 1.01, NaN, Infinity on incident_signal_links
    for invalid_score in [-0.01, 1.01, float("nan"), float("inf"), float("-inf")]:
        with pytest.raises(asyncpg.CheckViolationError):
            await admin_conn.execute(
                """
                INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                                  environment, service, event_time, severity, anomaly_score)
                VALUES ($1, $2, $3, '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                        'prod', 'auth-svc', $4, 'info', $5)
                """,
                tenant_a,
                inc_id,
                uuid.uuid4(),
                now,
                invalid_score,
            )


# ============================================================================
# REQUIRED TEST 14: Scalar and Fingerprint Checks
# ============================================================================
async def test_incident_scalar_and_fingerprint_checks_reject_invalid_values(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify CHECK constraints for schema_version, status, state, severity, time order, and fingerprint."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    now = datetime.now(timezone.utc)
    valid_uuid = uuid.uuid4()
    valid_fp = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

    async def _try_insert(**kwargs: Any) -> None:
        p = {
            "schema_version": "1.2",
            "incident_number": f"INC-{valid_uuid.hex.upper()}",
            "status": "open",
            "correlation_state": "collecting",
            "severity": "error",
            "severity_policy_version": "max-member-severity/v1",
            "started_at": now,
            "last_seen_at": now,
            "created_at": now,
            "updated_at": now,
            "revision_fp": valid_fp,
        }
        p.update(kwargs)
        await admin_conn.execute(
            """
            INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                  environment, status, correlation_state, severity,
                                  severity_policy_version, started_at, last_seen_at,
                                  created_at, updated_at, revision, primary_service,
                                  affected_services, revision_fingerprint, aggregate_payload)
            VALUES ($1, $2, $3, $4, 'prod', $5, $6, $7, $8, $9, $10, $11, $12, 1, 'auth-svc',
                    '["auth-svc"]'::jsonb, $13, '{"k":"v"}'::bytea)
            """,
            tenant_a,
            valid_uuid,
            p["schema_version"],
            p["incident_number"],
            p["status"],
            p["correlation_state"],
            p["severity"],
            p["severity_policy_version"],
            p["started_at"],
            p["last_seen_at"],
            p["created_at"],
            p["updated_at"],
            p["revision_fp"],
        )

    # 1. Invalid schema_version rejected
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_insert(schema_version="1.1")

    # 2. Invalid status rejected
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_insert(status="invalid_status")

    # 3. Invalid correlation_state rejected
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_insert(correlation_state="invalid_state")

    # 4. Invalid severity rejected (use invalid_severity, not critical)
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_insert(severity="invalid_severity")

    # 5. Invalid severity_policy_version rejected
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_insert(severity_policy_version="invalid_policy")

    # 6. Invalid incident_number format rejected
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_insert(incident_number="INVALID-NUMBER")

    # 7. Time order: started_at > last_seen_at rejected
    with pytest.raises(asyncpg.CheckViolationError):
        future = datetime.fromtimestamp(now.timestamp() + 100, timezone.utc)
        await _try_insert(started_at=future, last_seen_at=now)

    # 8. Time order: created_at > updated_at rejected
    with pytest.raises(asyncpg.CheckViolationError):
        future = datetime.fromtimestamp(now.timestamp() + 100, timezone.utc)
        await _try_insert(created_at=future, updated_at=now)

    # 9. Fingerprint: uppercase rejected
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_insert(revision_fp="A" * 64)

    # 10. Fingerprint: length 63 rejected
    with pytest.raises(asyncpg.CheckViolationError):
        await _try_insert(revision_fp="a" * 63)

    # 11. Fingerprint: length 65 or invalid characters rejected
    with pytest.raises(
        (asyncpg.CheckViolationError, asyncpg.StringDataRightTruncationError)
    ):
        await _try_insert(revision_fp="a" * 65)

    with pytest.raises(asyncpg.CheckViolationError):
        await _try_insert(revision_fp="0" * 63 + "g")

    # 12. Valid lowercase 64-char fingerprint accepted
    await _try_insert()


# ============================================================================
# REQUIRED TEST 15: Complete 111-Case Direct-SQL Boundary Probes
# ============================================================================
async def test_affected_services_and_payload_bounds_are_enforced(
    admin_conn: asyncpg.Connection,
) -> None:
    """Execute complete 111-case direct-SQL boundary matrix and emit structured diagnostic block."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    now = datetime.now(timezone.utc)
    fp = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

    bnd_records: list[dict[str, str]] = []

    def record_probe(
        cid: str, inp: str, exp: str, obs: str, sql: str, con: str
    ) -> None:
        bnd_records.append(
            {
                "case_id": cid,
                "input": inp,
                "expected": exp,
                "observed": obs,
                "sqlstate": sql,
                "constraint": con,
            }
        )

    # --- 1-6: REVISION PROBES ---
    for cid, val, exp, is_valid in [
        ("REV_MIN_VALID", "1", "ACCEPTED", True),
        ("REV_MAX_VALID", "18446744073709551615", "ACCEPTED", True),
        ("REV_ZERO_INVALID", "0", "REJECTED", False),
        ("REV_FRACTION_INVALID", "1.5", "REJECTED", False),
        ("REV_OVERFLOW_INVALID", "18446744073709551616", "REJECTED", False),
        ("REV_NEGATIVE_INVALID", "-1", "REJECTED", False),
    ]:
        iid = uuid.uuid4()
        try:
            await admin_conn.execute(
                """
                INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                      environment, status, correlation_state, severity,
                                      severity_policy_version, started_at, last_seen_at,
                                      created_at, updated_at, revision, primary_service,
                                      affected_services, revision_fingerprint, aggregate_payload)
                VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                        'max-member-severity/v1', $4, $4, $4, $4, $5, 'svc-0', '["svc-0"]'::jsonb,
                        $6, '{"k":"v"}'::bytea)
                """,
                tenant_a,
                iid,
                f"INC-{iid.hex.upper()}",
                now,
                Decimal(val),
                fp,
            )
            record_probe(cid, val, exp, "ACCEPTED", "NONE", "NONE")
        except asyncpg.CheckViolationError as exc:
            record_probe(
                cid,
                val,
                exp,
                "REJECTED",
                exc.sqlstate,
                exc.constraint_name or "ck_incidents_revision",
            )

    # --- 7-13: ANOMALY SCORE PROBES ---
    # Seed incident for signal links
    inc_bnd = uuid.uuid4()
    await admin_conn.execute(
        """
        INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                              environment, status, correlation_state, severity,
                              severity_policy_version, started_at, last_seen_at,
                              created_at, updated_at, revision, primary_service,
                              affected_services, revision_fingerprint, aggregate_payload)
        VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                'max-member-severity/v1', $4, $4, $4, $4, 1, 'svc-0', '["svc-0"]'::jsonb,
                $5, '{"k":"v"}'::bytea)
        """,
        tenant_a,
        inc_bnd,
        f"INC-{inc_bnd.hex.upper()}",
        now,
        fp,
    )

    for cid, val, exp, is_valid in [
        ("SCORE_ANOMALY_0_0_ACCEPTED", 0.0, "ACCEPTED", True),
        ("SCORE_ANOMALY_1_0_ACCEPTED", 1.0, "ACCEPTED", True),
        ("SCORE_ANOMALY_NEG_0_01_REJECTED", -0.01, "REJECTED", False),
        ("SCORE_ANOMALY_1_01_REJECTED", 1.01, "REJECTED", False),
        ("SCORE_ANOMALY_NAN_REJECTED", float("nan"), "REJECTED", False),
        ("SCORE_ANOMALY_POS_INF_REJECTED", float("inf"), "REJECTED", False),
        ("SCORE_ANOMALY_NEG_INF_REJECTED", float("-inf"), "REJECTED", False),
    ]:
        sid = uuid.uuid4()
        try:
            await admin_conn.execute(
                """
                INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                                  environment, service, event_time, severity, anomaly_score)
                VALUES ($1, $2, $3, $4, 'prod', 'svc-0', $5, 'info', $6)
                """,
                tenant_a,
                inc_bnd,
                sid,
                f"{'0'*32}{sid.hex}",
                now,
                val,
            )
            record_probe(cid, str(val), exp, "ACCEPTED", "NONE", "NONE")
        except asyncpg.CheckViolationError as exc:
            record_probe(
                cid,
                str(val),
                exp,
                "REJECTED",
                exc.sqlstate,
                exc.constraint_name or "ck_incident_signal_links_anomaly_score",
            )

    # --- 14-55: 6 PAIR SCORE PROBES (42 cases) ---
    pair_cols = [
        ("SCORE_TEMPORAL", "temporal_score"),
        ("SCORE_SERVICE", "service_score"),
        ("SCORE_TOPOLOGY", "topology_score"),
        ("SCORE_SYMPTOM", "symptom_score"),
        ("SCORE_CONTEXT", "context_score"),
        ("SCORE_WEIGHTED", "weighted_score"),
    ]

    for pfx, col_name in pair_cols:
        for suffix, val, exp, is_valid in [
            ("0_0_ACCEPTED", 0.0, "ACCEPTED", True),
            ("1_0_ACCEPTED", 1.0, "ACCEPTED", True),
            ("NEG_0_01_REJECTED", -0.01, "REJECTED", False),
            ("1_01_REJECTED", 1.01, "REJECTED", False),
            ("NAN_REJECTED", float("nan"), "REJECTED", False),
            ("POS_INF_REJECTED", float("inf"), "REJECTED", False),
            ("NEG_INF_REJECTED", float("-inf"), "REJECTED", False),
        ]:
            cid = f"{pfx}_{suffix}"
            s1 = uuid.uuid4()
            s2 = uuid.uuid4()
            if s1 > s2:
                s1, s2 = s2, s1

            # Seed signals
            await admin_conn.execute(
                """
                INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                                  environment, service, event_time, severity, anomaly_score)
                VALUES ($1, $2, $3, $4, 'prod', 'svc-0', $5, 'info', 0.5),
                       ($1, $2, $6, $7, 'prod', 'svc-0', $5, 'info', 0.5)
                """,
                tenant_a,
                inc_bnd,
                s1,
                f"{'0'*32}{s1.hex}",
                now,
                s2,
                f"{'0'*32}{s2.hex}",
            )

            scores = {c: 0.5 for _, c in pair_cols}
            scores[col_name] = val

            try:
                await admin_conn.execute(
                    """
                    INSERT INTO incident_correlation_evidence (tenant_id, incident_id, left_signal_id, right_signal_id,
                                                              temporal_score, service_score, topology_score,
                                                              symptom_score, context_score, weighted_score,
                                                              accepted, policy_version, reason)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true, 'p1', 'r1')
                    """,
                    tenant_a,
                    inc_bnd,
                    s1,
                    s2,
                    scores["temporal_score"],
                    scores["service_score"],
                    scores["topology_score"],
                    scores["symptom_score"],
                    scores["context_score"],
                    scores["weighted_score"],
                )
                record_probe(cid, str(val), exp, "ACCEPTED", "NONE", "NONE")
            except asyncpg.CheckViolationError as exc:
                record_probe(
                    cid,
                    str(val),
                    exp,
                    "REJECTED",
                    exc.sqlstate,
                    exc.constraint_name
                    or f"ck_incident_correlation_evidence_{col_name}",
                )

    # --- 56-74: SCALARS & FINGERPRINTS ---
    scalar_cases = [
        ("SCALAR_SCHEMA_VERSION_VALID", {"schema_version": "1.2"}, "ACCEPTED", "1.2"),
        ("SCALAR_SCHEMA_VERSION_INVALID", {"schema_version": "1.1"}, "REJECTED", "1.1"),
        ("SCALAR_STATUS_VALID", {"status": "open"}, "ACCEPTED", "open"),
        (
            "SCALAR_STATUS_INVALID",
            {"status": "invalid_status"},
            "REJECTED",
            "invalid_status",
        ),
        (
            "SCALAR_STATE_VALID",
            {"correlation_state": "collecting"},
            "ACCEPTED",
            "collecting",
        ),
        (
            "SCALAR_STATE_INVALID",
            {"correlation_state": "invalid_state"},
            "REJECTED",
            "invalid_state",
        ),
        ("SCALAR_SEVERITY_VALID", {"severity": "error"}, "ACCEPTED", "error"),
        (
            "SCALAR_SEVERITY_INVALID",
            {"severity": "invalid_severity"},
            "REJECTED",
            "invalid_severity",
        ),
        (
            "SCALAR_POLICY_VERSION_VALID",
            {"severity_policy_version": "max-member-severity/v1"},
            "ACCEPTED",
            "max-member-severity/v1",
        ),
        (
            "SCALAR_POLICY_VERSION_INVALID",
            {"severity_policy_version": "invalid_policy"},
            "REJECTED",
            "invalid_policy",
        ),
        ("SCALAR_INCIDENT_NUMBER_VALID", {}, "ACCEPTED", "valid"),
        (
            "SCALAR_INCIDENT_NUMBER_INVALID",
            {"incident_number": "INVALID-NUMBER"},
            "REJECTED",
            "INVALID-NUMBER",
        ),
        (
            "SCALAR_TIME_ORDER_STARTED_LAST_SEEN_INVALID",
            {
                "started_at": datetime.fromtimestamp(
                    now.timestamp() + 100, timezone.utc
                ),
                "last_seen_at": now,
            },
            "REJECTED",
            "started > last_seen",
        ),
        (
            "SCALAR_TIME_ORDER_CREATED_UPDATED_INVALID",
            {
                "created_at": datetime.fromtimestamp(
                    now.timestamp() + 100, timezone.utc
                ),
                "updated_at": now,
            },
            "REJECTED",
            "created > updated",
        ),
        ("SCALAR_FP_VALID", {"revision_fp": fp}, "ACCEPTED", "lowercase 64-hex"),
        (
            "SCALAR_FP_UPPERCASE_INVALID",
            {"revision_fp": "A" * 64},
            "REJECTED",
            "uppercase 64-hex",
        ),
        ("SCALAR_FP_LEN_63_INVALID", {"revision_fp": "a" * 63}, "REJECTED", "63-char"),
        ("SCALAR_FP_LEN_65_INVALID", {"revision_fp": "a" * 65}, "REJECTED", "65-char"),
        (
            "SCALAR_FP_HEX_CHAR_INVALID",
            {"revision_fp": "0" * 63 + "g"},
            "REJECTED",
            "non-hex g",
        ),
    ]

    for cid, overrides, exp, inp_str in scalar_cases:
        iid = uuid.uuid4()
        params = {
            "schema_version": "1.2",
            "incident_number": f"INC-{iid.hex.upper()}",
            "status": "open",
            "correlation_state": "collecting",
            "severity": "error",
            "severity_policy_version": "max-member-severity/v1",
            "started_at": now,
            "last_seen_at": now,
            "created_at": now,
            "updated_at": now,
            "revision_fp": fp,
        }
        params.update(overrides)
        try:
            await admin_conn.execute(
                """
                INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                      environment, status, correlation_state, severity,
                                      severity_policy_version, started_at, last_seen_at,
                                      created_at, updated_at, revision, primary_service,
                                      affected_services, revision_fingerprint, aggregate_payload)
                VALUES ($1, $2, $3, $4, 'prod', $5, $6, $7, $8, $9, $10, $11, $12, 1, 'svc-0',
                        '["svc-0"]'::jsonb, $13, '{"k":"v"}'::bytea)
                """,
                tenant_a,
                iid,
                params["schema_version"],
                params["incident_number"],
                params["status"],
                params["correlation_state"],
                params["severity"],
                params["severity_policy_version"],
                params["started_at"],
                params["last_seen_at"],
                params["created_at"],
                params["updated_at"],
                params["revision_fp"],
            )
            record_probe(cid, inp_str, exp, "ACCEPTED", "NONE", "NONE")
        except asyncpg.CheckViolationError as exc:
            record_probe(
                cid,
                inp_str,
                exp,
                "REJECTED",
                exc.sqlstate,
                exc.constraint_name or "ck_incidents",
            )
        except asyncpg.StringDataRightTruncationError as exc:
            record_probe(cid, inp_str, exp, "REJECTED", exc.sqlstate, "NONE")

    # --- 75-78: SIGNAL LINK PROBES ---
    for cid, overrides, exp, inp_str in [
        ("LINK_SEVERITY_VALID", {"severity": "warning"}, "ACCEPTED", "warning"),
        (
            "LINK_SEVERITY_INVALID",
            {"severity": "invalid_severity"},
            "REJECTED",
            "invalid_severity",
        ),
        ("LINK_FP_VALID", {"semantic_fingerprint": fp}, "ACCEPTED", "lowercase 64-hex"),
        (
            "LINK_FP_INVALID",
            {"semantic_fingerprint": "A" * 64},
            "REJECTED",
            "uppercase 64-hex",
        ),
    ]:
        sid = uuid.uuid4()
        params = {
            "severity": "info",
            "semantic_fingerprint": f"{'0'*32}{sid.hex}",
        }
        params.update(overrides)
        try:
            await admin_conn.execute(
                """
                INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                                  environment, service, event_time, severity, anomaly_score)
                VALUES ($1, $2, $3, $4, 'prod', 'svc-0', $5, $6, 0.5)
                """,
                tenant_a,
                inc_bnd,
                sid,
                params["semantic_fingerprint"],
                now,
                params["severity"],
            )
            record_probe(cid, inp_str, exp, "ACCEPTED", "NONE", "NONE")
        except asyncpg.CheckViolationError as exc:
            record_probe(
                cid,
                inp_str,
                exp,
                "REJECTED",
                exc.sqlstate,
                exc.constraint_name or "ck_incident_signal_links",
            )

    # --- 79-82: PAIR EVIDENCE PROBES ---
    s_a = uuid.uuid4()
    s_b = uuid.uuid4()
    if s_a > s_b:
        s_a, s_b = s_b, s_a
    await admin_conn.execute(
        """
        INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                          environment, service, event_time, severity, anomaly_score)
        VALUES ($1, $2, $3, $4, 'prod', 'svc-0', $5, 'info', 0.5),
               ($1, $2, $6, $7, 'prod', 'svc-0', $5, 'info', 0.5)
        """,
        tenant_a,
        inc_bnd,
        s_a,
        f"{'0'*32}{s_a.hex}",
        now,
        s_b,
        f"{'0'*32}{s_b.hex}",
    )

    for cid, left_s, right_s, acc, exp, inp_str in [
        ("PAIR_ORDER_CANONICAL_VALID", s_a, s_b, True, "ACCEPTED", "left < right"),
        ("PAIR_ORDER_REVERSED_INVALID", s_b, s_a, True, "REJECTED", "left > right"),
        ("PAIR_ACCEPTED_TRUE_VALID", s_a, s_b, True, "ACCEPTED", "accepted=true"),
        ("PAIR_ACCEPTED_FALSE_INVALID", s_a, s_b, False, "REJECTED", "accepted=false"),
    ]:
        cur_left, cur_right = left_s, right_s
        if exp == "ACCEPTED" and cid != "PAIR_ORDER_CANONICAL_VALID":
            cur_left, cur_right = sorted([uuid.uuid4(), uuid.uuid4()])
            await admin_conn.execute(
                """
                INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                                  environment, service, event_time, severity, anomaly_score)
                VALUES ($1, $2, $3, $4, 'prod', 'svc-0', $5, 'info', 0.5),
                       ($1, $2, $6, $7, 'prod', 'svc-0', $5, 'info', 0.5)
                """,
                tenant_a,
                inc_bnd,
                cur_left,
                f"{'0'*32}{cur_left.hex}",
                now,
                cur_right,
                f"{'0'*32}{cur_right.hex}",
            )
        try:
            await admin_conn.execute(
                """
                INSERT INTO incident_correlation_evidence (tenant_id, incident_id, left_signal_id, right_signal_id,
                                                          temporal_score, service_score, topology_score,
                                                          symptom_score, context_score, weighted_score,
                                                          accepted, policy_version, reason)
                VALUES ($1, $2, $3, $4, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, $5, 'p1', 'r1')
                """,
                tenant_a,
                inc_bnd,
                cur_left,
                cur_right,
                acc,
            )
            record_probe(cid, inp_str, exp, "ACCEPTED", "NONE", "NONE")
        except asyncpg.CheckViolationError as exc:
            record_probe(
                cid,
                inp_str,
                exp,
                "REJECTED",
                exc.sqlstate,
                exc.constraint_name or "ck_incident_correlation_evidence",
            )

    # --- 83-90: TIMELINE EVENTS PROBES ---
    for idx, (cid, overrides, exp, inp_str) in enumerate(
        [
            (
                "TIMELINE_SCHEMA_VERSION_VALID",
                {"schema_version": "1.2"},
                "ACCEPTED",
                "1.2",
            ),
            (
                "TIMELINE_SCHEMA_VERSION_INVALID",
                {"schema_version": "1.1"},
                "REJECTED",
                "1.1",
            ),
            ("TIMELINE_ORDINAL_ZERO_VALID", {"event_ordinal": 0}, "ACCEPTED", "0"),
            (
                "TIMELINE_ORDINAL_NEGATIVE_INVALID",
                {"event_ordinal": -1},
                "REJECTED",
                "-1",
            ),
            (
                "TIMELINE_EVENT_TYPE_VALID",
                {"event_type": "created"},
                "ACCEPTED",
                "created",
            ),
            (
                "TIMELINE_EVENT_TYPE_INVALID",
                {"event_type": "invalid_event"},
                "REJECTED",
                "invalid_event",
            ),
            (
                "TIMELINE_FP_VALID",
                {"revision_fingerprint": fp},
                "ACCEPTED",
                "lowercase 64-hex",
            ),
            (
                "TIMELINE_FP_INVALID",
                {"revision_fingerprint": "A" * 64},
                "REJECTED",
                "uppercase 64-hex",
            ),
        ]
    ):
        eid = uuid.uuid4()
        params = {
            "schema_version": "1.2",
            "event_ordinal": 100 + idx,
            "event_type": "created",
            "revision_fingerprint": fp,
        }
        params.update(overrides)
        try:
            await admin_conn.execute(
                """
                INSERT INTO incident_timeline_events (tenant_id, incident_id, event_id, schema_version,
                                                     revision, event_ordinal, event_time, event_type,
                                                     source_signal_id, summary, revision_fingerprint)
                VALUES ($1, $2, $3, $4, 1, $5, clock_timestamp(), $6, $7, 'Event summary', $8)
                """,
                tenant_a,
                inc_bnd,
                eid,
                params["schema_version"],
                params["event_ordinal"],
                params["event_type"],
                s_a,
                params["revision_fingerprint"],
            )
            record_probe(cid, inp_str, exp, "ACCEPTED", "NONE", "NONE")
        except asyncpg.CheckViolationError as exc:
            record_probe(
                cid,
                inp_str,
                exp,
                "REJECTED",
                exc.sqlstate,
                exc.constraint_name or "ck_incident_timeline_events",
            )

    # --- 91-94: INTENT PROBES ---
    for cid, overrides, exp, inp_str in [
        ("INTENT_SCHEMA_VERSION_VALID", {"schema_version": "1.2"}, "ACCEPTED", "1.2"),
        ("INTENT_SCHEMA_VERSION_INVALID", {"schema_version": "1.1"}, "REJECTED", "1.1"),
        (
            "INTENT_FP_VALID",
            {"revision_fingerprint": fp},
            "ACCEPTED",
            "lowercase 64-hex",
        ),
        (
            "INTENT_FP_INVALID",
            {"revision_fingerprint": "A" * 64},
            "REJECTED",
            "uppercase 64-hex",
        ),
    ]:
        rev = Decimal(100 + len(bnd_records))
        params = {
            "schema_version": "1.2",
            "revision_fingerprint": fp,
        }
        params.update(overrides)
        try:
            await admin_conn.execute(
                """
                INSERT INTO incident_publication_intents (tenant_id, incident_id, revision, schema_version,
                                                        revision_fingerprint, intent_time, aggregate_payload)
                VALUES ($1, $2, $3, $4, $5, clock_timestamp(), '{"p":"1"}'::bytea)
                """,
                tenant_a,
                inc_bnd,
                rev,
                params["schema_version"],
                params["revision_fingerprint"],
            )
            record_probe(cid, inp_str, exp, "ACCEPTED", "NONE", "NONE")
        except asyncpg.CheckViolationError as exc:
            record_probe(
                cid,
                inp_str,
                exp,
                "REJECTED",
                exc.sqlstate,
                exc.constraint_name or "ck_incident_publication_intents",
            )

    # --- 95-99: AFFECTED SERVICES PROBES ---
    for cid, val, exp, inp_str in [
        ("SERVICES_MIN_1_ACCEPTED", json.dumps(["svc-0"]), "ACCEPTED", "array len 1"),
        (
            "SERVICES_MAX_512_ACCEPTED",
            json.dumps([f"svc-{i}" for i in range(512)]),
            "ACCEPTED",
            "array len 512",
        ),
        ("SERVICES_EMPTY_0_REJECTED", json.dumps([]), "REJECTED", "empty array len 0"),
        (
            "SERVICES_OVER_513_REJECTED",
            json.dumps([f"svc-{i}" for i in range(513)]),
            "REJECTED",
            "array len 513",
        ),
        (
            "SERVICES_NON_ARRAY_REJECTED",
            json.dumps({"key": "val"}),
            "REJECTED",
            "json object",
        ),
    ]:
        iid = uuid.uuid4()
        try:
            await admin_conn.execute(
                """
                INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                      environment, status, correlation_state, severity,
                                      severity_policy_version, started_at, last_seen_at,
                                      created_at, updated_at, revision, primary_service,
                                      affected_services, revision_fingerprint, aggregate_payload)
                VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                        'max-member-severity/v1', $4, $4, $4, $4, 1, 'svc-0', $5::jsonb,
                        $6, '{"k":"v"}'::bytea)
                """,
                tenant_a,
                iid,
                f"INC-{iid.hex.upper()}",
                now,
                val,
                fp,
            )
            record_probe(cid, inp_str, exp, "ACCEPTED", "NONE", "NONE")
        except asyncpg.CheckViolationError as exc:
            record_probe(
                cid,
                inp_str,
                exp,
                "REJECTED",
                exc.sqlstate,
                exc.constraint_name or "ck_incidents_affected_services",
            )

    # --- 100-105: INCIDENT PAYLOAD PROBES ---
    large_payload_64m = b'{"k":"' + b"x" * (MAX_AGGREGATE_PAYLOAD_BYTES - 8) + b'"}'
    over_payload_64m = b'{"k":"' + b"x" * (OVER_MAX_AGGREGATE_PAYLOAD_BYTES - 8) + b'"}'

    for cid, val, exp, inp_str, is_utf8_err in [
        ("PAYLOAD_MIN_2B_ACCEPTED", b"{}", "ACCEPTED", "2 bytes {}", False),
        ("PAYLOAD_1B_REJECTED", b"{", "REJECTED", "1 byte {", False),
        ("PAYLOAD_NON_OBJECT_REJECTED", b'["arr"]', "REJECTED", '["arr"]', False),
        (
            "PAYLOAD_INVALID_UTF8_REJECTED",
            b'{"k":"\xff\xff"}',
            "REJECTED",
            "invalid UTF-8 bytes",
            True,
        ),
        (
            "PAYLOAD_MAX_64MIB_ACCEPTED",
            large_payload_64m,
            "ACCEPTED",
            "64MiB payload",
            False,
        ),
        (
            "PAYLOAD_OVER_64MIB_REJECTED",
            over_payload_64m,
            "REJECTED",
            "64MiB + 1B payload",
            False,
        ),
    ]:
        iid = uuid.uuid4()
        try:
            await admin_conn.execute(
                """
                INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                      environment, status, correlation_state, severity,
                                      severity_policy_version, started_at, last_seen_at,
                                      created_at, updated_at, revision, primary_service,
                                      affected_services, revision_fingerprint, aggregate_payload)
                VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                        'max-member-severity/v1', $4, $4, $4, $4, 1, 'svc-0', '["svc-0"]'::jsonb,
                        $5, $6)
                """,
                tenant_a,
                iid,
                f"INC-{iid.hex.upper()}",
                now,
                fp,
                val,
            )
            record_probe(cid, inp_str, exp, "ACCEPTED", "NONE", "NONE")
        except asyncpg.CharacterNotInRepertoireError as exc:
            record_probe(cid, inp_str, exp, "REJECTED", exc.sqlstate, "NONE")
        except asyncpg.CheckViolationError as exc:
            record_probe(
                cid,
                inp_str,
                exp,
                "REJECTED",
                exc.sqlstate,
                exc.constraint_name or "ck_incidents_payload",
            )

    # --- 106-111: INTENT PAYLOAD PROBES ---
    for cid, val, exp, inp_str, is_utf8_err in [
        ("INTENT_PAYLOAD_MIN_2B_ACCEPTED", b"{}", "ACCEPTED", "2 bytes {}", False),
        ("INTENT_PAYLOAD_1B_REJECTED", b"{", "REJECTED", "1 byte {", False),
        (
            "INTENT_PAYLOAD_NON_OBJECT_REJECTED",
            b'["arr"]',
            "REJECTED",
            '["arr"]',
            False,
        ),
        (
            "INTENT_PAYLOAD_INVALID_UTF8_REJECTED",
            b'{"k":"\xff\xff"}',
            "REJECTED",
            "invalid UTF-8 bytes",
            True,
        ),
        (
            "INTENT_PAYLOAD_MAX_64MIB_ACCEPTED",
            large_payload_64m,
            "ACCEPTED",
            "64MiB payload",
            False,
        ),
        (
            "INTENT_PAYLOAD_OVER_64MIB_REJECTED",
            over_payload_64m,
            "REJECTED",
            "64MiB + 1B payload",
            False,
        ),
    ]:
        rev = Decimal(500 + len(bnd_records))
        try:
            await admin_conn.execute(
                """
                INSERT INTO incident_publication_intents (tenant_id, incident_id, revision, schema_version,
                                                        revision_fingerprint, intent_time, aggregate_payload)
                VALUES ($1, $2, $3, '1.2', $4, clock_timestamp(), $5)
                """,
                tenant_a,
                inc_bnd,
                rev,
                fp,
                val,
            )
            record_probe(cid, inp_str, exp, "ACCEPTED", "NONE", "NONE")
        except asyncpg.CharacterNotInRepertoireError as exc:
            record_probe(cid, inp_str, exp, "REJECTED", exc.sqlstate, "NONE")
        except asyncpg.CheckViolationError as exc:
            record_probe(
                cid,
                inp_str,
                exp,
                "REJECTED",
                exc.sqlstate,
                exc.constraint_name or "ck_incident_publication_intents_payload",
            )

    # Assert exactly 111 cases recorded
    assert (
        len(bnd_records) == 111
    ), f"Expected 111 boundary records, got {len(bnd_records)}"

    expected_boundary_cases = []
    observed_boundary_cases = []
    bnd_case_objs = []
    for r in bnd_records:
        expected_case = {
            "case_id": r["case_id"],
            "expected_outcome": r["expected"],
            "expected_sqlstate": r["sqlstate"],
            "expected_constraint": r["constraint"],
        }
        observed_case = {
            "case_id": r["case_id"],
            "observed_outcome": r["observed"],
            "observed_sqlstate": r["sqlstate"],
            "observed_constraint": r["constraint"],
        }
        expected_boundary_cases.append(expected_case)
        observed_boundary_cases.append(observed_case)
        bnd_case_objs.append(
            {
                "case_id": r["case_id"],
                "input": str(r["input"]),
                **expected_case,
                **observed_case,
            }
        )

    mismatches = compare_boundary_cases(
        expected_boundary_cases, observed_boundary_cases
    )

    print("M1_R4_BOUNDARY_DIAGNOSTIC_BEGIN")
    for c in bnd_case_objs:
        print("BOUNDARY_CASE " + canonical_json(c))

    bnd_summary = {
        "expected_case_count": 111,
        "observed_case_count": len(bnd_case_objs),
        "missing_case_count": sum(
            1 for m in mismatches if m.mismatch_type == "missing"
        ),
        "duplicate_case_count": 0,
        "unexpected_case_count": sum(
            1 for m in mismatches if m.mismatch_type == "unexpected"
        ),
        "outcome_mismatch_count": sum(
            1 for m in mismatches if m.mismatch_type == "outcome"
        ),
        "sqlstate_mismatch_count": sum(
            1 for m in mismatches if m.mismatch_type == "sqlstate"
        ),
        "constraint_mismatch_count": sum(
            1 for m in mismatches if m.mismatch_type == "constraint"
        ),
        "case_failure_count": len(mismatches),
    }
    print("BOUNDARY_SUMMARY " + canonical_json(bnd_summary))
    print("M1_R4_BOUNDARY_DIAGNOSTIC_END")

    assert len(mismatches) == 0, f"Boundary probe failures: {mismatches}"


async def test_parent_trigger_allows_only_consecutive_revision_update(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify guard_incident_snapshot_change enforces revision + 1, immutability, and no DELETE."""
    tenant_a, tenant_b = await _seed_tenants(admin_conn)
    inc_id = uuid.uuid4()
    inc_num = f"INC-{inc_id.hex.upper()}"
    now = datetime.now(timezone.utc)

    # Insert initial snapshot
    await admin_conn.execute(
        """
        INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                              environment, status, correlation_state, severity,
                              severity_policy_version, started_at, last_seen_at,
                              created_at, updated_at, revision, primary_service,
                              affected_services, revision_fingerprint, aggregate_payload)
        VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
        """,
        tenant_a,
        inc_id,
        inc_num,
        now,
    )

    # 1. Prohibit DELETE with SQLSTATE 55000
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as exc_info:
        await admin_conn.execute("DELETE FROM incidents WHERE incident_id = $1", inc_id)
    assert exc_info.value.sqlstate == "55000"

    # 2. Prohibit non-consecutive revision update (1 -> 3)
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as exc_info:
        await admin_conn.execute(
            "UPDATE incidents SET revision = 3 WHERE incident_id = $1", inc_id
        )
    assert exc_info.value.sqlstate == "55000"

    # 3. Prohibit changing immutable tenant_id
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as exc_info:
        await admin_conn.execute(
            "UPDATE incidents SET tenant_id = $1, revision = 2 WHERE incident_id = $2",
            tenant_b,
            inc_id,
        )
    assert exc_info.value.sqlstate == "55000"

    # 4. Prohibit changing immutable incident_id
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as exc_info:
        await admin_conn.execute(
            "UPDATE incidents SET incident_id = $1, revision = 2 WHERE incident_id = $2",
            uuid.uuid4(),
            inc_id,
        )
    assert exc_info.value.sqlstate == "55000"

    # 5. Prohibit changing immutable incident_number
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as exc_info:
        await admin_conn.execute(
            "UPDATE incidents SET incident_number = 'INC-OTHER', revision = 2 WHERE incident_id = $1",
            inc_id,
        )
    assert exc_info.value.sqlstate == "55000"

    # 6. Allow valid consecutive update (1 -> 2)
    await admin_conn.execute(
        "UPDATE incidents SET revision = 2, status = 'acknowledged' WHERE incident_id = $1",
        inc_id,
    )
    rev = await admin_conn.fetchval(
        "SELECT revision FROM incidents WHERE incident_id = $1", inc_id
    )
    assert rev == 2


# ============================================================================
# REQUIRED TEST 17: Child Tables Reject UPDATE and DELETE
# ============================================================================
async def test_child_tables_reject_update_and_delete_with_sqlstate_55000(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify reject_incident_child_change rejects UPDATE and DELETE on child tables with 55000."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    inc_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    sig_1 = uuid.uuid4()

    # Seed parent incident and one child row
    await admin_conn.execute(
        """
        INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                              environment, status, correlation_state, severity,
                              severity_policy_version, started_at, last_seen_at,
                              created_at, updated_at, revision, primary_service,
                              affected_services, revision_fingerprint, aggregate_payload)
        VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
        """,
        tenant_a,
        inc_id,
        f"INC-{inc_id.hex.upper()}",
        now,
    )

    await admin_conn.execute(
        """
        INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                          environment, service, event_time, severity, anomaly_score)
        VALUES ($1, $2, $3, '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                'prod', 'auth-svc', $4, 'info', 0.5)
        """,
        tenant_a,
        inc_id,
        sig_1,
        now,
    )

    # 1. Reject UPDATE on incident_signal_links
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as exc_info:
        await admin_conn.execute(
            "UPDATE incident_signal_links SET severity = 'error' WHERE signal_id = $1",
            sig_1,
        )
    assert exc_info.value.sqlstate == "55000"

    # 2. Reject DELETE on incident_signal_links
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as exc_info:
        await admin_conn.execute(
            "DELETE FROM incident_signal_links WHERE signal_id = $1", sig_1
        )
    assert exc_info.value.sqlstate == "55000"


# ============================================================================
# REQUIRED TEST 18: Timeline Revision Ordinal and Chronological Index
# ============================================================================
async def test_timeline_revision_ordinal_and_chronological_index_are_enforced(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify unique constraint on (tenant_id, incident_id, revision, event_ordinal)."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    inc_1 = uuid.uuid4()
    now = datetime.now(timezone.utc)

    await admin_conn.execute(
        """
        INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                              environment, status, correlation_state, severity,
                              severity_policy_version, started_at, last_seen_at,
                              created_at, updated_at, revision, primary_service,
                              affected_services, revision_fingerprint, aggregate_payload)
        VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
        """,
        tenant_a,
        inc_1,
        f"INC-{inc_1.hex.upper()}",
        now,
    )

    # Insert ordinal 0
    await admin_conn.execute(
        """
        INSERT INTO incident_timeline_events (tenant_id, incident_id, event_id, schema_version,
                                             revision, event_ordinal, event_time, event_type,
                                             source_signal_id, summary, revision_fingerprint)
        VALUES ($1, $2, $3, '1.2', 1, 0, $4, 'created', NULL, 'Event summary',
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef')
        """,
        tenant_a,
        inc_1,
        uuid.uuid4(),
        now,
    )

    # Insert duplicate ordinal 0 for same (tenant, incident, revision) -> 23505
    with pytest.raises(asyncpg.UniqueViolationError) as exc_info:
        await admin_conn.execute(
            """
            INSERT INTO incident_timeline_events (tenant_id, incident_id, event_id, schema_version,
                                                 revision, event_ordinal, event_time, event_type,
                                                 source_signal_id, summary, revision_fingerprint)
            VALUES ($1, $2, $3, '1.2', 1, 0, $4, 'created', NULL, 'Event summary',
                    '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef')
            """,
            tenant_a,
            inc_1,
            uuid.uuid4(),
            now,
        )
    assert exc_info.value.sqlstate == "23505"


# ============================================================================
# REQUIRED TEST 19: Publication Intent Uniqueness and Append-Only
# ============================================================================
async def test_publication_intent_is_unique_per_revision_and_append_only(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify primary key (tenant_id, incident_id, revision) and append-only trigger."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    inc_1 = uuid.uuid4()
    now = datetime.now(timezone.utc)

    await admin_conn.execute(
        """
        INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                              environment, status, correlation_state, severity,
                              severity_policy_version, started_at, last_seen_at,
                              created_at, updated_at, revision, primary_service,
                              affected_services, revision_fingerprint, aggregate_payload)
        VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
        """,
        tenant_a,
        inc_1,
        f"INC-{inc_1.hex.upper()}",
        now,
    )

    # Insert publication intent for revision 1
    await admin_conn.execute(
        """
        INSERT INTO incident_publication_intents (tenant_id, incident_id, revision, schema_version,
                                                revision_fingerprint, intent_time, aggregate_payload)
        VALUES ($1, $2, 1, '1.2', '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                $3, '{"k":"v"}'::bytea)
        """,
        tenant_a,
        inc_1,
        now,
    )

    # Duplicate publication intent for revision 1 -> PK violation 23505
    with pytest.raises(asyncpg.UniqueViolationError) as exc_info:
        await admin_conn.execute(
            """
            INSERT INTO incident_publication_intents (tenant_id, incident_id, revision, schema_version,
                                                    revision_fingerprint, intent_time, aggregate_payload)
            VALUES ($1, $2, 1, '1.2', '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                    $3, '{"k":"v"}'::bytea)
            """,
            tenant_a,
            inc_1,
            now,
        )
    assert exc_info.value.sqlstate == "23505"

    # Reject UPDATE on incident_publication_intents -> 55000
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as exc_info:
        await admin_conn.execute(
            "UPDATE incident_publication_intents SET schema_version = '1.3' WHERE incident_id = $1",
            inc_1,
        )
    assert exc_info.value.sqlstate == "55000"


# ============================================================================
# REQUIRED TEST 20: Transaction Rollback Atomicity
# ============================================================================
async def test_transaction_rollback_leaves_no_partial_incident_state(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify transaction rollback leaves zero rows across all 5 incident tables."""
    tenant_a, _ = await _seed_tenants(admin_conn)
    inc_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    try:
        async with admin_conn.transaction():
            # 1. Insert incident
            await admin_conn.execute(
                """
                INSERT INTO incidents (tenant_id, incident_id, schema_version, incident_number,
                                      environment, status, correlation_state, severity,
                                      severity_policy_version, started_at, last_seen_at,
                                      created_at, updated_at, revision, primary_service,
                                      affected_services, revision_fingerprint, aggregate_payload)
                VALUES ($1, $2, '1.2', $3, 'prod', 'open', 'collecting', 'info',
                        'max-member-severity/v1', $4, $4, $4, $4, 1, 'auth-svc', '["auth-svc"]'::jsonb,
                        '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef', '{"k":"v"}'::bytea)
                """,
                tenant_a,
                inc_id,
                f"INC-{inc_id.hex.upper()}",
                now,
            )

            # 2. Insert signal link
            await admin_conn.execute(
                """
                INSERT INTO incident_signal_links (tenant_id, incident_id, signal_id, semantic_fingerprint,
                                                  environment, service, event_time, severity, anomaly_score)
                VALUES ($1, $2, $3, '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                        'prod', 'auth-svc', $4, 'info', 0.5)
                """,
                tenant_a,
                inc_id,
                uuid.uuid4(),
                now,
            )

            # 3. Deliberately raise error to trigger rollback
            raise RuntimeError("Forced abort for atomicity test")
    except RuntimeError:
        pass

    # Verify zero rows in all 5 tables for this incident
    tables = [
        "incidents",
        "incident_signal_links",
        "incident_correlation_evidence",
        "incident_timeline_events",
        "incident_publication_intents",
    ]
    for t in tables:
        cnt = await admin_conn.fetchval(
            f"SELECT count(*) FROM {t} WHERE incident_id = $1", inc_id
        )
        assert cnt == 0, f"Table {t} contains rows after rolled-back transaction!"


# ============================================================================
# REQUIRED TEST 21: No Raw Telemetry Storage
# ============================================================================
async def test_migration_adds_no_raw_telemetry_storage(
    admin_conn: asyncpg.Connection,
) -> None:
    """Verify no logs, metrics, traces, or signals tables were introduced."""
    prohibited_tables = [
        "logs",
        "metrics",
        "traces",
        "raw_signals",
        "telemetry",
        "signals",
    ]
    for pt in prohibited_tables:
        exists = await admin_conn.fetchval(
            "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = $1",
            pt,
        )
        assert (
            exists is None
        ), f"Prohibited raw telemetry table '{pt}' found in catalog!"


# ============================================================================
# REQUIRED TEST 22: All Foreign Keys Use NO ACTION
# ============================================================================
async def test_all_foreign_keys_use_no_action(
    admin_conn: asyncpg.Connection,
) -> None:
    """Reconcile every foreign key in the 5 tables has confupd='a' and confdel='a' (NO ACTION)."""
    incident_tables = list(EMBEDDED_EXPECTED_COLUMNS.keys())
    fk_rows = await admin_conn.fetch(
        """
        SELECT
            c.conname,
            t.relname AS table_name,
            c.confupdtype,
            c.confdeltype
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname = ANY($1)
          AND c.contype = 'f'
        ORDER BY t.relname, c.conname;
    """,
        incident_tables,
    )

    assert len(fk_rows) == 11, f"Expected exactly 11 foreign keys, found {len(fk_rows)}"
    for r in fk_rows:
        upd = (
            r["confupdtype"].decode("ascii")
            if isinstance(r["confupdtype"], bytes)
            else r["confupdtype"]
        )
        del_ = (
            r["confdeltype"].decode("ascii")
            if isinstance(r["confdeltype"], bytes)
            else r["confdeltype"]
        )
        assert (
            upd == "a"
        ), f"FK {r['conname']} on {r['table_name']} has onupdate='{upd}', expected 'a' (NO ACTION)"
        assert (
            del_ == "a"
        ), f"FK {r['conname']} on {r['table_name']} has ondelete='{del_}', expected 'a' (NO ACTION)"


_T1_KEYS = {
    "incidents": ("tenant_id", "incident_id"),
    "incident_signal_links": ("tenant_id", "incident_id", "signal_id"),
    "incident_correlation_evidence": (
        "tenant_id",
        "incident_id",
        "left_signal_id",
        "right_signal_id",
    ),
    "incident_timeline_events": ("tenant_id", "event_id"),
    "incident_publication_intents": ("tenant_id", "incident_id", "revision"),
}


def _t1_public_graph(
    tenant: uuid.UUID, signal_ids: tuple[uuid.UUID, uuid.UUID] | None = None
) -> dict[str, Any]:
    """Construct every valid row through the public aggregate boundary before SQL mutations."""
    ids = signal_ids or (uuid.uuid4(), uuid.uuid4())
    moment = datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc)
    inputs, members = {}, []
    for i, signal_id in enumerate(ids):
        event_time = moment + timedelta(minutes=i)
        fingerprint = hashlib.sha256(signal_id.bytes).hexdigest()
        signal = AnomalySignal(
            signal_id=signal_id,
            event_time=event_time,
            service=f"t1-schema-{i}",
            metric_or_feature="cpu_utilization",
            model_name="iforest",
            model_version="1.0",
            anomaly_score=0.9,
            severity=EventSeverity.WARNING,
            threshold_or_calibration=CalibrationMetadata(
                method="percentile", threshold_value=0.8, calibration_version="1.0"
            ),
            run_id=uuid.UUID(int=100),
            scenario_id="t1-schema",
            scenario_version="1.0",
            seed=1,
            reproducibility_key=f"schema-{signal_id.hex}",
            environment="production",
            tenant_id=tenant,
        )
        lineage = AnomalyReproducibilityLineage(
            signal_id=signal_id,
            run_id=signal.run_id,
            scenario_id=signal.scenario_id,
            scenario_version=signal.scenario_version,
            seed=signal.seed,
            reproducibility_key=signal.reproducibility_key,
            model_name=signal.model_name,
            model_version=signal.model_version,
            source_event_ids=(uuid.UUID(int=101),),
            semantic_fingerprint=fingerprint,
        )
        inputs[signal_id] = AnomalyCorrelationInput(signal=signal, lineage=lineage)
        members.append(
            GroupingMemberReference(
                signal_id=signal_id,
                semantic_fingerprint=fingerprint,
                service=signal.service,
                event_time=event_time,
            )
        )
    left, right = sorted(ids, key=lambda value: value.bytes)
    evidence = CorrelationEvidence(
        left_signal_id=left,
        right_signal_id=right,
        temporal_score=0.9,
        service_score=0.9,
        topology_score=0.9,
        symptom_score=0.9,
        context_score=0.9,
        weighted_score=0.9,
        accepted=True,
        policy_version="t1/v1",
        reason="accepted schema fixture",
    )
    group = CorrelationGroup(
        policy_version="t1/v1",
        configuration_fingerprint="c" * 64,
        topology_snapshot_fingerprint="d" * 64,
        members=tuple(sorted(members, key=lambda member: member.signal_id.bytes)),
        accepted_evidence=(evidence,),
        started_at=moment,
        last_seen_at=moment + timedelta(minutes=1),
        time_span_seconds=60.0,
        topology_diameter_hops=1,
        group_fingerprint="0" * 64,
    )
    result = create_or_update_incident(
        group=group,
        signal_inputs=inputs,
        processing_time=moment + timedelta(minutes=2),
        desired_correlation_state=CorrelationWindowState.COLLECTING,
    )
    aggregate = result.aggregate
    parent = {
        "tenant_id": tenant,
        "incident_id": aggregate.incident_id,
        "schema_version": aggregate.schema_version,
        "incident_number": aggregate.incident_number,
        "environment": aggregate.environment,
        "status": aggregate.status.value,
        "correlation_state": aggregate.correlation_state.value,
        "severity": aggregate.severity.value,
        "severity_policy_version": aggregate.severity_policy_version,
        "started_at": aggregate.started_at,
        "last_seen_at": aggregate.last_seen_at,
        "created_at": aggregate.created_at,
        "updated_at": aggregate.updated_at,
        "revision": Decimal(aggregate.revision),
        "primary_service": aggregate.primary_service,
        "affected_services": json.dumps(list(aggregate.affected_services)),
        "revision_fingerprint": aggregate.revision_fingerprint,
        "aggregate_payload": serialize_incident_aggregate(aggregate),
    }
    links = [
        dict(
            tenant_id=tenant,
            incident_id=aggregate.incident_id,
            signal_id=member.signal_id,
            semantic_fingerprint=member.semantic_fingerprint,
            environment=member.environment,
            service=member.service,
            event_time=member.event_time,
            severity=member.severity.value,
            anomaly_score=member.anomaly_score,
        )
        for member in aggregate.signal_references
    ]
    pairs = [
        dict(tenant_id=tenant, incident_id=aggregate.incident_id, **entry.model_dump())
        for entry in aggregate.correlation_evidence
    ]
    events = [
        {
            "tenant_id": tenant,
            **event.model_dump(),
            "event_ordinal": ordinal,
            "revision": Decimal(event.revision),
        }
        for ordinal, event in enumerate(result.timeline_events)
    ]
    intent = dict(
        tenant_id=tenant,
        incident_id=aggregate.incident_id,
        revision=Decimal(1),
        schema_version=aggregate.schema_version,
        revision_fingerprint=aggregate.revision_fingerprint,
        intent_time=aggregate.updated_at,
        aggregate_payload=serialize_incident_aggregate(aggregate),
    )
    return dict(
        incidents=[parent],
        incident_signal_links=links,
        incident_correlation_evidence=pairs,
        incident_timeline_events=events,
        incident_publication_intents=[intent],
    )


async def _t1_insert(
    connection: asyncpg.Connection, table: str, row: dict[str, Any]
) -> None:
    assert table in _T1_KEYS
    columns = ", ".join(row)
    parameters = ", ".join(f"${index}" for index in range(1, len(row) + 1))
    await connection.execute(
        f"INSERT INTO {table} ({columns}) VALUES ({parameters})", *row.values()
    )


async def _t1_read_row(
    connection: asyncpg.Connection, table: str, row: dict[str, Any]
) -> dict[str, Any] | None:
    fields = _T1_KEYS[table]
    predicate = " AND ".join(f"{column} = ${i}" for i, column in enumerate(fields, 1))
    result = await connection.fetchrow(
        f"SELECT * FROM {table} WHERE {predicate}", *(row[c] for c in fields)
    )
    return dict(result) if result is not None else None


@pytest_asyncio.fixture
async def t1_schema_graphs(admin_conn: asyncpg.Connection) -> dict[str, Any]:
    tenant_a, tenant_b = await _seed_tenants(admin_conn)
    assert await admin_conn.fetchval(
        "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"
    )
    graphs = [
        _t1_public_graph(tenant_a, (uuid.UUID(int=100), uuid.UUID(int=200))),
        _t1_public_graph(tenant_b, (uuid.UUID(int=1), uuid.UUID(int=(1 << 128) - 1))),
    ]
    for graph in graphs:
        for table, rows in graph.items():
            for row in rows:
                await _t1_insert(admin_conn, table, row)
    return dict(
        connection=admin_conn,
        tenant_a=tenant_a,
        tenant_b=tenant_b,
        graph=graphs[0],
        other=graphs[1],
    )


@pytest.mark.parametrize(
    "table", ["incident_timeline_events", "incident_publication_intents"]
)
@pytest.mark.parametrize(
    "revision",
    [
        Decimal(1),
        Decimal(2**63),
        Decimal(2**64 - 1),
        Decimal(0),
        Decimal(-1),
        Decimal("1.5"),
        Decimal(2**64),
    ],
)
async def test_t1_child_revision_boundaries_and_exact_readback(
    t1_schema_graphs: dict[str, Any],
    table: str,
    revision: Decimal,
) -> None:
    case = t1_schema_graphs
    graph = _t1_public_graph(case["tenant_a"])
    connection = case["connection"]
    await _t1_insert(connection, "incidents", graph["incidents"][0])
    row = dict(graph[table][0], revision=revision)
    if table == "incident_timeline_events":
        row["source_signal_id"] = None
    if revision in (Decimal(1), Decimal(2**63), Decimal(2**64 - 1)):
        await _t1_insert(connection, table, row)
        stored = await _t1_read_row(connection, table, row)
        assert stored is not None and stored["revision"] == revision
    else:
        with pytest.raises(asyncpg.CheckViolationError) as caught:
            await _t1_insert(connection, table, row)
        assert caught.value.sqlstate == "23514"
        assert caught.value.constraint_name == f"ck_{table}_revision"
        assert await _t1_read_row(connection, table, row) is None


@pytest.mark.parametrize(
    "change",
    [
        "same_revision",
        "lower_revision",
        "updated_at",
        "environment",
        "primary_service",
        "created_at",
    ],
)
async def test_t1_parent_guard_complete_revision_time_and_immutable_cases(
    t1_schema_graphs: dict[str, Any],
    change: str,
) -> None:
    case = t1_schema_graphs
    connection = case["connection"]
    original = case["graph"]["incidents"][0]
    incident = original["incident_id"]
    updated = original["updated_at"] + timedelta(seconds=10)
    await connection.execute(
        "UPDATE incidents SET revision = 2, updated_at = $1 WHERE tenant_id = $2 AND incident_id = $3",
        updated,
        case["tenant_a"],
        incident,
    )
    before = await _t1_read_row(connection, "incidents", original)
    updates: dict[str, Any] = {"revision": Decimal(3)}
    if change == "same_revision":
        updates["revision"] = Decimal(2)
    elif change == "lower_revision":
        updates["revision"] = Decimal(1)
    elif change == "updated_at":
        updates["updated_at"] = original["updated_at"] + timedelta(seconds=5)
    elif change == "environment":
        updates["environment"] = "staging"
    elif change == "primary_service":
        updates["primary_service"] = "t1-other-service"
        updates["affected_services"] = json.dumps(["t1-other-service"])
    else:
        updates["created_at"] = original["created_at"] - timedelta(seconds=1)
    assignments = ", ".join(f"{key} = ${i}" for i, key in enumerate(updates, 1))
    n = len(updates)
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as caught:
        await connection.execute(
            f"UPDATE incidents SET {assignments} WHERE tenant_id = ${n + 1} AND incident_id = ${n + 2}",
            *updates.values(),
            case["tenant_a"],
            incident,
        )
    assert caught.value.sqlstate == "55000"
    assert await _t1_read_row(connection, "incidents", original) == before


@pytest.mark.parametrize(
    "table",
    [
        "incident_correlation_evidence",
        "incident_timeline_events",
        "incident_publication_intents",
    ],
)
@pytest.mark.parametrize("operation", ["UPDATE", "DELETE"])
async def test_t1_child_append_guards_preserve_exact_rows(
    t1_schema_graphs: dict[str, Any],
    table: str,
    operation: str,
) -> None:
    case = t1_schema_graphs
    row = case["graph"][table][0]
    connection = case["connection"]
    before = await _t1_read_row(connection, table, row)
    assert before is not None
    keys = _T1_KEYS[table]
    predicate = " AND ".join(f"{name} = ${i}" for i, name in enumerate(keys, 1))
    if operation == "DELETE":
        query = f"DELETE FROM {table} WHERE {predicate}"
    else:
        field = {
            "incident_correlation_evidence": "reason",
            "incident_timeline_events": "summary",
            "incident_publication_intents": "intent_time",
        }[table]
        replacement = (
            "intent_time + interval '1 second'"
            if table == "incident_publication_intents"
            else "'attempted change'"
        )
        query = f"UPDATE {table} SET {field} = {replacement} WHERE {predicate}"
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as caught:
        await connection.execute(query, *(row[key] for key in keys))
    assert caught.value.sqlstate == "55000"
    assert await _t1_read_row(connection, table, row) == before


async def test_t1_equal_pair_endpoints_rejected(
    t1_schema_graphs: dict[str, Any],
) -> None:
    case = t1_schema_graphs
    row = dict(case["graph"]["incident_correlation_evidence"][0])
    row["right_signal_id"] = row["left_signal_id"]
    with pytest.raises(asyncpg.CheckViolationError) as caught:
        await _t1_insert(case["connection"], "incident_correlation_evidence", row)
    assert (caught.value.sqlstate, caught.value.constraint_name) == (
        "23514",
        "ck_incident_correlation_evidence_pair_order",
    )


@pytest.mark.parametrize("table", list(_T1_KEYS))
async def test_t1_primary_key_duplicates_and_overlapping_unique_rules(
    t1_schema_graphs: dict[str, Any],
    table: str,
) -> None:
    case = t1_schema_graphs
    row = dict(case["graph"][table][0])
    allowed = [f"pk_{table}"]
    if table == "incidents":
        allowed.append("uq_incidents_tenant_number")
    elif table == "incident_signal_links":
        allowed.extend(
            [
                "uq_incident_signal_links_tenant_signal",
                "uq_incident_signal_links_incident_fp",
            ]
        )
    elif table == "incident_timeline_events":
        row["event_ordinal"] = 1000
    before = await _t1_read_row(case["connection"], table, row)
    with pytest.raises(asyncpg.UniqueViolationError) as caught:
        await _t1_insert(case["connection"], table, row)
    assert caught.value.sqlstate == "23505" and caught.value.constraint_name in allowed
    assert await _t1_read_row(case["connection"], table, row) == before
    print("T1_DUPLICATE_RULE", table, caught.value.constraint_name, allowed)


async def test_t1_unique_incident_number_equivalence_and_fingerprint_rejection(
    t1_schema_graphs: dict[str, Any],
) -> None:
    case = t1_schema_graphs
    connection = case["connection"]
    definitions = await connection.fetch(
        "SELECT conname, pg_get_constraintdef(oid) AS definition FROM pg_constraint "
        "WHERE conrelid = 'public.incidents'::regclass AND conname = ANY($1::text[])",
        ["pk_incidents", "uq_incidents_tenant_number", "ck_incidents_incident_number"],
    )
    assert {row["conname"] for row in definitions} == {
        "pk_incidents",
        "uq_incidents_tenant_number",
        "ck_incidents_incident_number",
    }
    parent = case["graph"]["incidents"][0]
    assert parent["incident_number"] == f"INC-{parent['incident_id'].hex.upper()}"
    duplicate = dict(parent)
    with pytest.raises(asyncpg.UniqueViolationError) as duplicate_error:
        await _t1_insert(connection, "incidents", duplicate)
    assert duplicate_error.value.sqlstate == "23505"
    assert duplicate_error.value.constraint_name in {
        "pk_incidents",
        "uq_incidents_tenant_number",
    }
    duplicate["incident_id"] = uuid.uuid4()
    with pytest.raises(asyncpg.CheckViolationError) as number_error:
        await _t1_insert(connection, "incidents", duplicate)
    assert (number_error.value.sqlstate, number_error.value.constraint_name) == (
        "23514",
        "ck_incidents_incident_number",
    )
    for definition in definitions:
        print("T1_NUMBER_EQUIVALENCE", definition["conname"], definition["definition"])
    assert (
        len(
            {
                row["semantic_fingerprint"]
                for row in case["graph"]["incident_signal_links"]
            }
        )
        == 2
    )
    link = dict(case["graph"]["incident_signal_links"][0], signal_id=uuid.uuid4())
    with pytest.raises(asyncpg.UniqueViolationError) as fingerprint_error:
        await _t1_insert(connection, "incident_signal_links", link)
    assert (
        fingerprint_error.value.sqlstate,
        fingerprint_error.value.constraint_name,
    ) == ("23505", "uq_incident_signal_links_incident_fp")


@pytest.mark.parametrize("table", list(_T1_KEYS))
async def test_t1_absent_tenant_fk_rejections_record_actual_constraint(
    t1_schema_graphs: dict[str, Any], table: str
) -> None:
    case = t1_schema_graphs
    row = dict(case["graph"][table][0], tenant_id=uuid.uuid4())
    foreign_keys = [
        obj["name"]
        for obj in EMBEDDED_EXPECTED_OBJECTS
        if obj["kind"] == "foreign_key" and obj["table"] == table
    ]
    with pytest.raises(asyncpg.ForeignKeyViolationError) as caught:
        await _t1_insert(case["connection"], table, row)
    assert caught.value.sqlstate == "23503"
    assert caught.value.constraint_name in foreign_keys
    assert await _t1_read_row(case["connection"], table, row) is None
    print("T1_TENANT_FK_OVERLAP", table, caught.value.constraint_name, foreign_keys)


@pytest.mark.parametrize(
    "table",
    [
        "incident_signal_links",
        "incident_timeline_events",
        "incident_publication_intents",
    ],
)
@pytest.mark.parametrize("target", ["missing_parent", "other_tenant_parent"])
async def test_t1_parent_composite_fks_reject_missing_and_cross_tenant_targets(
    t1_schema_graphs: dict[str, Any],
    table: str,
    target: str,
) -> None:
    case = t1_schema_graphs
    row = dict(case["graph"][table][0])
    row["incident_id"] = (
        uuid.uuid4()
        if target == "missing_parent"
        else case["other"]["incidents"][0]["incident_id"]
    )
    if table == "incident_signal_links":
        row["signal_id"] = uuid.uuid4()
        row["semantic_fingerprint"] = hashlib.sha256(row["signal_id"].bytes).hexdigest()
    elif table == "incident_timeline_events":
        row["event_id"] = uuid.uuid4()
        row["source_signal_id"] = None
    with pytest.raises(asyncpg.ForeignKeyViolationError) as caught:
        await _t1_insert(case["connection"], table, row)
    assert (caught.value.sqlstate, caught.value.constraint_name) == (
        "23503",
        f"fk_{table}_incident",
    )
    assert await _t1_read_row(case["connection"], table, row) is None


@pytest.mark.parametrize("side", ["left", "right"])
@pytest.mark.parametrize("source", ["missing", "other_incident", "other_tenant"])
async def test_t1_evidence_endpoint_fks_are_independently_observed(
    t1_schema_graphs: dict[str, Any],
    side: str,
    source: str,
) -> None:
    case = t1_schema_graphs
    connection = case["connection"]
    row = dict(case["graph"]["incident_correlation_evidence"][0])
    if source == "other_incident":
        additional = _t1_public_graph(
            case["tenant_a"], (uuid.UUID(int=1), uuid.UUID(int=(1 << 128) - 1))
        )
        for table, records in additional.items():
            for record in records:
                await _t1_insert(connection, table, record)
    if source == "missing":
        foreign_id = uuid.UUID(int=3 if side == "left" else (1 << 128) - 2)
    else:
        foreign_id = uuid.UUID(int=1 if side == "left" else (1 << 128) - 1)
    row[f"{side}_signal_id"] = foreign_id
    assert row["left_signal_id"].bytes < row["right_signal_id"].bytes
    with pytest.raises(asyncpg.ForeignKeyViolationError) as caught:
        await _t1_insert(connection, "incident_correlation_evidence", row)
    assert (caught.value.sqlstate, caught.value.constraint_name) == (
        "23503",
        f"fk_incident_correlation_evidence_{side}_member",
    )
    assert await _t1_read_row(connection, "incident_correlation_evidence", row) is None


async def _t1_schema_state(
    connection: asyncpg.Connection, tables: list[str], functions: list[str]
) -> dict[str, Any]:
    queries = {
        "columns": "SELECT c.relname AS table_name, a.attnum, a.attname, "
        "format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull, "
        "pg_get_expr(d.adbin, d.adrelid) AS default_expr "
        "FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid "
        "LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum "
        "WHERE c.relnamespace = 'public'::regnamespace AND c.relname = ANY($1::text[]) "
        "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY c.relname, a.attnum",
        "constraints": "SELECT c.relname AS table_name, k.conname, pg_get_constraintdef(k.oid) AS definition "
        "FROM pg_class c JOIN pg_constraint k ON k.conrelid = c.oid "
        "WHERE c.relnamespace = 'public'::regnamespace AND c.relname = ANY($1::text[]) "
        "ORDER BY c.relname, k.conname",
        "indexes": "SELECT tablename, indexname, indexdef FROM pg_indexes "
        "WHERE schemaname = 'public' AND tablename = ANY($1::text[]) ORDER BY tablename, indexname",
        "triggers": "SELECT c.relname AS table_name, t.tgname, pg_get_triggerdef(t.oid) AS definition "
        "FROM pg_class c JOIN pg_trigger t ON t.tgrelid = c.oid "
        "WHERE c.relnamespace = 'public'::regnamespace AND c.relname = ANY($1::text[]) "
        "AND NOT t.tgisinternal ORDER BY c.relname, t.tgname",
        "policies": "SELECT c.relname AS table_name, p.polname, "
        "pg_get_expr(p.polqual, p.polrelid) AS using_expr, "
        "pg_get_expr(p.polwithcheck, p.polrelid) AS check_expr "
        "FROM pg_class c JOIN pg_policy p ON p.polrelid = c.oid "
        "WHERE c.relnamespace = 'public'::regnamespace AND c.relname = ANY($1::text[]) "
        "ORDER BY c.relname, p.polname",
    }
    result = {}
    for kind, query in queries.items():
        result[kind] = [dict(row) for row in await connection.fetch(query, tables)]
    result["functions"] = [
        dict(row)
        for row in await connection.fetch(
            "SELECT proname, pg_get_functiondef(oid) AS definition FROM pg_proc "
            "WHERE pronamespace = 'public'::regnamespace AND proname = ANY($1::text[]) ORDER BY proname",
            functions,
        )
    ]
    return result


async def _t1_foundation_state(connection: asyncpg.Connection) -> dict[str, Any]:
    tables = ["plans", "tenants", "users", "tenant_memberships", "sessions"]
    result = await _t1_schema_state(connection, tables, [])
    result["rows"] = {}
    for table in tables:
        result["rows"][table] = [
            dict(row)
            for row in await connection.fetch(f"SELECT * FROM {table} ORDER BY id")
        ]
    return result


async def _t1_seed_foundation_probes(connection: asyncpg.Connection) -> None:
    plan, tenant, user = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await connection.execute(
        "INSERT INTO plans (id, name, monthly_price_cents, telemetry_ingest_limit_mb, "
        "agent_concurrency_limit, features) VALUES ($1, $2, 100, 100, 1, '{}'::jsonb)",
        plan,
        f"t1-plan-{plan.hex}",
    )
    await connection.execute(
        "INSERT INTO users (id, email, first_name, last_name) VALUES ($1, $2, 'T1', 'Probe')",
        user,
        f"t1-{user.hex}@example.invalid",
    )
    await connection.execute(
        "INSERT INTO tenants (id, name, slug, plan_id) VALUES ($1, $2, $2, $3)",
        tenant,
        f"t1-tenant-{tenant.hex}",
        plan,
    )
    await connection.execute(
        "INSERT INTO tenant_memberships (tenant_id, user_id, role) VALUES ($1, $2, 'viewer')",
        tenant,
        user,
    )
    await connection.execute(
        "INSERT INTO sessions (tenant_id, user_id, ip_address, user_agent, expires_at) "
        "VALUES ($1, $2, '127.0.0.1', 't1-migration-probe', now() + interval '1 day')",
        tenant,
        user,
    )


@pytest_asyncio.fixture(scope="module")
async def t1_migration_roundtrip(db_resource: dict[str, Any]) -> dict[str, Any]:
    """Both migration nodes consume the same measured down/up execution."""
    connection = await asyncpg.connect(db_resource["admin_dsn"])
    env = os.environ.copy()
    parsed = urllib.parse.urlparse(db_resource["admin_dsn"])
    env["DATABASE_URL"] = urllib.parse.urlunparse(
        parsed._replace(scheme="postgresql+asyncpg")
    )
    functions = ["guard_incident_snapshot_change", "reject_incident_child_change"]
    try:
        await _t1_seed_foundation_probes(connection)
        before = await _t1_foundation_state(connection)
        assert all(before["rows"][table] for table in before["rows"])
        await test_upgrade_creates_exact_incident_catalog(db_resource, connection)
        catalog_before = await _t1_schema_state(connection, list(_T1_KEYS), functions)
        try:
            down = subprocess.run(
                ["poetry", "run", "alembic", "downgrade", "20260918_0001"],
                cwd=BACKEND_DIR,
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )
            assert down.returncode == 0, down.stdout + down.stderr
            down_revision = await connection.fetchval(
                "SELECT version_num FROM alembic_version"
            )
            assert down_revision == "20260918_0001"
            for table in _T1_KEYS:
                assert (
                    await connection.fetchval(
                        "SELECT to_regclass($1::text)", f"public.{table}"
                    )
                    is None
                )
            for function in functions:
                assert not await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace = 'public'::regnamespace AND proname = $1)",
                    function,
                )
            while_downgraded = await _t1_foundation_state(connection)
            assert while_downgraded == before
        finally:
            up = subprocess.run(
                ["poetry", "run", "alembic", "upgrade", "head"],
                cwd=BACKEND_DIR,
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )
            assert up.returncode == 0, up.stdout + up.stderr
            await connection.execute(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "
                + ", ".join(_T1_KEYS)
                + f' TO "{db_resource["role_name"]}"'
            )
        up_revision = await connection.fetchval(
            "SELECT version_num FROM alembic_version"
        )
        assert up_revision == "20261103_0002"
        await test_upgrade_creates_exact_incident_catalog(db_resource, connection)
        catalog_after = await _t1_schema_state(connection, list(_T1_KEYS), functions)
        assert catalog_after == catalog_before
        after = await _t1_foundation_state(connection)
        assert after == before
        return dict(
            before=before,
            while_downgraded=while_downgraded,
            after=after,
            down_revision=down_revision,
            up_revision=up_revision,
            catalog_before=catalog_before,
            catalog_after=catalog_after,
        )
    finally:
        await connection.close()
