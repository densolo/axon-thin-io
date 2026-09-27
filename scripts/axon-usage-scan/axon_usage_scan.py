#!/usr/bin/env python3
"""
Scan a (multi-module) Spring / Kotlin / Java project for everything Axon Framework and report
which features are in use — and which of them axon-thin supports today.

Pure stdlib, heuristic (regex) based: good enough for an inventory, not a compiler.

    python3 axon_usage_scan.py /path/to/project [/other/project ...] [-o out-dir] [--examples 5]

Writes <out>/axon-usage.json (full detail) and <out>/axon-usage.md (summary) and prints the summary.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------------------------------------------------------------
# Feature catalog: Axon simple name -> (area, thin status, note).
# Status: SUPPORTED | PARTIAL | MISSING | N/A (irrelevant without event store / Axon Server).
# Keep in sync with axon-thin as features land.
# --------------------------------------------------------------------------------------------------------------------
S, P, M, NA = "SUPPORTED", "PARTIAL", "MISSING", "N/A"

CATALOG: dict[str, tuple[str, str, str]] = {
    # command side
    "CommandHandler": ("commands", S, "on Spring beans; on aggregates/constructors see aggregate.*"),
    "CommandGateway": ("commands", S, "send / sendAndWait / callbacks / dispatch interceptors"),
    "CommandBus": ("commands", M, "thin exposes only the gateway"),
    "CommandMessage": ("messages", S, "handler parameter / gateway argument"),
    "GenericCommandMessage": ("messages", S, ""),
    "CommandCallback": ("commands", S, ""),
    "CommandResultMessage": ("commands", S, ""),
    "CommandExecutionException": ("commands", S, "wraps checked exceptions, like DefaultCommandGateway"),
    "NoHandlerForCommandException": ("commands", S, ""),
    "TargetAggregateIdentifier": ("aggregates", P, "only used for routing; harmless but lives in axon-modelling"),
    "RoutingKey": ("distribution", NA, "local bus only"),
    "CommandGatewayFactory": ("commands", M, "custom gateway interfaces"),
    "Timeout": ("commands", M, ""),
    "IntervalRetryScheduler": ("commands", M, "retry scheduler"),
    "RetryScheduler": ("commands", M, "retry scheduler"),
    # event side
    "EventHandler": ("events", S, "subscribing semantics, same transaction as the command"),
    "EventGateway": ("events", S, ""),
    "EventBus": ("events", M, "use EventGateway"),
    "EventMessage": ("messages", S, "handler parameter"),
    "GenericEventMessage": ("messages", S, ""),
    "DomainEventMessage": ("messages", M, "event-sourcing only"),
    "ProcessingGroup": ("events", P, "ignored: all handlers behave as one subscribing group (needs axon-configuration types)"),
    "EventProcessingConfigurer": ("config", P, "subscribing mode is the only mode"),
    "SubscribingEventProcessor": ("events", S, "implicit"),
    "TrackingEventProcessor": ("events", M, "async/tracking processors"),
    "PooledStreamingEventProcessor": ("events", M, "async/streaming processors"),
    "TrackingEventProcessorConfiguration": ("events", M, ""),
    "ResetHandler": ("replay", NA, "no replay without event store"),
    "DisallowReplay": ("replay", NA, ""),
    "AllowReplay": ("replay", NA, ""),
    "ReplayStatus": ("replay", NA, ""),
    "ListenerInvocationErrorHandler": ("events", P, "axon.thin.event-handler-error-mode=log|propagate"),
    "PropagatingErrorHandler": ("events", S, "event-handler-error-mode=propagate"),
    "LoggingErrorHandler": ("events", S, "event-handler-error-mode=log (default)"),
    "ErrorHandler": ("events", M, "processor-level error handler"),
    "SequencingPolicy": ("events", NA, ""),
    "SequentialPerAggregatePolicy": ("events", NA, ""),
    # message-level parameters
    "MetaDataValue": ("parameters", S, ""),
    "MessageIdentifier": ("parameters", S, ""),
    "Timestamp": ("parameters", S, "events only"),
    "MetaData": ("messages", S, "parameter + gateway overloads"),
    "SequenceNumber": ("parameters", M, "event-sourcing only"),
    "SourceId": ("parameters", M, "event-sourcing only"),
    "AggregateType": ("parameters", M, ""),
    "ConcludesBatch": ("parameters", M, ""),
    "Message": ("messages", S, ""),
    "MessageDispatchInterceptor": ("interceptors", S, "registerDispatchInterceptor on both gateways"),
    "MessageHandlerInterceptor": ("interceptors", M, ""),
    "CommandHandlerInterceptor": ("interceptors", M, "@CommandHandlerInterceptor in aggregates"),
    "ExceptionHandler": ("interceptors", M, "@ExceptionHandler"),
    "InterceptorChain": ("interceptors", M, ""),
    "CorrelationDataProvider": ("messages", P, "fixed MessageOriginProvider (correlationId, traceId)"),
    "MessageOriginProvider": ("messages", S, "built in"),
    "SimpleCorrelationDataProvider": ("messages", M, ""),
    "MultiCorrelationDataProvider": ("messages", M, ""),
    "UnitOfWork": ("unit-of-work", M, "thin has an internal unit of work only"),
    "CurrentUnitOfWork": ("unit-of-work", M, ""),
    "DefaultUnitOfWork": ("unit-of-work", M, ""),
    "TransactionManager": ("transactions", P, "Spring PlatformTransactionManager is used directly"),
    "SpringTransactionManager": ("transactions", P, ""),
    # aggregates / event sourcing
    "Aggregate": ("aggregates", M, "@Aggregate"),
    "AggregateRoot": ("aggregates", M, ""),
    "AggregateIdentifier": ("aggregates", M, ""),
    "AggregateVersion": ("aggregates", M, ""),
    "AggregateMember": ("aggregates", M, ""),
    "EntityId": ("aggregates", M, ""),
    "AggregateLifecycle": ("aggregates", M, "apply(), markDeleted(), createNew()"),
    "EventSourcingHandler": ("aggregates", M, ""),
    "CreationPolicy": ("aggregates", M, ""),
    "AggregateCreationPolicy": ("aggregates", M, ""),
    "Repository": ("aggregates", M, "org.axonframework.modelling.command.Repository"),
    "GenericJpaRepository": ("aggregates", M, "state-stored JPA aggregates"),
    "EventSourcingRepository": ("aggregates", M, ""),
    "AggregateNotFoundException": ("aggregates", M, ""),
    "ConflictResolver": ("aggregates", M, ""),
    "Snapshotter": ("event-store", NA, ""),
    "SnapshotTriggerDefinition": ("event-store", NA, ""),
    "EventStore": ("event-store", NA, "thin keeps no event store"),
    "EventStorageEngine": ("event-store", NA, ""),
    "JpaEventStorageEngine": ("event-store", NA, ""),
    "JdbcEventStorageEngine": ("event-store", NA, ""),
    "EmbeddedEventStore": ("event-store", NA, ""),
    "TokenStore": ("event-store", NA, ""),
    "JpaTokenStore": ("event-store", NA, ""),
    "Upcaster": ("serialization", NA, ""),
    "EventUpcaster": ("serialization", NA, ""),
    "SingleEventUpcaster": ("serialization", NA, ""),
    "Revision": ("serialization", NA, "only matters for stored/serialized events"),
    "Serializer": ("serialization", NA, "thin never serializes"),
    "JacksonSerializer": ("serialization", NA, ""),
    "XStreamSerializer": ("serialization", NA, ""),
    # sagas & deadlines
    "Saga": ("sagas", M, ""),
    "SagaEventHandler": ("sagas", M, ""),
    "StartSaga": ("sagas", M, ""),
    "EndSaga": ("sagas", M, ""),
    "SagaLifecycle": ("sagas", M, ""),
    "SagaStore": ("sagas", M, ""),
    "DeadlineManager": ("deadlines", M, ""),
    "DeadlineHandler": ("deadlines", M, ""),
    "EventScheduler": ("deadlines", M, ""),
    # queries (planned: replaced by direct service/repository calls)
    "QueryHandler": ("queries", M, "replace with direct service/repository calls"),
    "QueryGateway": ("queries", M, "replace with direct service/repository calls"),
    "QueryBus": ("queries", M, ""),
    "QueryUpdateEmitter": ("queries", M, "subscription queries"),
    "ResponseTypes": ("queries", M, ""),
    "SubscriptionQueryResult": ("queries", M, ""),
    # configuration / infra
    "Configurer": ("config", M, "Axon Configuration API"),
    "Configuration": ("config", M, "org.axonframework.config.Configuration"),
    "ConfigurerModule": ("config", M, ""),
    "EventProcessorInfoConfiguration": ("config", M, ""),
    "AxonServerConfiguration": ("axon-server", NA, ""),
    "SpanFactory": ("observability", M, ""),
    "MessageMonitor": ("observability", M, ""),
    "HandlerDefinition": ("extension", M, "custom handler definitions"),
    "HandlerEnhancerDefinition": ("extension", M, ""),
    "ParameterResolverFactory": ("extension", M, "custom parameter resolvers"),
    "Registration": ("commands", S, "interceptor registration handle"),
}

# Annotations whose methods we treat as handlers (signature details are captured).
HANDLER_ANNOTATIONS = {
    "CommandHandler", "EventHandler", "EventSourcingHandler", "QueryHandler", "SagaEventHandler",
    "DeadlineHandler", "ExceptionHandler", "MessageHandlerInterceptor", "CommandHandlerInterceptor", "ResetHandler",
}
CLASS_LEVEL_MARKERS = {"Aggregate", "Saga", "ProcessingGroup", "AggregateRoot"}

SOURCE_EXT = {".kt", ".java", ".kts"}
BUILD_FILES = {"pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts",
               "gradle.properties", "libs.versions.toml"}
CONFIG_RE = re.compile(r"^(application|bootstrap)[\w-]*\.(ya?ml|properties)$")
DEFAULT_EXCLUDES = {".git", ".idea", ".gradle", "build", "target", "node_modules", "out", ".kotlin", "generated"}

# --------------------------------------------------------------------------------------------------------------------


@dataclass
class Occurrence:
    file: str
    line: int
    module: str
    snippet: str


@dataclass
class Report:
    roots: list[str]
    files_scanned: int = 0
    files_with_axon: int = 0
    modules: Counter = field(default_factory=Counter)
    names: dict[str, list[Occurrence]] = field(default_factory=lambda: defaultdict(list))
    unclassified: dict[str, list[Occurrence]] = field(default_factory=lambda: defaultdict(list))
    annotations: dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))  # name -> target kind
    member_calls: dict[str, list[Occurrence]] = field(default_factory=lambda: defaultdict(list))
    supertypes: dict[str, list[Occurrence]] = field(default_factory=lambda: defaultdict(list))
    bean_overrides: dict[str, list[Occurrence]] = field(default_factory=lambda: defaultdict(list))
    handlers: list[dict] = field(default_factory=list)
    config_keys: dict[str, list[Occurrence]] = field(default_factory=lambda: defaultdict(list))
    dependencies: dict[str, set] = field(default_factory=lambda: defaultdict(set))
    packages: Counter = field(default_factory=Counter)


# --------------------------------------------------------------------------------------------------------------------
# source helpers
# --------------------------------------------------------------------------------------------------------------------

_COMMENT_RE = re.compile(r"//[^\n]*|/\*.*?\*/", re.S)


def strip_comments(text: str) -> str:
    """Blank out comments but keep offsets/line numbers intact."""
    return _COMMENT_RE.sub(lambda m: re.sub(r"[^\n]", " ", m.group(0)), text)


def line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def snippet_at(lines: list[str], line: int) -> str:
    return lines[line - 1].strip()[:160] if 0 < line <= len(lines) else ""


IMPORT_RE = re.compile(r"^\s*import\s+(static\s+)?(org\.axonframework\.[\w.]+(?:\.\*)?)(?:\s+as\s+(\w+))?\s*;?",
                       re.M)
INLINE_FQN_RE = re.compile(r"\borg\.axonframework\.(?:[a-z]\w*\.)+([A-Z]\w*)")
ANNOTATION_RE = re.compile(r"@([A-Z]\w*)")


def matching_paren(text: str, open_idx: int) -> int:
    depth = 0
    for i in range(open_idx, len(text)):
        c = text[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
    return len(text) - 1


def skip_annotation_args(text: str, idx: int) -> int:
    """idx points right after '@Name'; skip optional '(...)'."""
    j = idx
    while j < len(text) and text[j] in " \t":
        j += 1
    if j < len(text) and text[j] == "(":
        return matching_paren(text, j) + 1
    return idx


MODIFIERS = {"public", "private", "protected", "internal", "open", "final", "abstract", "override", "static",
             "suspend", "inline", "operator", "synchronized", "default", "lateinit", "const", "data", "sealed",
             "inner", "enum", "annotation", "value", "transient", "volatile", "native", "strictfp", "external"}


def classify_target(text: str, pos: int) -> tuple[str, int]:
    """Classify what the annotation ending at `pos` decorates. Returns (kind, index of declaration)."""
    i = pos
    while True:
        m = re.compile(r"\s*").match(text, i)
        i = m.end()
        if text.startswith("@", i):  # another annotation
            m2 = re.compile(r"@[\w.:]+").match(text, i)
            if not m2:
                break
            i = skip_annotation_args(text, m2.end())
            continue
        m3 = re.compile(r"([A-Za-z_]\w*)").match(text, i)
        if not m3:
            break
        word = m3.group(1)
        if word in MODIFIERS:
            i = m3.end()
            continue
        if word in ("class", "interface", "object", "record", "enum"):
            return "class", i
        if word == "fun":
            return "method", i
        if word == "constructor":
            return "constructor", i
        if word in ("val", "var"):
            # Kotlin: property, or constructor-parameter property; decide by preceding '(' / ','
            return ("parameter" if _inside_parens(text, pos) else "property"), i
        if word == "get" or word == "set" or word in ("field", "param", "property"):  # use-site targets
            return "property", i
        # Kotlin param `name: Type`, Java: `Type name` followed by , ) ; = or `Type name(` (method)
        rest = text[i:i + 300]
        if re.match(r"[A-Za-z_]\w*\s*:", rest):
            return "parameter", i
        jm = re.match(r"[\w.<>\[\], ?]+?\s+([A-Za-z_]\w*)\s*([,);=(])", rest)
        if jm:
            return {"(": "method"}.get(jm.group(2), "parameter" if _inside_parens(text, pos) else "field"), i
        if re.match(r"[A-Z]\w*\s*\(", rest):  # Java constructor
            return "constructor", i
        return "other", i
    return "other", i


def _inside_parens(text: str, pos: int) -> bool:
    depth = 0
    for c in reversed(text[max(0, pos - 2000):pos]):
        if c == ")":
            depth += 1
        elif c == "(":
            if depth == 0:
                return True
            depth -= 1
        elif c in "{};" and depth == 0:
            return False
    return False


def split_params(params: str) -> list[str]:
    out, depth, cur = [], 0, []
    for c in params:
        if c in "(<[":
            depth += 1
        elif c in ")>]":
            depth -= 1
        if c == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
    if "".join(cur).strip():
        out.append("".join(cur).strip())
    return out


def parse_param(p: str) -> dict:
    anns = ANNOTATION_RE.findall(p)
    bare = re.sub(r"@[\w.]+(\s*\([^)]*\))?", "", p).strip()
    bare = re.sub(r"\b(final|val|var|vararg)\b", "", bare).strip()
    if ":" in bare:  # kotlin name: Type = default
        name, typ = bare.split(":", 1)
        typ = typ.split("=")[0].strip()
    else:  # java Type name
        parts = bare.rsplit(None, 1)
        typ, name = (parts[0], parts[1]) if len(parts) == 2 else (bare, "")
    return {"name": name.strip(), "type": typ.strip(), "annotations": anns}


def enclosing_class(text: str, pos: int) -> tuple[str, list[str]]:
    """Nearest preceding class declaration and its annotations (heuristic)."""
    best = None
    for m in re.finditer(r"\b(class|object|interface|record)\s+([A-Z]\w*)", text[:pos]):
        best = m
    if not best:
        return "", []
    head = text[max(0, best.start() - 600):best.start()]
    # annotations directly above the declaration (stop at previous closing brace/semicolon)
    head = re.split(r"[;{}]", head)[-1]
    return best.group(2), ANNOTATION_RE.findall(head)


def handler_signature(text: str, decl_idx: int, kind: str) -> dict | None:
    m = re.compile(r"(?:fun\s+(?:<[^>]*>\s*)?([\w.]+)|constructor|([A-Za-z_][\w<>\[\], ?.]*?)\s+([A-Za-z_]\w*))\s*\(")\
        .search(text, decl_idx, decl_idx + 400)
    if not m:
        return None
    open_idx = m.end() - 1
    close_idx = matching_paren(text, open_idx)
    params = [parse_param(p) for p in split_params(text[open_idx + 1:close_idx])]
    after = text[close_idx + 1:close_idx + 120]
    ret = ""
    rm = re.match(r"\s*:\s*([\w.<>?, \[\]]+?)\s*(?:[{=]|$|\n)", after)
    if kind == "method" and m.group(1):
        name = m.group(1)
        ret = rm.group(1).strip() if rm else "Unit"
    elif m.group(3):
        name, ret = m.group(3), m.group(2).split()[-1] if m.group(2) else ""
    else:
        name = "<constructor>"
    return {"name": name, "params": params, "returns": ret}


# Kotlin scope functions / Object members: never Axon API
GENERIC_MEMBERS = {"also", "apply", "let", "run", "takeIf", "takeUnless", "toString", "equals", "hashCode",
                   "javaClass", "getClass"}
VAR_KT_RE = r"\b(\w+)\s*:\s*(?:\w+\.)*{t}\b"
VAR_JAVA_RE = r"\b(?:\w+\.)*{t}(?:<[^>]*>)?\s+(\w+)\s*[;,)=]"


# --------------------------------------------------------------------------------------------------------------------
# scanning
# --------------------------------------------------------------------------------------------------------------------

def find_module(path: Path, root: Path, cache: dict) -> str:
    d = path.parent
    while True:
        if d in cache:
            return cache[d]
        if any((d / b).exists() for b in ("pom.xml", "build.gradle", "build.gradle.kts")):
            name = str(d.relative_to(root)) if d != root else root.name
            cache[path.parent] = name
            return name
        if d == root or d.parent == d:
            cache[path.parent] = root.name
            return root.name
        d = d.parent


def scan_source(path: Path, rel: str, module: str, rep: Report) -> None:
    raw = path.read_text(encoding="utf-8", errors="replace")
    if "org.axonframework" not in raw:
        return
    rep.files_with_axon += 1
    rep.modules[module] += 1
    text = strip_comments(raw)
    lines = raw.splitlines()

    def occ(offset: int) -> Occurrence:
        ln = line_of(text, offset)
        return Occurrence(rel, ln, module, snippet_at(lines, ln))

    # imports -> simple name map
    simple_to_fqn: dict[str, str] = {}
    function_imports: dict[str, str] = {}
    wildcard_pkgs: list[str] = []
    for m in IMPORT_RE.finditer(text):
        fqn, alias = m.group(2), m.group(3)
        rep.packages[fqn.rsplit(".", 1)[0] if not fqn.endswith(".*") else fqn[:-2]] += 1
        if fqn.endswith(".*"):
            wildcard_pkgs.append(fqn[:-2])
            continue
        last = fqn.rsplit(".", 1)[1]
        local = alias or last
        if last[0].islower() or m.group(1):  # function / static member import (e.g. AggregateLifecycle.apply)
            owner = fqn.rsplit(".", 2)[-2]
            # top-level Kotlin extension functions (axon-kotlin) have a package, not a class, as owner
            function_imports[local] = f"{owner}.{last}" if owner[0].isupper() else f"{fqn.rsplit('.', 1)[0]}.{last}"
        else:
            simple_to_fqn[local] = fqn
    body_start = max((m.end() for m in IMPORT_RE.finditer(text)), default=0)
    body = text

    def is_axon(name: str) -> bool:
        return name in simple_to_fqn or (bool(wildcard_pkgs) and name in CATALOG)

    def canonical(name: str) -> str:
        fqn = simple_to_fqn.get(name)
        return fqn.rsplit(".", 1)[1] if fqn else name

    # inline fully-qualified usages
    for m in INLINE_FQN_RE.finditer(body, body_start):
        rep.names[m.group(1)].append(occ(m.start()))

    # annotations
    for m in ANNOTATION_RE.finditer(body, body_start):
        name = m.group(1)
        if not is_axon(name):
            continue
        cname = canonical(name)
        end = skip_annotation_args(body, m.end())
        kind, decl = classify_target(body, end)
        rep.annotations[cname][kind] += 1
        rep.names[cname].append(occ(m.start()))
        if cname in HANDLER_ANNOTATIONS and kind in ("method", "constructor"):
            sig = handler_signature(body, decl, kind)
            for param in (sig or {}).get("params", []):  # keep only Axon parameter annotations
                param["annotations"] = [canonical(a) for a in param["annotations"] if is_axon(a)]
            cls, cls_anns = enclosing_class(body, m.start())
            ln = line_of(body, m.start())
            rep.handlers.append({
                "annotation": cname,
                "kind": kind,
                "file": rel,
                "line": ln,
                "module": module,
                "class": cls,
                "class_annotations": [canonical(a) for a in cls_anns],
                "in_aggregate": any(canonical(a) in ("Aggregate", "AggregateRoot") for a in cls_anns),
                "in_saga": any(canonical(a) == "Saga" for a in cls_anns),
                **(sig or {"name": "?", "params": [], "returns": ""}),
            })

    # type references (non-annotation), member calls on variables of Axon types, static calls
    for local, fqn in simple_to_fqn.items():
        cname = fqn.rsplit(".", 1)[1]
        for m in re.finditer(rf"(?<![@\w.]){re.escape(local)}\b", body[body_start:]):
            rep.names[cname].append(occ(body_start + m.start()))
        variables = set(re.findall(VAR_KT_RE.format(t=re.escape(local)), body))
        variables |= set(re.findall(VAR_JAVA_RE.format(t=re.escape(local)), body))
        variables -= {"fun", "class", "val", "var", "return"}
        for v in variables:
            for m in re.finditer(rf"\b{re.escape(v)}\s*\??\.\s*(\w+)\s*[(<{{]", body):
                if m.group(1) in GENERIC_MEMBERS:
                    continue
                rep.member_calls[f"{cname}.{m.group(1)}"].append(occ(m.start()))
        for m in re.finditer(rf"(?<![\w.]){re.escape(local)}\s*\.\s*(\w+)\s*[(<]", body[body_start:]):
            rep.member_calls[f"{cname}.{m.group(1)}"].append(occ(body_start + m.start()))
    for local, qualified in function_imports.items():
        for m in re.finditer(rf"(?<![\w.]){re.escape(local)}\s*[(<]", body[body_start:]):
            rep.member_calls[qualified].append(occ(body_start + m.start()))
            if qualified[0].isupper():
                rep.names[qualified.split(".")[0]].append(occ(body_start + m.start()))

    # supertypes (implements / extends / Kotlin ':')
    for m in re.finditer(r"\b(?:class|object|interface)\s+\w+", body):
        decl_end = body.find("{", m.end())
        decl_end = len(body) if decl_end < 0 else decl_end
        decl = body[m.end():decl_end]
        paren = re.match(r"\s*(?:<[^>]*>)?\s*(?:@\w+\s*)*(?:\w+\s+)*(?:constructor\s*)?\(", decl)
        if paren:  # drop Kotlin primary constructor: `class X(val a: A) : B`
            decl = decl[matching_paren(decl, paren.end() - 1) + 1:]
        decl = re.sub(r"\bwhere\b.*", "", decl, flags=re.S)
        while re.search(r"<[^<>]*>", decl):  # drop generic arguments
            decl = re.sub(r"<[^<>]*>", "", decl)
        sm = re.search(r"^\s*:(.*)|\b(?:extends|implements)\b(.*)", decl, re.S)
        if sm:
            for t in re.findall(r"\b([A-Z]\w*)", sm.group(1) or sm.group(2)):
                if is_axon(t):
                    rep.supertypes[canonical(t)].append(occ(m.start()))

    # Spring @Bean factories returning Axon types (= engine customization)
    for m in re.finditer(r"@Bean\b", body):
        window = body[m.end():m.end() + 500]
        km = re.search(r"fun\s+\w+\s*\([^)]*\)\s*:\s*([A-Z]\w*)", window, re.S)
        jm = re.search(r"(?:public|protected|private)?\s*([A-Z]\w*)(?:<[^>]*>)?\s+\w+\s*\(", window)
        t = (km or jm).group(1) if (km or jm) else None
        if t and is_axon(t):
            rep.bean_overrides[canonical(t)].append(occ(m.start()))

    # names imported but never matched above still count as "used" (e.g. only in generics)
    for local, fqn in simple_to_fqn.items():
        cname = fqn.rsplit(".", 1)[1]
        if cname not in rep.names:
            rep.names[cname].append(occ(0))


def flatten_yaml_keys(text: str) -> list[tuple[str, int]]:
    """Minimal YAML key flattener (maps only; lists/values ignored). Returns (dotted.key, line)."""
    stack: list[tuple[int, str]] = []
    out = []
    for ln, line in enumerate(text.splitlines(), 1):
        if not line.strip() or line.lstrip().startswith(("#", "-", "---")):
            continue
        m = re.match(r"^(\s*)([\w.\-\[\]\"']+)\s*:(.*)$", line)
        if not m:
            continue
        indent, key, value = len(m.group(1)), m.group(2).strip("\"'"), m.group(3).strip()
        while stack and stack[-1][0] >= indent:
            stack.pop()
        stack.append((indent, key))
        if value and not value.startswith("#"):
            out.append((".".join(k for _, k in stack), ln))
    return out


def scan_config(path: Path, rel: str, module: str, rep: Report) -> None:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    if path.suffix == ".properties":
        keys = [(l.split("=", 1)[0].split(":", 1)[0].strip(), i) for i, l in enumerate(lines, 1)
                if l.strip() and not l.lstrip().startswith(("#", "!"))]
    else:
        keys = flatten_yaml_keys(text)
    for key, ln in keys:
        if key.startswith("axon."):
            rep.config_keys[key].append(Occurrence(rel, ln, module, snippet_at(lines, ln)))


DEP_POM_RE = re.compile(r"<groupId>\s*(org\.axonframework[\w.]*)\s*</groupId>\s*<artifactId>\s*([\w.\-]+)\s*"
                        r"</artifactId>(?:\s*<version>\s*([^<]+?)\s*</version>)?")
DEP_GRADLE_RE = re.compile(r"[\"'](org\.axonframework[\w.]*):([\w.\-]+)(?::([^\"'@]+))?[\"']")
DEP_TOML_RE = re.compile(r"(?:module\s*=\s*\"|group\s*=\s*\")(org\.axonframework[\w.]*)(?::|\"\s*,\s*name\s*=\s*\")"
                         r"([\w.\-]+)\"")


def scan_build(path: Path, rel: str, rep: Report) -> None:
    text = path.read_text(encoding="utf-8", errors="replace")
    for rx in (DEP_POM_RE, DEP_GRADLE_RE, DEP_TOML_RE):
        for m in rx.finditer(text):
            version = m.group(3) if m.lastindex and m.lastindex >= 3 else None
            rep.dependencies[f"{m.group(1)}:{m.group(2)}"].add(f"{rel}{' @ ' + version if version else ''}")
    for m in re.finditer(r"axon[\w.\-]*version[\"'>\s=:]+([\d][\w.\-]*)", text, re.I):
        rep.dependencies["(version property)"].add(f"{rel}: {m.group(0).strip()}")


def scan(roots: list[Path], excludes: set[str]) -> Report:
    rep = Report(roots=[str(r) for r in roots])
    for root in roots:
        cache: dict = {}
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in excludes and not d.startswith(".")]
            for fn in filenames:
                p = Path(dirpath) / fn
                rel = str(p.relative_to(root)) if len(roots) == 1 else f"{root.name}/{p.relative_to(root)}"
                if fn in BUILD_FILES:
                    scan_build(p, rel, rep)
                elif CONFIG_RE.match(fn):
                    scan_config(p, rel, find_module(p, root, cache), rep)
                elif p.suffix in SOURCE_EXT:
                    rep.files_scanned += 1
                    scan_source(p, rel, find_module(p, root, cache), rep)
    for name in list(rep.names):
        if name not in CATALOG:
            rep.unclassified[name] = rep.names[name]
    return rep


# --------------------------------------------------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------------------------------------------------

STATUS_ORDER = {M: 0, P: 1, NA: 2, S: 3}


def handler_findings(rep: Report) -> list[tuple[str, str, int, list]]:
    """Handler shapes that need a thin feature, beyond the annotation itself."""
    findings: dict[str, list] = defaultdict(list)
    for h in rep.handlers:
        where = Occurrence(h["file"], h["line"], h["module"], f'{h["class"]}.{h["name"]}')
        if h["in_aggregate"]:
            findings[f'@{h["annotation"]} inside @Aggregate'].append(where)
        if h["in_saga"]:
            findings[f'@{h["annotation"]} inside @Saga'].append(where)
        if h["kind"] == "constructor":
            findings[f'@{h["annotation"]} on constructor'].append(where)
        for i, p in enumerate(h["params"]):
            if i == 0 and not p["annotations"]:
                continue
            label = ", ".join("@" + a for a in p["annotations"]) or p["type"]
            findings[f"parameter: {label}"].append(where)
    return sorted(((k, S if _param_supported(k) else M, len(v), v) for k, v in findings.items()),
                  key=lambda f: (-f[2], f[0]))


def _param_supported(label: str) -> bool:
    if not label.startswith("parameter: "):
        return False
    p = label[len("parameter: "):]
    if p.startswith("@"):
        return all(a.strip().lstrip("@") in ("MetaDataValue", "MessageIdentifier", "Timestamp")
                   for a in p.split(","))
    base = re.sub(r"<.*", "", p).rstrip("?")
    if base in ("UnitOfWork", "InterceptorChain", "ReplayStatus", "ScopeDescriptor", "DeadlineMessage"):
        return False
    return True  # Message types, MetaData or Spring beans


def to_json(rep: Report, examples: int) -> dict:
    def occs(v: list[Occurrence]) -> dict:
        return {"count": len(v), "examples": [o.__dict__ for o in v[:examples]]}

    return {
        "roots": rep.roots,
        "files_scanned": rep.files_scanned,
        "files_with_axon": rep.files_with_axon,
        "modules": dict(rep.modules.most_common()),
        "dependencies": {k: sorted(v) for k, v in sorted(rep.dependencies.items())},
        "features": {
            name: {
                "area": CATALOG[name][0], "thin": CATALOG[name][1], "note": CATALOG[name][2],
                "annotation_targets": dict(rep.annotations.get(name, {})),
                "modules": dict(Counter(o.module for o in v)),
                **occs(v),
            }
            for name, v in sorted(rep.names.items()) if name in CATALOG
        },
        "unclassified": {k: occs(v) for k, v in sorted(rep.unclassified.items())},
        "member_calls": {k: occs(v) for k, v in sorted(rep.member_calls.items(), key=lambda kv: -len(kv[1]))},
        "supertypes": {k: occs(v) for k, v in rep.supertypes.items()},
        "bean_overrides": {k: occs(v) for k, v in rep.bean_overrides.items()},
        "config_keys": {k: occs(v) for k, v in sorted(rep.config_keys.items())},
        "handler_findings": [
            {"shape": k, "thin": s, "count": c, "examples": [o.__dict__ for o in v[:examples]]}
            for k, s, c, v in handler_findings(rep)
        ],
        "handlers": rep.handlers,
    }


def to_markdown(rep: Report, examples: int) -> str:
    out: list[str] = []
    w = out.append
    w("# Axon usage inventory\n")
    w(f"Roots: {', '.join(rep.roots)}  ")
    w(f"Source files scanned: **{rep.files_scanned}**, referencing Axon: **{rep.files_with_axon}**, "
      f"handlers found: **{len(rep.handlers)}**\n")

    def ex(v: list[Occurrence]) -> str:
        return "<br>".join(f"`{o.file}:{o.line}`" for o in v[:examples])

    # gaps first: that is what drives the thin roadmap
    rows = []
    for name, v in rep.names.items():
        if name in CATALOG:
            area, status, note = CATALOG[name]
            rows.append((STATUS_ORDER[status], area, name, status, len(v), len({o.module for o in v}), note, v))
    rows.sort(key=lambda r: (r[0], -r[4], r[1], r[2]))
    w("## Features by thin support\n")
    w("| status | area | Axon type | refs | modules | note | examples |")
    w("|---|---|---|---:|---:|---|---|")
    for _, area, name, status, n, mods, note, v in rows:
        targets = rep.annotations.get(name)
        tgt = f" ({', '.join(f'{k}:{c}' for k, c in targets.items())})" if targets else ""
        w(f"| {status} | {area} | `{name}`{tgt} | {n} | {mods} | {note} | {ex(v)} |")

    w("\n## Handler shapes\n")
    by_ann = Counter(h["annotation"] for h in rep.handlers)
    w(", ".join(f"`@{k}`: {c}" for k, c in by_ann.most_common()) or "_none_")
    w("")
    w("| shape | thin | count | examples |")
    w("|---|---|---:|---|")
    for shape, status, count, v in handler_findings(rep):
        w(f"| {shape} | {status} | {count} | {ex(v)} |")
    returns = Counter(h["returns"] or "?" for h in rep.handlers
                      if h["annotation"] == "CommandHandler" and h["kind"] == "method")
    if returns:
        w("\nCommand handler return types: " + ", ".join(f"`{k}`: {c}" for k, c in returns.most_common()))

    w("\n## Calls on Axon types\n")
    w("| call | count | examples |")
    w("|---|---:|---|")
    for k, v in sorted(rep.member_calls.items(), key=lambda kv: -len(kv[1])):
        w(f"| `{k}` | {len(v)} | {ex(v)} |")

    if rep.supertypes:
        w("\n## Classes implementing / extending Axon types\n")
        for k, v in sorted(rep.supertypes.items(), key=lambda kv: -len(kv[1])):
            w(f"- `{k}` × {len(v)}: {ex(v)}")
    if rep.bean_overrides:
        w("\n## `@Bean`s providing Axon infrastructure (engine customization)\n")
        for k, v in sorted(rep.bean_overrides.items(), key=lambda kv: -len(kv[1])):
            w(f"- `{k}` × {len(v)}: {ex(v)}")
    if rep.config_keys:
        w("\n## `axon.*` configuration keys\n")
        for k, v in rep.config_keys.items():
            w(f"- `{k}` — {ex(v)}")
    if rep.dependencies:
        w("\n## Axon dependencies\n")
        for k, v in sorted(rep.dependencies.items()):
            w(f"- `{k}`: {', '.join(sorted(v))}")
    if rep.unclassified:
        w("\n## Unclassified Axon types (add to CATALOG)\n")
        for k, v in sorted(rep.unclassified.items(), key=lambda kv: -len(kv[1])):
            w(f"- `{k}` × {len(v)}: {ex(v)}")
    w("\n## Files per module\n")
    for k, c in rep.modules.most_common():
        w(f"- {k}: {c}")
    return "\n".join(out) + "\n"


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("roots", nargs="+", type=Path, help="project root(s) to scan")
    ap.add_argument("-o", "--out", type=Path, default=Path("axon-usage-report"), help="output directory")
    ap.add_argument("--examples", type=int, default=5, help="example locations per item")
    ap.add_argument("--exclude", action="append", default=[], help="extra directory names to skip")
    ap.add_argument("--quiet", action="store_true", help="do not print the markdown summary")
    args = ap.parse_args(argv)

    roots = [r.resolve() for r in args.roots]
    for r in roots:
        if not r.is_dir():
            ap.error(f"not a directory: {r}")
    rep = scan(roots, DEFAULT_EXCLUDES | set(args.exclude))
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "axon-usage.json").write_text(json.dumps(to_json(rep, args.examples), indent=2, default=list))
    md = to_markdown(rep, args.examples)
    (args.out / "axon-usage.md").write_text(md)
    if not args.quiet:
        print(md)
    print(f"Wrote {args.out / 'axon-usage.md'} and {args.out / 'axon-usage.json'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
