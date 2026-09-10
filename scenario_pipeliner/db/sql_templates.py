from __future__ import annotations

from scenario_pipeliner.db.exceptions import PluginSqlSplitError, PluginSqlTemplateError

CORE_SCHEMA_PLACEHOLDER = "{{core_schema}}"
PLUGIN_SCHEMA_PLACEHOLDER = "{{plugin_schema}}"

_TXN_KEYWORDS = frozenset({"BEGIN", "COMMIT", "ROLLBACK", "START"})


def render_plugin_sql(
    sql: str,
    *,
    core_schema: str,
    plugin_schema: str,
) -> str:
    if CORE_SCHEMA_PLACEHOLDER not in sql or PLUGIN_SCHEMA_PLACEHOLDER not in sql:
        raise PluginSqlTemplateError(
            "plugin SQL must contain both "
            f"{CORE_SCHEMA_PLACEHOLDER} and {PLUGIN_SCHEMA_PLACEHOLDER}"
        )
    rendered = sql.replace(CORE_SCHEMA_PLACEHOLDER, core_schema).replace(
        PLUGIN_SCHEMA_PLACEHOLDER, plugin_schema
    )
    if "{{" in rendered:
        raise PluginSqlTemplateError(
            "plugin SQL still contains unresolved '{{' placeholders after render"
        )
    return rendered


def split_sql_statements(sql: str) -> list[str]:
    """Split on ``;`` outside quotes, comments, and dollar-quoted bodies.

    Apply already runs inside a transaction, so top-level ``BEGIN`` /
    ``COMMIT`` / ``ROLLBACK`` / ``START TRANSACTION`` are rejected.
    """
    statements: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":
            end = sql.find("\n", i)
            if end == -1:
                buf.append(sql[i:])
                break
            buf.append(sql[i : end + 1])
            i = end + 1
            continue
        if ch == "/" and nxt == "*":
            end = sql.find("*/", i + 2)
            if end == -1:
                raise PluginSqlSplitError("unterminated block comment in plugin SQL")
            buf.append(sql[i : end + 2])
            i = end + 2
            continue
        if ch in {"'", '"'}:
            j = _scan_quoted(sql, i, ch)
            buf.append(sql[i:j])
            i = j
            continue
        if ch == "$":
            tag, tag_end = _dollar_tag(sql, i)
            if tag is not None:
                close = sql.find(tag, tag_end)
                if close == -1:
                    raise PluginSqlSplitError(
                        "unterminated dollar-quoted body in plugin SQL"
                    )
                end = close + len(tag)
                buf.append(sql[i:end])
                i = end
                continue
        if ch == ";":
            statement = "".join(buf).strip()
            if statement:
                _reject_transaction_control(statement)
                statements.append(statement)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        _reject_transaction_control(tail)
        statements.append(tail)
    return statements


def _scan_quoted(sql: str, start: int, quote: str) -> int:
    n = len(sql)
    j = start + 1
    while j < n:
        if sql[j] == quote:
            if j + 1 < n and sql[j + 1] == quote:
                j += 2
                continue
            return j + 1
        j += 1
    kind = "string literal" if quote == "'" else "quoted identifier"
    raise PluginSqlSplitError(f"unterminated {kind} in plugin SQL")


def _dollar_tag(sql: str, start: int) -> tuple[str | None, int]:
    if start >= len(sql) or sql[start] != "$":
        return None, start
    i = start + 1
    while i < len(sql) and (sql[i].isalnum() or sql[i] == "_"):
        i += 1
    if i < len(sql) and sql[i] == "$":
        return sql[start : i + 1], i + 1
    return None, start


def _reject_transaction_control(statement: str) -> None:
    body = statement.lstrip()
    while body.startswith("--") or body.startswith("/*"):
        if body.startswith("--"):
            nl = body.find("\n")
            if nl == -1:
                return
            body = body[nl + 1 :].lstrip()
            continue
        end = body.find("*/")
        if end == -1:
            return
        body = body[end + 2 :].lstrip()
    keyword = ""
    for ch in body:
        if ch.isalpha():
            keyword += ch.upper()
            continue
        break
    if keyword in _TXN_KEYWORDS:
        raise PluginSqlSplitError(
            "plugin SQL must not contain BEGIN/COMMIT/ROLLBACK/START TRANSACTION "
            "(apply already runs in a transaction)"
        )
