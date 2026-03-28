import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import ProgrammingError


@dataclass
class SnowflakeSettings:
    account: str
    user: str
    password: str
    warehouse: str
    role: str
    database: str = "ARTICENCE_ORDERS"
    schema: str = "CUSTOMER_DATA"


@dataclass(frozen=True)
class DataSourceConfig:
    key: str
    object_name: str


@dataclass(frozen=True)
class DataSourceProfile:
    key: str
    object_name: str
    column_map: dict[str, str]


STANDARD_ORDER_COLUMNS: tuple[str, ...] = (
    "order_id",
    "customer_id",
    "device",
    "order_date",
    "apple_care",
    "order_value",
)

REQUIRED_ORDER_COLUMNS: tuple[str, ...] = (
    "customer_id",
    "device",
    "order_date",
    "apple_care",
    "order_value",
)

SYNTHETIC_ORDER_ID = "__synthetic_order_id__"

TOKEN_EXPANSIONS: dict[str, str] = {
    "cust": "customer",
    "usr": "user",
    "dt": "date",
    "amt": "amount",
    "ord": "order",
    "txn": "transaction",
    "tx": "transaction",
    "ref": "reference",
    "num": "number",
}

HEURISTIC_TOKEN_HINTS: dict[str, tuple[str, ...]] = {
    "order_id": ("order", "transaction", "purchase", "id", "reference", "number", "key"),
    "customer_id": ("customer", "client", "user", "account", "id", "reference", "key"),
    "device": ("device", "product", "item", "model", "category", "gadget", "type"),
    "order_date": ("order", "purchase", "transaction", "date", "timestamp", "time"),
    "apple_care": ("applecare", "apple", "care", "plan", "flag", "status"),
    "order_value": ("amount", "price", "total", "value", "cost"),
}


COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "order_id": (
        "order_id",
        "orderid",
        "id",
        "orderno",
        "order_no",
        "order_number",
        "order_num",
        "order_ref",
        "order_reference",
    ),
    "customer_id": (
        "customer_id",
        "customerid",
        "cust_id",
        "custid",
        "cust_identifier",
        "customer_identifier",
        "client_ref",
        "client_id",
        "client_identifier",
        "cid",
        "customer_key",
        "customer_no",
    ),
    "device": (
        "device",
        "product",
        "product_name",
        "product_type",
        "item",
        "item_name",
        "gadget",
        "gadget_type",
        "model",
        "device_name",
    ),
    "order_date": (
        "order_date",
        "orderdate",
        "purchase_date",
        "purchase_dt",
        "purchased_on",
        "created_at",
        "order_ts",
        "date",
    ),
    "apple_care": ("apple_care", "applecare", "has_applecare", "is_applecare", "apple_care_active", "care_plan"),
    "order_value": (
        "order_value",
        "ordervalue",
        "amount",
        "amount_value",
        "price",
        "total",
        "order_total",
        "total_price",
        "value",
    ),
}


class SnowflakeClient:
    def __init__(self, settings: SnowflakeSettings) -> None:
        self.settings = settings
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None
        self._source_configs: dict[str, DataSourceConfig] = self._build_source_configs()
        self._source_profiles: dict[str, DataSourceProfile] = {}
        self._primary_orders_object = os.getenv("SNOWFLAKE_PRIMARY_ORDERS_OBJECT", "orders")
        self._customer_cluster_cache: dict[tuple[str, int], tuple[float, list[dict[str, Any]]]] = {}
        self._customer_cluster_cache_lock = threading.Lock()

    @classmethod
    def from_env(cls) -> "SnowflakeClient":
        settings = SnowflakeSettings(
            account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
            user=os.getenv("SNOWFLAKE_USER", ""),
            password=os.getenv("SNOWFLAKE_PASSWORD", ""),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", ""),
            role=os.getenv("SNOWFLAKE_ROLE", ""),
            database=os.getenv("SNOWFLAKE_DATABASE", "ARTICENCE_ORDERS"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "CUSTOMER_DATA"),
        )

        required = [
            settings.account,
            settings.user,
            settings.password,
            settings.warehouse,
            settings.role,
        ]
        if not all(required):
            raise ValueError(
                "Missing Snowflake credentials. Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, "
                "SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, and SNOWFLAKE_ROLE."
            )

        return cls(settings)

    def connect(self) -> None:
        self._conn = snowflake.connector.connect(
            account=self.settings.account,
            user=self.settings.user,
            password=self.settings.password,
            warehouse=self.settings.warehouse,
            role=self.settings.role,
            database=self.settings.database,
            schema=self.settings.schema,
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _require_conn(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None:
            raise RuntimeError("Snowflake connection is not initialized. Call connect() first.")
        return self._conn

    @staticmethod
    def _normalize_identifier(value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", str(value or "").lower())

    @staticmethod
    def _tokenize_identifier(value: str) -> set[str]:
        raw_tokens = [token for token in re.split(r"[^A-Za-z0-9]+", str(value or "").strip()) if token]
        tokens: set[str] = set()
        for token in raw_tokens:
            lowered = token.lower()
            tokens.add(lowered)
            tokens.add(TOKEN_EXPANSIONS.get(lowered, lowered))
        # Also keep normalized single token for names like ORDERREF or USERIDKEY.
        normalized = re.sub(r"[^a-z0-9]", "", str(value or "").lower())
        if normalized:
            tokens.add(normalized)
        return tokens

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        escaped = str(identifier).replace('"', '""')
        return f'"{escaped}"'

    @classmethod
    def _sql_object_reference(cls, object_name: str) -> str:
        # Accept either raw names (orders_sf1_view), qualified names (db.schema.view),
        # or already-quoted parts. For unquoted names, Snowflake stores uppercase identifiers.
        parts = [part.strip() for part in str(object_name).split(".") if part.strip()]
        if not parts:
            raise ValueError("Snowflake object name cannot be empty.")

        normalized_parts: list[str] = []
        for part in parts:
            if part.startswith('"') and part.endswith('"'):
                normalized_parts.append(part)
            else:
                normalized_parts.append(cls._quote_identifier(part.upper()))
        return ".".join(normalized_parts)

    @staticmethod
    def _build_source_configs() -> dict[str, DataSourceConfig]:
        # Keep names configurable for production while preserving expected defaults.
        return {
            "sf1": DataSourceConfig(key="sf1", object_name=os.getenv("SNOWFLAKE_ORDERS_SF1_OBJECT", "orders_sf1_view")),
            "sf10": DataSourceConfig(key="sf10", object_name=os.getenv("SNOWFLAKE_ORDERS_SF10_OBJECT", "orders_sf10_view")),
            "sf100": DataSourceConfig(key="sf100", object_name=os.getenv("SNOWFLAKE_ORDERS_SF100_OBJECT", "orders_sf100_mv")),
            "sf1000": DataSourceConfig(key="sf1000", object_name=os.getenv("SNOWFLAKE_ORDERS_SF1000_OBJECT", "orders_sf1000_mv")),
        }

    def list_data_sources(self) -> list[str]:
        return sorted(self._source_configs.keys())

    def get_customer_cluster_cache(
        self,
        source: str,
        customer_id: int,
        ttl_seconds: float,
    ) -> Optional[list[dict[str, Any]]]:
        if ttl_seconds <= 0:
            return None
        cache_key = (str(source).lower(), int(customer_id))
        now = time.monotonic()
        with self._customer_cluster_cache_lock:
            cached = self._customer_cluster_cache.get(cache_key)
            if cached is None:
                return None
            cached_at, rows = cached
            if (now - cached_at) > ttl_seconds:
                self._customer_cluster_cache.pop(cache_key, None)
                return None
            return rows

    def set_customer_cluster_cache(
        self,
        source: str,
        customer_id: int,
        rows: list[dict[str, Any]],
    ) -> None:
        cache_key = (str(source).lower(), int(customer_id))
        with self._customer_cluster_cache_lock:
            self._customer_cluster_cache[cache_key] = (time.monotonic(), rows)

    def _normalize_source_key(self, source: str | None) -> str:
        requested = str(source or os.getenv("DEFAULT_DATA_SOURCE", "sf1")).strip().lower()
        if requested not in self._source_configs:
            available = ", ".join(self.list_data_sources())
            raise ValueError(f"Unsupported data source '{requested}'. Expected one of: {available}.")
        return requested

    def _list_source_columns(self, source_key: str) -> list[str]:
        conn = self._require_conn()
        source_object = self._source_configs[source_key].object_name
        object_ref = self._sql_object_reference(source_object)
        query = f"SELECT * FROM {object_ref} LIMIT 0"
        with conn.cursor() as cursor:
            try:
                cursor.execute(query)
            except ProgrammingError as exc:
                raise RuntimeError(
                    f"Unable to access source '{source_key}' ({source_object}). "
                    "Verify object name, schema/database, and role permissions."
                ) from exc
            return [str(col[0]) for col in (cursor.description or [])]

    def _score_column_match(self, actual_column: str, aliases: tuple[str, ...]) -> int:
        normalized_actual = self._normalize_identifier(actual_column)
        best = 0
        for alias in aliases:
            normalized_alias = self._normalize_identifier(alias)
            if not normalized_alias:
                continue
            if normalized_actual == normalized_alias:
                best = max(best, 100)
            elif normalized_actual.startswith(normalized_alias) or normalized_alias.startswith(normalized_actual):
                best = max(best, 70)
            elif normalized_alias in normalized_actual or normalized_actual in normalized_alias:
                best = max(best, 50)
        return best

    def _heuristic_column_score(self, standard_col: str, actual_column: str) -> int:
        tokens = self._tokenize_identifier(actual_column)
        hints = set(HEURISTIC_TOKEN_HINTS.get(standard_col, ()))
        if not tokens or not hints:
            return 0

        overlap = len(tokens.intersection(hints))
        if overlap == 0:
            return 0

        # Enforce stronger disambiguation for *_id columns.
        if standard_col == "order_id":
            has_order_context = any(t in tokens for t in ("order", "transaction", "purchase", "ord", "txn"))
            has_id_context = any(t in tokens for t in ("id", "reference", "number", "key", "ref", "num"))
            if not (has_order_context and has_id_context):
                return 0

        if standard_col == "customer_id":
            has_customer_context = any(t in tokens for t in ("customer", "client", "user", "account", "cust", "usr"))
            has_id_context = any(t in tokens for t in ("id", "reference", "number", "key", "ref", "num"))
            if not (has_customer_context and has_id_context):
                return 0

        score = 30 + (overlap * 15)
        return min(95, score)

    def _resolve_column_map(self, source_key: str) -> dict[str, str]:
        actual_columns = self._list_source_columns(source_key)
        if not actual_columns:
            raise RuntimeError(f"No columns found for data source '{source_key}'.")

        candidates: list[tuple[int, int, str, str]] = []
        # (score, alias_score, standard_col, actual_col)
        for standard_col in STANDARD_ORDER_COLUMNS:
            aliases = COLUMN_ALIASES[standard_col]
            for column in actual_columns:
                alias_score = self._score_column_match(column, aliases)
                heuristic_score = self._heuristic_column_score(standard_col, column)
                score = max(alias_score, heuristic_score)
                if score > 0:
                    candidates.append((score, alias_score, standard_col, column))

        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)

        resolved: dict[str, str] = {}
        used_columns: set[str] = set()
        for score, alias_score, standard_col, column in candidates:
            if standard_col in resolved:
                continue
            if column in used_columns:
                continue
            if score <= 0 and alias_score <= 0:
                continue
            resolved[standard_col] = column
            used_columns.add(column)

        unresolved_required = [col for col in REQUIRED_ORDER_COLUMNS if col not in resolved]
        if unresolved_required:
            raise RuntimeError(
                f"Unable to map required columns {unresolved_required} for data source '{source_key}'. "
                f"Available columns: {actual_columns}"
            )

        # Some datasets do not carry a reliable order id. Synthesize one when needed.
        if "order_id" not in resolved:
            resolved["order_id"] = SYNTHETIC_ORDER_ID

        return resolved

    def get_source_profile(self, source: str | None = None) -> DataSourceProfile:
        source_key = self._normalize_source_key(source)
        cached = self._source_profiles.get(source_key)
        if cached is not None:
            return cached

        config = self._source_configs[source_key]
        profile = DataSourceProfile(
            key=source_key,
            object_name=config.object_name,
            column_map=self._resolve_column_map(source_key),
        )
        self._source_profiles[source_key] = profile
        return profile

    def _build_source_orders_select(self, profile: DataSourceProfile) -> str:
        object_ref = self._sql_object_reference(profile.object_name)
        customer_id_col = self._quote_identifier(profile.column_map["customer_id"])
        device_col = self._quote_identifier(profile.column_map["device"])
        order_date_col = self._quote_identifier(profile.column_map["order_date"])
        apple_care_col = self._quote_identifier(profile.column_map["apple_care"])
        order_value_col = self._quote_identifier(profile.column_map["order_value"])
        order_id_mapped = profile.column_map["order_id"]

        if order_id_mapped == SYNTHETIC_ORDER_ID:
            order_id_expr = (
                f"MD5(CONCAT_WS('|', TO_VARCHAR({customer_id_col}), TO_VARCHAR({order_date_col}), "
                f"TO_VARCHAR({device_col}), TO_VARCHAR({order_value_col})))"
            )
        else:
            order_id_expr = self._quote_identifier(order_id_mapped)

        select_items = [
            f"TO_VARCHAR({order_id_expr}) AS order_id",
            f"{customer_id_col} AS customer_id",
            f"{device_col} AS device",
            f"{order_date_col} AS order_date",
            (
                'IFF('
                f"{apple_care_col} IN (TRUE, 1, '1', 'TRUE', 'true', 'Y', 'y', 'YES', 'yes'), "
                'TRUE, FALSE) AS apple_care'
            ),
            f"{order_value_col} AS order_value",
        ]
        return (
            f'SELECT {", ".join(select_items)} '
            f"FROM {object_ref} "
            'WHERE "{customer_col}" = %s'
        ).format(customer_col=profile.column_map["customer_id"])

    def _build_primary_orders_select(self) -> str:
        object_ref = self._sql_object_reference(self._primary_orders_object)
        return (
            "SELECT "
            "TO_VARCHAR(order_id) AS order_id, "
            "customer_id AS customer_id, "
            "device AS device, "
            "order_date AS order_date, "
            "IFF(apple_care IN (TRUE, 1, '1', 'TRUE', 'true', 'Y', 'y', 'YES', 'yes'), TRUE, FALSE) AS apple_care, "
            "order_value AS order_value "
            f"FROM {object_ref} "
            "WHERE customer_id = %s"
        )

    def _build_union_orders_query(self, profile: DataSourceProfile) -> str:
        source_select = self._build_source_orders_select(profile)
        primary_select = self._build_primary_orders_select()
        return (
            "WITH merged_orders AS ("
            f"{source_select} UNION ALL {primary_select}"
            "), deduped AS ("
            "SELECT order_id, customer_id, device, order_date, apple_care, order_value, "
            "ROW_NUMBER() OVER ("
            "PARTITION BY order_id, customer_id, device "
            "ORDER BY order_date DESC"
            ") AS rn "
            "FROM merged_orders"
            ") "
            "SELECT order_id, customer_id, device, order_date, apple_care, order_value "
            "FROM deduped "
            "WHERE rn = 1 "
            "ORDER BY order_date DESC"
        )

    @staticmethod
    def _normalize_row_keys(row: dict[str, Any]) -> dict[str, Any]:
        return {str(key).lower(): value for key, value in row.items()}

    @staticmethod
    def _digits_only(phone: str) -> str:
        return "".join(ch for ch in str(phone) if ch.isdigit())

    def verify_customer(self, phone: str, email: str, source: str | None = None) -> Optional[dict[str, Any]]:
        conn = self._require_conn()
        normalized_phone = self._digits_only(phone)
        if not normalized_phone:
            return None

        # Validate source at verification time so invalid connector selection fails fast.
        self.get_source_profile(source)

        trailing_10 = normalized_phone[-10:]
        query = """
            SELECT customer_id, customer_name, phone, email
            FROM customers
            WHERE (
                REGEXP_REPLACE(phone, '[^0-9]', '') = %s
                OR RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 10) = %s
            )
              AND LOWER(email) = LOWER(%s)
            LIMIT 1
        """

        with conn.cursor(DictCursor) as cursor:
            cursor.execute(query, (normalized_phone, trailing_10, email))
            row = cursor.fetchone()
            return self._normalize_row_keys(dict(row)) if row else None

    def fetch_orders(self, customer_id: int, source: str | None = None) -> list[dict[str, Any]]:
        conn = self._require_conn()
        profile = self.get_source_profile(source)
        query = self._build_union_orders_query(profile)

        with conn.cursor(DictCursor) as cursor:
            cursor.execute(query, (customer_id, customer_id))
            rows = cursor.fetchall()
            return [self._normalize_row_keys(dict(row)) for row in rows]
