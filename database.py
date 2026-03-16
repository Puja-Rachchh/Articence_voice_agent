import os
from dataclasses import dataclass
from typing import Any, Optional

import snowflake.connector
from snowflake.connector import DictCursor


@dataclass
class SnowflakeSettings:
    account: str
    user: str
    password: str
    warehouse: str
    role: str
    database: str = "ARTICENCE_ORDERS"
    schema: str = "CUSTOMER_DATA"


class SnowflakeClient:
    def __init__(self, settings: SnowflakeSettings) -> None:
        self.settings = settings
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

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
    def _normalize_row_keys(row: dict[str, Any]) -> dict[str, Any]:
        return {str(key).lower(): value for key, value in row.items()}

    @staticmethod
    def _digits_only(phone: str) -> str:
        return "".join(ch for ch in str(phone) if ch.isdigit())

    def verify_customer(self, phone: str, email: str) -> Optional[dict[str, Any]]:
        conn = self._require_conn()
        normalized_phone = self._digits_only(phone)
        if not normalized_phone:
            return None

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

    def fetch_orders(self, customer_id: int) -> list[dict[str, Any]]:
        conn = self._require_conn()
        query = """
            SELECT order_id, customer_id, device, order_date, apple_care, order_value
            FROM orders
            WHERE customer_id = %s
            ORDER BY order_date DESC
        """

        with conn.cursor(DictCursor) as cursor:
            cursor.execute(query, (customer_id,))
            rows = cursor.fetchall()
            return [self._normalize_row_keys(dict(row)) for row in rows]
