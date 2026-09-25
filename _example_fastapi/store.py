"""MySQL access for mydb.mytable on the server in ../_persist."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

import pymysql
from pymysql.constants.ER import DUP_ENTRY
from pymysql.cursors import DictCursor

TABLE = "mytable"


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


@dataclass
class Person:
    id: int
    name: str
    email: str
    phone_numbers: list[str]
    created_at: datetime


@dataclass
class Filter:
    name: str = ""
    email: str = ""
    phone: str = ""
    created_after: datetime | None = None
    created_before: datetime | None = None

    def where(self) -> tuple[str, list[Any]]:
        conds: list[str] = []
        args: list[Any] = []
        if self.name:
            conds.append("name LIKE %s")
            args.append(like_contains(self.name))
        if self.email:
            conds.append("email LIKE %s")
            args.append(like_contains(self.email))
        if self.phone:
            conds.append("CAST(phone_numbers AS CHAR) LIKE %s")
            args.append(like_contains(self.phone))
        if self.created_after is not None:
            conds.append("created_at >= %s")
            args.append(self.created_after)
        if self.created_before is not None:
            conds.append("created_at <= %s")
            args.append(self.created_before)
        if not conds:
            return "", args
        return " WHERE " + " AND ".join(conds), args


@dataclass
class Page:
    people: list[Person]
    total: int


class Store(Protocol):
    def list(self, filt: Filter, page: int, size: int) -> Page: ...
    def get(self, person_id: int) -> Person: ...
    def insert(self, person: Person) -> Person: ...
    def update(self, person: Person) -> None: ...
    def delete(self, person_id: int) -> None: ...


def like_contains(value: str) -> str:
    """Substring match. MySQL's default LIKE escape is backslash."""
    out = ["%"]
    for ch in value:
        if ch in "\\%_":
            out.append("\\")
        out.append(ch)
    out.append("%")
    return "".join(out)


def normalize_phones(phones: list[str] | None) -> list[str]:
    return [] if phones is None else phones


class MySQLStore:
    """Writes use primary. List and get round-robin across replicas when set."""

    def __init__(self, primary: str, replicas: list[str], user: str, password: str, database: str):
        self._primary = primary
        self._replicas = replicas
        self._user = user
        self._password = password
        self._database = database
        self._next = 0
        self._lock = threading.Lock()
        self.ping(primary)
        for addr in replicas:
            self.ping(addr)

    def ping(self, addr: str) -> None:
        with self._connect(addr) as conn:
            conn.ping()

    def list(self, filt: Filter, page: int, size: int) -> Page:
        where, args = filt.where()
        with self._cursor(self._reader()) as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM {TABLE}{where}", args)
            total = int(cur.fetchone()["n"])
            offset = (page - 1) * size
            cur.execute(
                f"SELECT id, name, email, phone_numbers, created_at FROM {TABLE}{where} "
                "ORDER BY name, email LIMIT %s OFFSET %s",
                [*args, size, offset],
            )
            return Page(people=[row_person(row) for row in cur.fetchall()], total=total)

    def get(self, person_id: int) -> Person:
        with self._cursor(self._reader()) as cur:
            cur.execute(
                f"SELECT id, name, email, phone_numbers, created_at FROM {TABLE} WHERE id = %s",
                (person_id,),
            )
            row = cur.fetchone()
        if row is None:
            raise NotFound
        return row_person(row)

    def insert(self, person: Person) -> Person:
        phones = json.dumps(normalize_phones(person.phone_numbers))
        try:
            with self._cursor(self._primary) as cur:
                cur.execute(
                    f"INSERT INTO {TABLE} (name, email, phone_numbers, created_at) VALUES (%s, %s, %s, %s)",
                    (person.name, person.email, phones, person.created_at),
                )
                person.id = int(cur.lastrowid)
        except pymysql.IntegrityError as exc:
            if exc.args and exc.args[0] == DUP_ENTRY:
                raise Conflict from exc
            raise
        person.phone_numbers = normalize_phones(person.phone_numbers)
        return person

    def update(self, person: Person) -> None:
        phones = json.dumps(normalize_phones(person.phone_numbers))
        with self._cursor(self._primary) as cur:
            cur.execute(
                f"UPDATE {TABLE} SET name = %s, email = %s, phone_numbers = %s, created_at = %s WHERE id = %s",
                (person.name, person.email, phones, person.created_at, person.id),
            )
            if cur.rowcount == 0:
                raise NotFound

    def delete(self, person_id: int) -> None:
        with self._cursor(self._primary) as cur:
            cur.execute(f"DELETE FROM {TABLE} WHERE id = %s", (person_id,))
            if cur.rowcount == 0:
                raise NotFound

    def _reader(self) -> str:
        if not self._replicas:
            return self._primary
        with self._lock:
            addr = self._replicas[self._next % len(self._replicas)]
            self._next += 1
            return addr

    def _cursor(self, addr: str):
        return _Cursor(self._connect(addr))

    def _connect(self, addr: str) -> pymysql.Connection:
        host, port = split_host_port(addr)
        return pymysql.connect(
            host=host,
            port=port,
            user=self._user,
            password=self._password,
            database=self._database,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=DictCursor,
            connect_timeout=5,
        )


class _Cursor:
    def __init__(self, conn: pymysql.Connection):
        self._conn = conn
        self._cur: DictCursor | None = None

    def __enter__(self) -> DictCursor:
        self._cur = self._conn.cursor()
        return self._cur

    def __exit__(self, *exc: object) -> None:
        if self._cur is not None:
            self._cur.close()
        self._conn.close()


def row_person(row: dict[str, Any]) -> Person:
    created = row["created_at"]
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    else:
        created = created.astimezone(timezone.utc)
    return Person(
        id=int(row["id"]),
        name=row["name"],
        email=row["email"],
        phone_numbers=decode_phones(row["phone_numbers"]),
        created_at=created,
    )


def decode_phones(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list):
        raise TypeError("phone_numbers is not a JSON array")
    return [str(item) for item in value]


def split_host_port(addr: str) -> tuple[str, int]:
    host, sep, port = addr.rpartition(":")
    if not sep or not host:
        raise ValueError(f"address must be host:port, got {addr!r}")
    return host, int(port)
