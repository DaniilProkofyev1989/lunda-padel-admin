import json
import logging
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from config import DATABASE_URL

logger = logging.getLogger(__name__)

# Base schema — only creates tables, no columns that might not exist yet
_SCHEMA_BASE = """
CREATE TABLE IF NOT EXISTS events (
    uid TEXT PRIMARY KEY,
    raw_json JSONB NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scrape_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL,
    response_code INTEGER,
    events_count INTEGER,
    error_message TEXT
);
"""

# Indexes — created after columns are added
_SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_events_planned_date ON events(planned_date DESC);
CREATE INDEX IF NOT EXISTS idx_events_city ON events(city);
CREATE INDEX IF NOT EXISTS idx_events_game_status ON events(game_status);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_club_uid ON events(club_uid);
"""

# Columns added in schema v2 (parsed fields from raw_json)
_V2_COLUMNS = [
    ("type", "TEXT"),
    ("kind", "TEXT"),
    ("game_status", "TEXT"),
    ("planned_date", "TIMESTAMPTZ"),
    ("duration", "INTEGER"),
    ("city", "TEXT"),
    ("club_uid", "TEXT"),
    ("club_name", "TEXT"),
    ("owner_uid", "TEXT"),
    ("owner_name", "TEXT"),
    ("players_limit", "INTEGER"),
    ("count_players", "INTEGER"),
    ("min_grade", "TEXT"),
    ("max_grade", "TEXT"),
    ("display_min_grade", "TEXT"),
    ("display_max_grade", "TEXT"),
    ("price", "INTEGER"),
    ("ranking", "BOOLEAN"),
    ("court_booked", "BOOLEAN"),
    ("private_tour_game", "BOOLEAN"),
    ("title", "TEXT"),
    ("description", "TEXT"),
]

_SCHEMA_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS event_snapshots (
    id SERIAL PRIMARY KEY,
    event_uid TEXT NOT NULL,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    count_players INTEGER,
    game_status TEXT
);
CREATE INDEX IF NOT EXISTS idx_snapshots_event_uid ON event_snapshots(event_uid);
CREATE INDEX IF NOT EXISTS idx_snapshots_scraped_at ON event_snapshots(scraped_at);
"""

_MIGRATION_BACKFILL = """
UPDATE events SET
    type = raw_json->>'type',
    kind = raw_json->>'kind',
    game_status = raw_json->>'gameStatus',
    planned_date = (raw_json->>'plannedDate')::timestamptz,
    duration = (raw_json->>'duration')::integer,
    city = raw_json->>'city',
    club_uid = raw_json->'club'->>'uid',
    club_name = raw_json->'club'->>'caption',
    owner_uid = raw_json->'owner'->>'uid',
    owner_name = raw_json->'owner'->>'displayName',
    players_limit = (raw_json->>'playersLimit')::integer,
    count_players = (raw_json->>'countPlayers')::integer,
    min_grade = raw_json->>'minGrade',
    max_grade = raw_json->>'maxGrade',
    display_min_grade = raw_json->>'displayMinGrade',
    display_max_grade = raw_json->>'displayMaxGrade',
    price = (raw_json->>'price')::integer,
    ranking = (raw_json->>'ranking')::boolean,
    court_booked = (raw_json->>'courtBooked')::boolean,
    private_tour_game = (raw_json->>'privateTourGame')::boolean,
    title = raw_json->>'title',
    description = raw_json->>'description'
WHERE type IS NULL;
"""


def _parse_event(event: dict) -> dict:
    """Extract structured fields from raw event JSON."""
    club = event.get("club") or {}
    owner = event.get("owner") or {}
    return {
        "type": event.get("type"),
        "kind": event.get("kind"),
        "game_status": event.get("gameStatus"),
        "planned_date": event.get("plannedDate"),
        "duration": event.get("duration"),
        "city": event.get("city"),
        "club_uid": club.get("uid"),
        "club_name": club.get("caption"),
        "owner_uid": owner.get("uid"),
        "owner_name": owner.get("displayName"),
        "players_limit": event.get("playersLimit"),
        "count_players": event.get("countPlayers"),
        "min_grade": event.get("minGrade"),
        "max_grade": event.get("maxGrade"),
        "display_min_grade": event.get("displayMinGrade"),
        "display_max_grade": event.get("displayMaxGrade"),
        "price": event.get("price"),
        "ranking": event.get("ranking"),
        "court_booked": event.get("courtBooked"),
        "private_tour_game": event.get("privateTourGame"),
        "title": event.get("title"),
        "description": event.get("description"),
    }


class Database:
    def __init__(self, database_url: str | None = None):
        self._database_url = database_url or DATABASE_URL
        self.conn = psycopg.connect(self._database_url, row_factory=dict_row)
        self._init_schema()

    def _ensure_connected(self):
        """Reconnect if the connection was closed (e.g. Neon idle timeout)."""
        if self.conn.closed:
            logger.info("DB connection lost, reconnecting...")
            self.conn = psycopg.connect(self._database_url, row_factory=dict_row)

    def _reconnect(self):
        """Force close and reconnect (e.g. after SSL drop mid-query)."""
        try:
            self.conn.close()
        except Exception:
            pass
        logger.info("Reconnecting to DB...")
        self.conn = psycopg.connect(self._database_url, row_factory=dict_row)

    def _init_schema(self):
        with self.conn.cursor() as cur:
            # 1. Create base tables
            cur.execute(_SCHEMA_BASE)
            # 1b. Rename id → uid if old schema (idempotent)
            cur.execute("""
                DO $$ BEGIN
                    ALTER TABLE events RENAME COLUMN id TO uid;
                EXCEPTION WHEN undefined_column THEN NULL;
                END $$;
            """)
            # 2. Add v2 columns if they don't exist (idempotent migration)
            for col_name, col_type in _V2_COLUMNS:
                cur.execute(f"""
                    DO $$ BEGIN
                        ALTER TABLE events ADD COLUMN {col_name} {col_type};
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$;
                """)
            # 3. Create indexes (now that columns exist)
            cur.execute(_SCHEMA_INDEXES)
            # 4. Create snapshots table
            cur.execute(_SCHEMA_SNAPSHOTS)
            # 5. Backfill parsed fields for rows that don't have them yet
            cur.execute(_MIGRATION_BACKFILL)
        self.conn.commit()

    def upsert_event(self, event_id: str, raw_json: dict):
        """Insert new event or update existing one with parsed fields.

        Saves a snapshot to event_snapshots when count_players or game_status changes.
        """
        now = datetime.now(timezone.utc)
        parsed = _parse_event(raw_json)
        with self.conn.cursor() as cur:
            # Check current state for snapshot
            cur.execute(
                "SELECT count_players, game_status FROM events WHERE uid = %s",
                (str(event_id),),
            )
            existing = cur.fetchone()
            if existing and (
                existing["count_players"] != parsed["count_players"]
                or existing["game_status"] != parsed["game_status"]
            ):
                cur.execute(
                    """INSERT INTO event_snapshots (event_uid, scraped_at, count_players, game_status)
                       VALUES (%s, %s, %s, %s)""",
                    (str(event_id), now, existing["count_players"], existing["game_status"]),
                )
            elif not existing:
                # First time seeing this event — save initial snapshot
                cur.execute(
                    """INSERT INTO event_snapshots (event_uid, scraped_at, count_players, game_status)
                       VALUES (%s, %s, %s, %s)""",
                    (str(event_id), now, parsed["count_players"], parsed["game_status"]),
                )
            cur.execute(
                """
                INSERT INTO events (
                    uid, type, kind, game_status, planned_date, duration,
                    city, club_uid, club_name, owner_uid, owner_name,
                    players_limit, count_players,
                    min_grade, max_grade, display_min_grade, display_max_grade,
                    price, ranking, court_booked, private_tour_game,
                    title, description,
                    raw_json, first_seen, last_updated
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT(uid) DO UPDATE SET
                    type = EXCLUDED.type,
                    kind = EXCLUDED.kind,
                    game_status = EXCLUDED.game_status,
                    planned_date = EXCLUDED.planned_date,
                    duration = EXCLUDED.duration,
                    city = EXCLUDED.city,
                    club_uid = EXCLUDED.club_uid,
                    club_name = EXCLUDED.club_name,
                    owner_uid = EXCLUDED.owner_uid,
                    owner_name = EXCLUDED.owner_name,
                    players_limit = EXCLUDED.players_limit,
                    count_players = EXCLUDED.count_players,
                    min_grade = EXCLUDED.min_grade,
                    max_grade = EXCLUDED.max_grade,
                    display_min_grade = EXCLUDED.display_min_grade,
                    display_max_grade = EXCLUDED.display_max_grade,
                    price = EXCLUDED.price,
                    ranking = EXCLUDED.ranking,
                    court_booked = EXCLUDED.court_booked,
                    private_tour_game = EXCLUDED.private_tour_game,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    raw_json = EXCLUDED.raw_json,
                    last_updated = EXCLUDED.last_updated
                """,
                (
                    str(event_id),
                    parsed["type"], parsed["kind"], parsed["game_status"],
                    parsed["planned_date"], parsed["duration"],
                    parsed["city"], parsed["club_uid"], parsed["club_name"],
                    parsed["owner_uid"], parsed["owner_name"],
                    parsed["players_limit"], parsed["count_players"],
                    parsed["min_grade"], parsed["max_grade"],
                    parsed["display_min_grade"], parsed["display_max_grade"],
                    parsed["price"], parsed["ranking"],
                    parsed["court_booked"], parsed["private_tour_game"],
                    parsed["title"], parsed["description"],
                    Jsonb(raw_json), now, now,
                ),
            )

    def upsert_events(self, events: list[dict]) -> int:
        """Batch upsert a list of events. Returns count of processed events.

        Handles Neon SSL drops mid-batch: reconnects and retries failed events.
        """
        self._ensure_connected()
        count = 0
        for event in events:
            event_id = event.get("uid")
            if event_id is None:
                logger.warning("Event without uid, skipping")
                continue
            try:
                self.upsert_event(event_id, event)
            except psycopg.OperationalError as e:
                logger.warning("DB connection lost during upsert (uid=%s): %s", event_id, e)
                self._reconnect()
                self.upsert_event(event_id, event)
            count += 1
        self.conn.commit()
        return count

    def log_scrape(
        self,
        status: str,
        response_code: int | None = None,
        events_count: int | None = None,
        error_message: str | None = None,
    ):
        # Ensure connection is alive + rollback any failed transaction
        self._ensure_connected()
        try:
            self.conn.rollback()
        except Exception:
            pass
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scrape_log (status, response_code, events_count, error_message) VALUES (%s, %s, %s, %s)",
                (status, response_code, events_count, error_message),
            )
        self.conn.commit()

    # --- Admin panel queries ---

    def get_events_paginated(
        self,
        page: int = 1,
        per_page: int = 50,
        city: str | None = None,
        game_status: str | None = None,
        event_type: str | None = None,
        min_grade: str | None = None,
    ) -> tuple[list[dict], int]:
        """Return paginated events with optional filters. Returns (events, total_count)."""
        self._ensure_connected()
        conditions = []
        params = []

        if city:
            conditions.append("city = %s")
            params.append(city)
        if game_status:
            conditions.append("game_status = %s")
            params.append(game_status)
        if event_type:
            conditions.append("type = %s")
            params.append(event_type)
        if min_grade:
            conditions.append("min_grade = %s")
            params.append(min_grade)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM events {where}", params)
            total = cur.fetchone()["cnt"]

            offset = (page - 1) * per_page
            cur.execute(
                f"""SELECT uid, type, kind, game_status, planned_date, duration,
                           city, club_name, owner_name,
                           players_limit, count_players,
                           display_min_grade, display_max_grade,
                           price, title, first_seen, last_updated
                    FROM events {where}
                    ORDER BY planned_date DESC NULLS LAST
                    LIMIT %s OFFSET %s""",
                params + [per_page, offset],
            )
            events = cur.fetchall()

        return events, total

    def get_events_by_date(
        self,
        date: str,
        city: str | None = None,
        game_status: str | None = None,
        min_grade: str | None = None,
    ) -> list[dict]:
        """Return all events for a given date (YYYY-MM-DD), sorted by time ASC."""
        self._ensure_connected()
        conditions = ["planned_date::date = %s", "type = 'TOURNAMENT'"]
        params: list = [date]

        if city:
            conditions.append("city = %s")
            params.append(city)
        if game_status:
            conditions.append("game_status = %s")
            params.append(game_status)
        if min_grade:
            conditions.append("min_grade = %s")
            params.append(min_grade)

        where = "WHERE " + " AND ".join(conditions)

        with self.conn.cursor() as cur:
            cur.execute(
                f"""SELECT uid, type, kind, game_status, planned_date, duration,
                           city, club_name, owner_name,
                           players_limit, count_players,
                           display_min_grade, display_max_grade,
                           price, title, description, ranking, court_booked
                    FROM events {where}
                    ORDER BY planned_date ASC NULLS LAST""",
                params,
            )
            return cur.fetchall()

    def get_event_by_uid(self, uid: str) -> dict | None:
        self._ensure_connected()
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM events WHERE uid = %s", (uid,))
            return cur.fetchone()

    def get_scrape_stats(self) -> dict:
        """Get scraping statistics for dashboard."""
        self._ensure_connected()
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as total FROM events")
            total_events = cur.fetchone()["total"]

            cur.execute("""
                SELECT COUNT(*) as cnt FROM events
                WHERE first_seen > NOW() - INTERVAL '7 days'
            """)
            new_7d = cur.fetchone()["cnt"]

            cur.execute("""
                SELECT * FROM scrape_log
                ORDER BY timestamp DESC LIMIT 1
            """)
            last_scrape = cur.fetchone()

            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'success') as success_count,
                    COUNT(*) FILTER (WHERE status != 'success') as error_count
                FROM scrape_log
                WHERE timestamp > NOW() - INTERVAL '30 days'
            """)
            counts = cur.fetchone()

            cur.execute("""
                SELECT DATE(timestamp) as date,
                       SUM(events_count) as events,
                       COUNT(*) FILTER (WHERE status = 'success') as ok,
                       COUNT(*) FILTER (WHERE status != 'success') as err
                FROM scrape_log
                WHERE timestamp > NOW() - INTERVAL '30 days'
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
            """)
            daily = cur.fetchall()

        return {
            "total_events": total_events,
            "new_7d": new_7d,
            "last_scrape": last_scrape,
            "success_30d": counts["success_count"],
            "error_30d": counts["error_count"],
            "daily": daily,
        }

    def get_scrape_log_paginated(self, page: int = 1, per_page: int = 50) -> tuple[list[dict], int]:
        self._ensure_connected()
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM scrape_log")
            total = cur.fetchone()["cnt"]

            offset = (page - 1) * per_page
            cur.execute(
                "SELECT * FROM scrape_log ORDER BY timestamp DESC LIMIT %s OFFSET %s",
                (per_page, offset),
            )
            logs = cur.fetchall()

        return logs, total

    def get_filter_options(self) -> dict:
        """Get distinct values for filter dropdowns."""
        self._ensure_connected()
        with self.conn.cursor() as cur:
            cur.execute("SELECT DISTINCT city FROM events WHERE city IS NOT NULL ORDER BY city")
            cities = [r["city"] for r in cur.fetchall()]

            cur.execute("SELECT DISTINCT game_status FROM events WHERE game_status IS NOT NULL ORDER BY game_status")
            statuses = [r["game_status"] for r in cur.fetchall()]

            cur.execute("SELECT DISTINCT type FROM events WHERE type IS NOT NULL ORDER BY type")
            types = [r["type"] for r in cur.fetchall()]

            cur.execute("SELECT DISTINCT min_grade FROM events WHERE min_grade IS NOT NULL ORDER BY min_grade")
            grades = [r["min_grade"] for r in cur.fetchall()]

        return {"cities": cities, "statuses": statuses, "types": types, "grades": grades}

    def get_all_events(self) -> list[dict]:
        """Return all stored events as parsed dicts."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT uid, raw_json FROM events")
            rows = cur.fetchall()
        return [row["raw_json"] for row in rows]

    def get_event_count(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM events")
            row = cur.fetchone()
        return row["cnt"]

    # --- Tournament catalog ---

    def get_tournaments(
        self,
        page: int = 1,
        per_page: int = 50,
        city: str | None = None,
        game_status: str | None = None,
        min_grade: str | None = None,
        club_name: str | None = None,
        owner_name: str | None = None,
        price_min: int | None = None,
        price_max: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        sort: str = "planned_date",
        sort_dir: str = "desc",
    ) -> tuple[list[dict], int]:
        """Return paginated tournaments with filters and sorting."""
        self._ensure_connected()
        conditions = ["type = 'TOURNAMENT'"]
        params: list = []

        if city:
            conditions.append("city = %s")
            params.append(city)
        if game_status:
            conditions.append("game_status = %s")
            params.append(game_status)
        if min_grade:
            conditions.append("min_grade = %s")
            params.append(min_grade)
        if club_name:
            conditions.append("club_name = %s")
            params.append(club_name)
        if owner_name:
            conditions.append("owner_name = %s")
            params.append(owner_name)
        if price_min is not None:
            conditions.append("price >= %s")
            params.append(price_min)
        if price_max is not None:
            conditions.append("price <= %s")
            params.append(price_max)
        if date_from:
            conditions.append("planned_date::date >= %s")
            params.append(date_from)
        if date_to:
            conditions.append("planned_date::date <= %s")
            params.append(date_to)

        where = "WHERE " + " AND ".join(conditions)

        allowed_sorts = {
            "planned_date", "price", "count_players", "club_name",
            "owner_name", "display_min_grade",
        }
        if sort not in allowed_sorts:
            sort = "planned_date"
        direction = "ASC" if sort_dir == "asc" else "DESC"

        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM events {where}", params)
            total = cur.fetchone()["cnt"]

            offset = (page - 1) * per_page
            cur.execute(
                f"""SELECT uid, kind, game_status, planned_date,
                           city, club_name, owner_name,
                           players_limit, count_players,
                           display_min_grade, display_max_grade,
                           price, title, ranking
                    FROM events {where}
                    ORDER BY {sort} {direction} NULLS LAST
                    LIMIT %s OFFSET %s""",
                params + [per_page, offset],
            )
            events = cur.fetchall()

        return events, total

    def get_tournament_filters(self) -> dict:
        """Get distinct values for tournament filter dropdowns."""
        self._ensure_connected()
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT city FROM events
                WHERE type = 'TOURNAMENT' AND city IS NOT NULL ORDER BY city
            """)
            cities = [r["city"] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT game_status FROM events
                WHERE type = 'TOURNAMENT' AND game_status IS NOT NULL ORDER BY game_status
            """)
            statuses = [r["game_status"] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT min_grade FROM events
                WHERE type = 'TOURNAMENT' AND min_grade IS NOT NULL ORDER BY min_grade
            """)
            grades = [r["min_grade"] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT club_name FROM events
                WHERE type = 'TOURNAMENT' AND club_name IS NOT NULL ORDER BY club_name
            """)
            clubs = [r["club_name"] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT owner_name FROM events
                WHERE type = 'TOURNAMENT' AND owner_name IS NOT NULL ORDER BY owner_name
            """)
            owners = [r["owner_name"] for r in cur.fetchall()]

        return {
            "cities": cities,
            "statuses": statuses,
            "grades": grades,
            "clubs": clubs,
            "owners": owners,
        }

    def get_tournament_history(self, uid: str) -> list[dict]:
        """Return snapshots of count_players/game_status over time for a tournament."""
        self._ensure_connected()
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT scraped_at, count_players, game_status
                   FROM event_snapshots
                   WHERE event_uid = %s
                   ORDER BY scraped_at ASC""",
                (uid,),
            )
            return cur.fetchall()

    def get_tournaments_daily_stats(
        self, date_from: str, date_to: str,
    ) -> list[dict]:
        """Return tournament count per day in the given date range."""
        self._ensure_connected()
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT planned_date::date as date, COUNT(*) as count
                   FROM events
                   WHERE type = 'TOURNAMENT'
                     AND planned_date::date BETWEEN %s AND %s
                   GROUP BY planned_date::date
                   ORDER BY date""",
                (date_from, date_to),
            )
            return cur.fetchall()

    def get_analytics_chart_data(
        self, date_from: str, date_to: str, metric: str = "count",
    ) -> list[dict]:
        """Return daily chart data for the selected metric.

        Metrics: count, avg_fill, avg_price, total_players, avg_players.
        """
        self._ensure_connected()
        metric_sql = {
            "count": "COUNT(*) as value",
            "avg_fill": "ROUND(AVG(count_players::numeric / NULLIF(players_limit, 0) * 100), 1) as value",
            "avg_price": "ROUND(AVG(price)::numeric, 0) as value",
            "total_players": "SUM(count_players) as value",
            "avg_players": "ROUND(AVG(count_players)::numeric, 1) as value",
        }
        select_expr = metric_sql.get(metric, metric_sql["count"])

        with self.conn.cursor() as cur:
            cur.execute(
                f"""SELECT planned_date::date as date, {select_expr}
                    FROM events
                    WHERE type = 'TOURNAMENT'
                      AND planned_date::date BETWEEN %s AND %s
                    GROUP BY planned_date::date
                    ORDER BY date""",
                (date_from, date_to),
            )
            return cur.fetchall()

    def get_analytics_ratings(self, date_from: str, date_to: str) -> dict:
        """Return rating tables for analytics page."""
        self._ensure_connected()
        base_where = """
            WHERE type = 'TOURNAMENT'
              AND planned_date::date BETWEEN %s AND %s
        """
        params = [date_from, date_to]

        with self.conn.cursor() as cur:
            # Total tournaments in period (for percentage calculations)
            cur.execute(
                f"SELECT COUNT(*) as total, SUM(count_players) as total_players FROM events {base_where}",
                params,
            )
            totals = cur.fetchone()
            total_tournaments = totals["total"] or 1
            total_players_all = totals["total_players"] or 1

            # By organizer
            cur.execute(
                f"""SELECT owner_name as name,
                           COUNT(*) as tournaments,
                           SUM(count_players) as total_players,
                           ROUND(AVG(count_players::numeric / NULLIF(players_limit, 0) * 100), 1) as avg_fill,
                           ROUND(AVG(price)::numeric, 0) as avg_price,
                           ROUND(AVG(count_players)::numeric, 1) as avg_players
                    FROM events {base_where} AND owner_name IS NOT NULL
                    GROUP BY owner_name
                    ORDER BY tournaments DESC""",
                params,
            )
            by_organizer = cur.fetchall()

            # By club
            cur.execute(
                f"""SELECT club_name as name,
                           COUNT(*) as tournaments,
                           SUM(count_players) as total_players,
                           ROUND(AVG(count_players::numeric / NULLIF(players_limit, 0) * 100), 1) as avg_fill,
                           ROUND(AVG(price)::numeric, 0) as avg_price,
                           ROUND(AVG(count_players)::numeric, 1) as avg_players
                    FROM events {base_where} AND club_name IS NOT NULL
                    GROUP BY club_name
                    ORDER BY tournaments DESC""",
                params,
            )
            by_club = cur.fetchall()

            # By grade
            cur.execute(
                f"""SELECT COALESCE(display_min_grade, 'Не указан') as name,
                           COUNT(*) as tournaments,
                           SUM(count_players) as total_players,
                           ROUND(AVG(count_players::numeric / NULLIF(players_limit, 0) * 100), 1) as avg_fill,
                           ROUND(AVG(price)::numeric, 0) as avg_price,
                           ROUND(AVG(count_players)::numeric, 1) as avg_players
                    FROM events {base_where}
                    GROUP BY display_min_grade
                    ORDER BY tournaments DESC""",
                params,
            )
            by_grade = cur.fetchall()

            # By hour of day
            cur.execute(
                f"""SELECT EXTRACT(HOUR FROM planned_date)::int as hour,
                           COUNT(*) as tournaments,
                           SUM(count_players) as total_players,
                           ROUND(AVG(count_players::numeric / NULLIF(players_limit, 0) * 100), 1) as avg_fill,
                           ROUND(AVG(price)::numeric, 0) as avg_price,
                           ROUND(AVG(count_players)::numeric, 1) as avg_players
                    FROM events {base_where} AND planned_date IS NOT NULL
                    GROUP BY hour
                    ORDER BY hour""",
                params,
            )
            by_hour = cur.fetchall()

            # By day of week
            cur.execute(
                f"""SELECT EXTRACT(DOW FROM planned_date)::int as dow,
                           COUNT(*) as tournaments,
                           SUM(count_players) as total_players,
                           ROUND(AVG(count_players::numeric / NULLIF(players_limit, 0) * 100), 1) as avg_fill,
                           ROUND(AVG(price)::numeric, 0) as avg_price,
                           ROUND(AVG(count_players)::numeric, 1) as avg_players
                    FROM events {base_where} AND planned_date IS NOT NULL
                    GROUP BY dow
                    ORDER BY dow""",
                params,
            )
            by_dow = cur.fetchall()

            # By price range (step 500₽)
            cur.execute(
                f"""SELECT
                        CASE
                            WHEN price IS NULL OR price = 0 THEN 'Бесплатно'
                            WHEN price < 1000 THEN 'до 1000 ₽'
                            WHEN price < 1500 THEN '1000–1499 ₽'
                            WHEN price < 2000 THEN '1500–1999 ₽'
                            WHEN price < 2500 THEN '2000–2499 ₽'
                            WHEN price < 3000 THEN '2500–2999 ₽'
                            WHEN price < 3500 THEN '3000–3499 ₽'
                            WHEN price < 4000 THEN '3500–3999 ₽'
                            WHEN price < 4500 THEN '4000–4499 ₽'
                            WHEN price < 5000 THEN '4500–4999 ₽'
                            WHEN price < 5500 THEN '5000–5499 ₽'
                            WHEN price < 6000 THEN '5500–5999 ₽'
                            WHEN price < 6500 THEN '6000–6499 ₽'
                            WHEN price < 7000 THEN '6500–6999 ₽'
                            WHEN price < 7500 THEN '7000–7499 ₽'
                            WHEN price < 8000 THEN '7500–7999 ₽'
                            ELSE '8000+ ₽'
                        END as name,
                        MIN(CASE
                            WHEN price IS NULL OR price = 0 THEN 0
                            WHEN price < 1000 THEN 1
                            WHEN price < 1500 THEN 2
                            WHEN price < 2000 THEN 3
                            WHEN price < 2500 THEN 4
                            WHEN price < 3000 THEN 5
                            WHEN price < 3500 THEN 6
                            WHEN price < 4000 THEN 7
                            WHEN price < 4500 THEN 8
                            WHEN price < 5000 THEN 9
                            WHEN price < 5500 THEN 10
                            WHEN price < 6000 THEN 11
                            WHEN price < 6500 THEN 12
                            WHEN price < 7000 THEN 13
                            WHEN price < 7500 THEN 14
                            WHEN price < 8000 THEN 15
                            ELSE 16
                        END) as sort_order,
                        COUNT(*) as tournaments,
                        SUM(count_players) as total_players,
                        ROUND(AVG(count_players::numeric / NULLIF(players_limit, 0) * 100), 1) as avg_fill,
                        ROUND(AVG(price)::numeric, 0) as avg_price,
                        ROUND(AVG(count_players)::numeric, 1) as avg_players
                    FROM events {base_where}
                    GROUP BY name
                    ORDER BY sort_order""",
                params,
            )
            by_price = cur.fetchall()

        return {
            "total_tournaments": total_tournaments,
            "total_players": total_players_all,
            "by_organizer": by_organizer,
            "by_club": by_club,
            "by_grade": by_grade,
            "by_hour": by_hour,
            "by_dow": by_dow,
            "by_price": by_price,
        }

    def close(self):
        self.conn.close()
