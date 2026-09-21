# coding=utf-8
"""
SQLite 存储 Mixin

提供共用的 SQLite 数据库操作逻辑，供 LocalStorageBackend 和 RemoteStorageBackend 复用。
"""

import functools
import logging
import sqlite3
from abc import abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar

from trendradar.storage.base import NewsItem, NewsData, RSSItem, RSSData, StorageError
from trendradar.utils.sqlite import execute_chunked_in
from trendradar.utils.url import normalize_url

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


def _is_url_like_title(title: Optional[str]) -> bool:
    return bool(title) and title.strip().startswith(("http://", "https://", "//"))


def _db_operation(
    description: str,
    *,
    default: Any = None,
    critical: bool = False,
) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
    """
    统一的 SQLite 操作包装器。

    失败时回滚所有缓存连接上未提交的事务，并带堆栈记录错误日志。
    - critical=False: 返回 default（可调用时视为工厂），供容错读取/写入使用
    - critical=True: 抛出 StorageError，供去重/幂等判断等不可失效开放的方法使用
    """

    def decorator(func: Callable[..., _T]) -> Callable[..., _T]:
        @functools.wraps(func)
        def wrapper(self: "SQLiteStorageMixin", *args: Any, **kwargs: Any) -> _T:
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                self._rollback_connections()
                if not isinstance(e, sqlite3.Error):
                    raise
                logger.error("%s (%s): %s", description, func.__name__, e, exc_info=True)
                if critical:
                    raise StorageError(f"{description}: {e}") from e
                return default() if callable(default) else default

        return wrapper

    return decorator


class SQLiteStorageMixin:
    """
    SQLite 存储操作 Mixin

    子类需要实现以下抽象方法：
    - _get_connection(date, db_type) -> sqlite3.Connection
    - _get_configured_time() -> datetime
    - _format_date_folder(date) -> str
    - _format_time_filename() -> str
    """

    # ========================================
    # 抽象方法 - 子类必须实现
    # ========================================

    @abstractmethod
    def _get_connection(self, date: Optional[str] = None, db_type: str = "news") -> sqlite3.Connection:
        """获取数据库连接"""
        pass

    @abstractmethod
    def _get_configured_time(self) -> datetime:
        """获取配置时区的当前时间"""
        pass

    @abstractmethod
    def _format_date_folder(self, date: Optional[str] = None) -> str:
        """格式化日期文件夹名 (ISO 格式: YYYY-MM-DD)"""
        pass

    @abstractmethod
    def _format_time_filename(self) -> str:
        """格式化时间文件名 (格式: HH-MM)"""
        pass

    _db_connections: Dict[str, sqlite3.Connection]

    def _rollback_connections(self) -> None:
        """回滚所有缓存连接上未提交的事务，避免脏事务被后续操作提交"""
        for conn in self._db_connections.values():
            try:
                conn.rollback()
            except sqlite3.Error:
                logger.warning("回滚事务失败", exc_info=True)

    # ========================================
    # Schema 管理
    # ========================================

    def _get_schema_path(self, db_type: str = "news") -> Path:
        """
        获取 schema.sql 文件路径

        Args:
            db_type: 数据库类型 ("news" 或 "rss")

        Returns:
            schema 文件路径
        """
        if db_type == "rss":
            return Path(__file__).parent / "rss_schema.sql"
        return Path(__file__).parent / "schema.sql"

    def _get_ai_filter_schema_path(self) -> Path:
        """获取 AI 筛选 schema 文件路径"""
        return Path(__file__).parent / "ai_filter_schema.sql"

    def _init_tables(self, conn: sqlite3.Connection, db_type: str = "news") -> None:
        """
        从 schema.sql 初始化数据库表结构

        Args:
            conn: 数据库连接
            db_type: 数据库类型 ("news" 或 "rss")
        """
        schema_path = self._get_schema_path(db_type)

        if schema_path.exists():
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_sql = f.read()
            conn.executescript(schema_sql)
        else:
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        # news 库额外加载 AI 筛选表结构
        if db_type == "news":
            ai_filter_schema = self._get_ai_filter_schema_path()
            if ai_filter_schema.exists():
                with open(ai_filter_schema, "r", encoding="utf-8") as f:
                    conn.executescript(f.read())

        if db_type == "rss":
            self._migrate_rss_schema(conn)

        conn.commit()

    def _migrate_rss_schema(self, conn: sqlite3.Connection) -> None:
        """迁移 rss_items 表结构（为已有数据库添加 guid 列）"""
        cursor = conn.execute("PRAGMA table_info(rss_items)")
        columns = {row[1] for row in cursor.fetchall()}
        if "guid" not in columns:
            conn.execute("ALTER TABLE rss_items ADD COLUMN guid TEXT DEFAULT ''")
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_rss_guid_feed
                ON rss_items(guid, feed_id) WHERE guid != ''
            """)

    # ========================================
    # 新闻数据存储
    # ========================================

    @_db_operation("[存储] 保存新闻数据失败", default=lambda: (False, 0, 0, 0, 0))
    def _save_news_data_impl(self, data: NewsData, log_prefix: str = "[存储]") -> tuple[bool, int, int, int, int]:
        """
        保存新闻数据到 SQLite（核心实现）

        Args:
            data: 新闻数据
            log_prefix: 日志前缀

        Returns:
            (success, new_count, updated_count, title_changed_count, off_list_count)
        """
        conn = self._get_connection(data.date)
        cursor = conn.cursor()

        now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")
        insert_news_sql = """
            INSERT INTO news_items
            (title, platform_id, rank, url, mobile_url,
             first_crawl_time, last_crawl_time, crawl_count,
             created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """
        insert_rank_sql = """
            INSERT INTO rank_history
            (news_item_id, rank, crawl_time, created_at)
            VALUES (?, ?, ?, ?)
        """

        cursor.executemany("""
            INSERT INTO platforms (id, name, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                updated_at = excluded.updated_at
        """, [(source_id, source_name, now_str) for source_id, source_name in data.id_to_name.items()])

        new_count = 0
        updated_count = 0
        title_changed_count = 0
        success_sources = []

        for source_id, news_list in data.items.items():
            success_sources.append(source_id)
            normalized = [(item, normalize_url(item.url, source_id) if item.url else "") for item in news_list]

            existing_by_url: Dict[str, tuple] = {}
            url_keys = list({url for _, url in normalized if url})
            for row_id, row_title, row_url in execute_chunked_in(
                cursor,
                "SELECT id, title, url FROM news_items WHERE url IN ({placeholders}) AND platform_id = ?",
                url_keys,
                extra_params=(source_id,),
            ):
                existing_by_url[row_url] = (row_id, row_title)

            title_change_rows: List[tuple] = []
            rank_rows: List[tuple] = []
            update_rows: List[tuple] = []
            pending_new: Dict[str, NewsItem] = {}
            deferred: List[tuple] = []

            def queue_update(item: NewsItem, url: str) -> None:
                nonlocal updated_count, title_changed_count
                existing_id, existing_title = existing_by_url[url]
                update_title = item.title
                if _is_url_like_title(update_title) and existing_title and not _is_url_like_title(existing_title):
                    update_title = existing_title
                if existing_title != update_title:
                    title_change_rows.append((existing_id, existing_title, update_title, now_str))
                    title_changed_count += 1
                    existing_by_url[url] = (existing_id, update_title)
                rank_rows.append((existing_id, item.rank, data.crawl_time, now_str))
                update_rows.append((update_title, item.rank, item.mobile_url,
                                    data.crawl_time, now_str, existing_id))
                updated_count += 1

            for item, normalized_url in normalized:
                if not normalized_url:
                    cursor.execute(insert_news_sql, (
                        item.title, source_id, item.rank, "", item.mobile_url,
                        data.crawl_time, data.crawl_time, now_str, now_str,
                    ))
                    rank_rows.append((cursor.lastrowid, item.rank, data.crawl_time, now_str))
                    new_count += 1
                elif normalized_url in existing_by_url:
                    queue_update(item, normalized_url)
                elif normalized_url in pending_new:
                    deferred.append((item, normalized_url))
                else:
                    pending_new[normalized_url] = item

            if pending_new:
                cursor.executemany(insert_news_sql, [
                    (item.title, source_id, item.rank, url, item.mobile_url,
                     data.crawl_time, data.crawl_time, now_str, now_str)
                    for url, item in pending_new.items()
                ])
                new_count += len(pending_new)
                for row_id, row_title, row_url in execute_chunked_in(
                    cursor,
                    "SELECT id, title, url FROM news_items WHERE url IN ({placeholders}) AND platform_id = ?",
                    list(pending_new.keys()),
                    extra_params=(source_id,),
                ):
                    existing_by_url[row_url] = (row_id, row_title)
                    rank_rows.append((row_id, pending_new[row_url].rank, data.crawl_time, now_str))

            for item, normalized_url in deferred:
                queue_update(item, normalized_url)

            cursor.executemany("""
                INSERT INTO title_changes
                (news_item_id, old_title, new_title, changed_at)
                VALUES (?, ?, ?, ?)
            """, title_change_rows)
            cursor.executemany(insert_rank_sql, rank_rows)
            cursor.executemany("""
                UPDATE news_items SET
                    title = ?, rank = ?, mobile_url = ?,
                    last_crawl_time = ?, crawl_count = crawl_count + 1,
                    updated_at = ?
                WHERE id = ?
            """, update_rows)

        total_items = new_count + updated_count
        off_list_count = 0
        cursor.execute("""
            SELECT crawl_time FROM crawl_records
            WHERE crawl_time < ? ORDER BY crawl_time DESC LIMIT 1
        """, (data.crawl_time,))
        prev_record = cursor.fetchone()
        if prev_record:
            prev_crawl_time = prev_record[0]
            for source_id in success_sources:
                current_urls = {
                    normalize_url(item.url, source_id)
                    for item in data.items.get(source_id, []) if item.url
                }
                cursor.execute("""
                    SELECT id, url FROM news_items
                    WHERE platform_id = ? AND last_crawl_time = ? AND url != ''
                """, (source_id, prev_crawl_time))
                off_list_rows = [
                    (news_id, 0, data.crawl_time, now_str)
                    for news_id, url in cursor.fetchall() if url not in current_urls
                ]
                cursor.executemany(insert_rank_sql, off_list_rows)
                off_list_count += len(off_list_rows)

        cursor.execute("""
            INSERT OR REPLACE INTO crawl_records
            (crawl_time, total_items, created_at) VALUES (?, ?, ?)
        """, (data.crawl_time, total_items, now_str))
        cursor.execute("SELECT id FROM crawl_records WHERE crawl_time = ?", (data.crawl_time,))
        record_row = cursor.fetchone()
        if record_row:
            crawl_record_id = record_row[0]
            cursor.executemany("""
                INSERT OR IGNORE INTO platforms (id, name, updated_at)
                VALUES (?, ?, ?)
            """, [(failed_id, failed_id, now_str) for failed_id in data.failed_ids])
            cursor.executemany("""
                INSERT OR REPLACE INTO crawl_source_status
                (crawl_record_id, platform_id, status) VALUES (?, ?, ?)
            """, [(crawl_record_id, source_id, "success") for source_id in success_sources]
               + [(crawl_record_id, failed_id, "failed") for failed_id in data.failed_ids])

        conn.commit()
        return True, new_count, updated_count, title_changed_count, off_list_count

    @_db_operation("[存储] 读取数据失败")
    def _get_today_all_data_impl(self, date: Optional[str] = None) -> Optional[NewsData]:
        """
        获取指定日期的所有新闻数据（合并后）

        Args:
            date: 日期字符串，默认为今天

        Returns:
            合并后的新闻数据
        """
        conn = self._get_connection(date)
        cursor = conn.cursor()

        # 获取所有新闻数据（包含 id 用于查询排名历史）
        cursor.execute("""
            SELECT n.id, n.title, n.platform_id, p.name as platform_name,
                   n.rank, n.url, n.mobile_url,
                   n.first_crawl_time, n.last_crawl_time, n.crawl_count
            FROM news_items n
            LEFT JOIN platforms p ON n.platform_id = p.id
            ORDER BY n.platform_id, n.last_crawl_time
        """)

        rows = cursor.fetchall()
        if not rows:
            return None

        # 收集所有 news_item_id
        news_ids = [row[0] for row in rows]

        # 批量查询排名历史（同时获取时间和排名）
        # 过滤逻辑：只保留 last_crawl_time 之前的脱榜记录（rank=0）
        # 这样可以避免显示新闻永久脱榜后的无意义记录
        rank_history_map: Dict[int, List[int]] = {}
        rank_timeline_map: Dict[int, List[Dict[str, Any]]] = {}
        if news_ids:
            rh_rows = execute_chunked_in(cursor, """
                SELECT rh.news_item_id, rh.rank, rh.crawl_time
                FROM rank_history rh
                JOIN news_items ni ON rh.news_item_id = ni.id
                WHERE rh.news_item_id IN ({placeholders})
                  AND NOT (rh.rank = 0 AND rh.crawl_time > ni.last_crawl_time)
                ORDER BY rh.news_item_id, rh.crawl_time
            """, news_ids)
            for rh_row in rh_rows:
                news_id, rank, crawl_time = rh_row[0], rh_row[1], rh_row[2]

                if not crawl_time:
                    continue

                # 构建 ranks 列表（去重，排除脱榜记录 rank=0）
                if news_id not in rank_history_map:
                    rank_history_map[news_id] = []
                if rank != 0 and rank not in rank_history_map[news_id]:
                    rank_history_map[news_id].append(rank)

                # 构建 rank_timeline 列表（完整时间线，包含脱榜）
                if news_id not in rank_timeline_map:
                    rank_timeline_map[news_id] = []
                # 提取时间部分（HH:MM）
                try:
                    time_part = crawl_time.split()[1][:5] if ' ' in crawl_time else crawl_time[:5]
                except (IndexError, AttributeError):
                    time_part = "??:??"
                rank_timeline_map[news_id].append({
                    "time": time_part,
                    "rank": rank if rank != 0 else None  # 0 转为 None 表示脱榜
                })

        # 按 platform_id 分组
        items: Dict[str, List[NewsItem]] = {}
        id_to_name: Dict[str, str] = {}
        crawl_date = self._format_date_folder(date)

        for row in rows:
            news_id = row[0]
            platform_id = row[2]
            title = row[1]
            platform_name = row[3] or platform_id

            id_to_name[platform_id] = platform_name

            if platform_id not in items:
                items[platform_id] = []

            # 获取排名历史，如果没有则使用当前排名
            ranks = rank_history_map.get(news_id, [row[4]])
            rank_timeline = rank_timeline_map.get(news_id, [])

            items[platform_id].append(NewsItem(
                title=title,
                source_id=platform_id,
                source_name=platform_name,
                rank=row[4],
                url=row[5] or "",
                mobile_url=row[6] or "",
                crawl_time=row[8],  # last_crawl_time
                ranks=ranks,
                first_time=row[7],  # first_crawl_time
                last_time=row[8],   # last_crawl_time
                count=row[9],       # crawl_count
                rank_timeline=rank_timeline,
            ))

        final_items = items

        # 获取失败的来源
        cursor.execute("""
            SELECT DISTINCT css.platform_id
            FROM crawl_source_status css
            JOIN crawl_records cr ON css.crawl_record_id = cr.id
            WHERE css.status = 'failed'
        """)
        failed_ids = [row[0] for row in cursor.fetchall()]

        # 获取最新的抓取时间
        cursor.execute("""
            SELECT crawl_time FROM crawl_records
            ORDER BY crawl_time DESC
            LIMIT 1
        """)

        time_row = cursor.fetchone()
        crawl_time = time_row[0] if time_row else self._format_time_filename()

        return NewsData(
            date=crawl_date,
            crawl_time=crawl_time,
            items=final_items,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
        )

    @_db_operation("[存储] 获取最新数据失败")
    def _get_latest_crawl_data_impl(self, date: Optional[str] = None) -> Optional[NewsData]:
        """
        获取最新一次抓取的数据

        Args:
            date: 日期字符串，默认为今天

        Returns:
            最新抓取的新闻数据
        """
        conn = self._get_connection(date)
        cursor = conn.cursor()

        # 获取最新的抓取时间
        cursor.execute("""
            SELECT crawl_time FROM crawl_records
            ORDER BY crawl_time DESC
            LIMIT 1
        """)

        time_row = cursor.fetchone()
        if not time_row:
            return None

        latest_time = time_row[0]

        # 获取该时间的新闻数据（包含 id 用于查询排名历史）
        cursor.execute("""
            SELECT n.id, n.title, n.platform_id, p.name as platform_name,
                   n.rank, n.url, n.mobile_url,
                   n.first_crawl_time, n.last_crawl_time, n.crawl_count
            FROM news_items n
            LEFT JOIN platforms p ON n.platform_id = p.id
            WHERE n.last_crawl_time = ?
        """, (latest_time,))

        rows = cursor.fetchall()
        if not rows:
            return None

        # 收集所有 news_item_id
        news_ids = [row[0] for row in rows]

        # 批量查询排名历史（同时获取时间和排名）
        # 过滤逻辑：只保留 last_crawl_time 之前的脱榜记录（rank=0）
        # 这样可以避免显示新闻永久脱榜后的无意义记录
        rank_history_map: Dict[int, List[int]] = {}
        rank_timeline_map: Dict[int, List[Dict[str, Any]]] = {}
        if news_ids:
            rh_rows = execute_chunked_in(cursor, """
                SELECT rh.news_item_id, rh.rank, rh.crawl_time
                FROM rank_history rh
                JOIN news_items ni ON rh.news_item_id = ni.id
                WHERE rh.news_item_id IN ({placeholders})
                  AND NOT (rh.rank = 0 AND rh.crawl_time > ni.last_crawl_time)
                ORDER BY rh.news_item_id, rh.crawl_time
            """, news_ids)
            for rh_row in rh_rows:
                news_id, rank, crawl_time = rh_row[0], rh_row[1], rh_row[2]

                if not crawl_time:
                    continue

                # 构建 ranks 列表（去重，排除脱榜记录 rank=0）
                if news_id not in rank_history_map:
                    rank_history_map[news_id] = []
                if rank != 0 and rank not in rank_history_map[news_id]:
                    rank_history_map[news_id].append(rank)

                # 构建 rank_timeline 列表（完整时间线，包含脱榜）
                if news_id not in rank_timeline_map:
                    rank_timeline_map[news_id] = []
                # 提取时间部分（HH:MM）
                try:
                    time_part = crawl_time.split()[1][:5] if ' ' in crawl_time else crawl_time[:5]
                except (IndexError, AttributeError):
                    time_part = "??:??"
                rank_timeline_map[news_id].append({
                    "time": time_part,
                    "rank": rank if rank != 0 else None  # 0 转为 None 表示脱榜
                })

        items: Dict[str, List[NewsItem]] = {}
        id_to_name: Dict[str, str] = {}
        crawl_date = self._format_date_folder(date)

        for row in rows:
            news_id = row[0]
            platform_id = row[2]
            platform_name = row[3] or platform_id
            id_to_name[platform_id] = platform_name

            if platform_id not in items:
                items[platform_id] = []

            # 获取排名历史，如果没有则使用当前排名
            ranks = rank_history_map.get(news_id, [row[4]])
            rank_timeline = rank_timeline_map.get(news_id, [])

            items[platform_id].append(NewsItem(
                title=row[1],
                source_id=platform_id,
                source_name=platform_name,
                rank=row[4],
                url=row[5] or "",
                mobile_url=row[6] or "",
                crawl_time=row[8],  # last_crawl_time
                ranks=ranks,
                first_time=row[7],  # first_crawl_time
                last_time=row[8],   # last_crawl_time
                count=row[9],       # crawl_count
                rank_timeline=rank_timeline,
            ))

        # 获取失败的来源（针对最新一次抓取）
        cursor.execute("""
            SELECT css.platform_id
            FROM crawl_source_status css
            JOIN crawl_records cr ON css.crawl_record_id = cr.id
            WHERE cr.crawl_time = ? AND css.status = 'failed'
        """, (latest_time,))

        failed_ids = [row[0] for row in cursor.fetchall()]

        return NewsData(
            date=crawl_date,
            crawl_time=latest_time,
            items=items,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
        )

    @_db_operation("[存储] 检测新标题失败", default=dict)
    def _detect_new_titles_impl(self, current_data: NewsData) -> Dict[str, Dict]:
        """
        检测新增的标题

        该方法比较当前抓取数据与历史数据，找出新增的标题。
        关键逻辑：只有在历史批次中从未出现过的标题才算新增。

        Args:
            current_data: 当前抓取的数据

        Returns:
            新增的标题数据 {source_id: {title: NewsItem}}
        """
        # 获取历史数据
        historical_data = self._get_today_all_data_impl(current_data.date)

        if not historical_data:
            # 没有历史数据，所有都是新的
            new_titles = {}
            for source_id, news_list in current_data.items.items():
                new_titles[source_id] = {item.title: item for item in news_list}
            return new_titles

        # 获取当前批次时间
        current_time = current_data.crawl_time

        # 收集历史标题（first_time < current_time 的标题）
        # 这样可以正确处理同一标题因 URL 变化而产生多条记录的情况
        historical_titles: Dict[str, set] = {}
        for source_id, news_list in historical_data.items.items():
            historical_titles[source_id] = set()
            for item in news_list:
                first_time = item.first_time or item.crawl_time
                if first_time < current_time:
                    historical_titles[source_id].add(item.title)

        # 检查是否有历史数据
        has_historical_data = any(len(titles) > 0 for titles in historical_titles.values())
        if not has_historical_data:
            # 第一次抓取，没有"新增"概念
            return {}

        # 检测新增
        new_titles = {}
        for source_id, news_list in current_data.items.items():
            hist_set = historical_titles.get(source_id, set())
            for item in news_list:
                if item.title not in hist_set:
                    if source_id not in new_titles:
                        new_titles[source_id] = {}
                    new_titles[source_id][item.title] = item

        return new_titles

    @_db_operation("[存储] 检查首次抓取失败", critical=True)
    def _is_first_crawl_today_impl(self, date: Optional[str] = None) -> bool:
        """
        检查是否是当天第一次抓取

        Args:
            date: 日期字符串，默认为今天

        Returns:
            是否是第一次抓取
        """
        conn = self._get_connection(date)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) as count FROM crawl_records
        """)

        row = cursor.fetchone()
        count = row[0] if row else 0

        # 如果只有一条或没有记录，视为第一次抓取
        return count <= 1

    @_db_operation("[存储] 获取抓取时间列表失败", default=list)
    def _get_crawl_times_impl(self, date: Optional[str] = None) -> List[str]:
        """
        获取指定日期的所有抓取时间列表

        Args:
            date: 日期字符串，默认为今天

        Returns:
            抓取时间列表（按时间排序）
        """
        conn = self._get_connection(date)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT crawl_time FROM crawl_records
            ORDER BY crawl_time
        """)

        rows = cursor.fetchall()
        return [row[0] for row in rows]

    # ========================================
    # 时间段执行记录（调度系统）
    # ========================================

    @_db_operation("[存储] 检查时间段执行记录失败", critical=True)
    def _has_period_executed_impl(self, date_str: str, period_key: str, action: str) -> bool:
        """
        检查指定时间段的某个 action 今天是否已执行

        Args:
            date_str: 日期字符串 YYYY-MM-DD
            period_key: 时间段 key
            action: 动作类型 (analyze / push)

        Returns:
            是否已执行
        """
        conn = self._get_connection(date_str)
        cursor = conn.cursor()

        # 先检查表是否存在
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='period_executions'
        """)
        if not cursor.fetchone():
            return False

        cursor.execute("""
            SELECT 1 FROM period_executions
            WHERE execution_date = ? AND period_key = ? AND action = ?
        """, (date_str, period_key, action))

        return cursor.fetchone() is not None

    @_db_operation("[存储] 记录时间段执行失败", default=False)
    def _record_period_execution_impl(self, date_str: str, period_key: str, action: str) -> bool:
        """
        记录时间段的 action 执行

        Args:
            date_str: 日期字符串 YYYY-MM-DD
            period_key: 时间段 key
            action: 动作类型 (analyze / push)

        Returns:
            是否记录成功
        """
        conn = self._get_connection(date_str)
        cursor = conn.cursor()

        # 确保表存在
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS period_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_date TEXT NOT NULL,
                period_key TEXT NOT NULL,
                action TEXT NOT NULL,
                executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(execution_date, period_key, action)
            )
        """)

        now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            INSERT OR IGNORE INTO period_executions (execution_date, period_key, action, executed_at)
            VALUES (?, ?, ?, ?)
        """, (date_str, period_key, action, now_str))

        conn.commit()
        return True

    # ========================================
    # RSS 数据存储
    # ========================================

    @_db_operation("[存储] 保存 RSS 数据失败", default=lambda: (False, 0, 0))
    def _save_rss_data_impl(self, data: RSSData, log_prefix: str = "[存储]") -> tuple[bool, int, int]:
        """
        保存 RSS 数据到 SQLite（以 URL 为唯一标识）

        Args:
            data: RSS 数据
            log_prefix: 日志前缀

        Returns:
            (success, new_count, updated_count)
        """
        conn = self._get_connection(data.date, db_type="rss")
        cursor = conn.cursor()
        now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

        cursor.executemany("""
            INSERT INTO rss_feeds (id, name, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name, updated_at = excluded.updated_at
        """, [(feed_id, feed_name, now_str) for feed_id, feed_name in data.id_to_name.items()])

        new_count = 0
        updated_count = 0
        for feed_id, rss_list in data.items.items():
            items_with_guid = [(item, getattr(item, "guid", "") or "") for item in rss_list]
            existing_by_guid: Dict[str, tuple] = {}
            existing_by_url: Dict[str, tuple] = {}
            guid_keys = list({guid for _, guid in items_with_guid if guid})
            url_keys = list({item.url for item, _ in items_with_guid if item.url})

            for row_id, row_title, row_guid in execute_chunked_in(
                cursor,
                "SELECT id, title, guid FROM rss_items WHERE guid IN ({placeholders}) AND feed_id = ?",
                guid_keys,
                extra_params=(feed_id,),
            ):
                existing_by_guid[row_guid] = (row_id, row_title)
            for row_id, row_title, row_url in execute_chunked_in(
                cursor,
                "SELECT id, title, url FROM rss_items WHERE url IN ({placeholders}) AND feed_id = ?",
                url_keys,
                extra_params=(feed_id,),
            ):
                existing_by_url[row_url] = (row_id, row_title)

            update_rows: List[tuple] = []
            insert_rows: List[tuple] = []
            for item, item_guid in items_with_guid:
                existing = existing_by_guid.get(item_guid) if item_guid else None
                if not existing and item.url:
                    existing = existing_by_url.get(item.url)
                if existing:
                    existing_id, existing_title = existing
                    update_title = item.title
                    if _is_url_like_title(update_title) and existing_title and not _is_url_like_title(existing_title):
                        update_title = existing_title
                    update_rows.append((
                        update_title, item.url, item.url, item_guid, item_guid,
                        item.published_at, item.summary, item.author,
                        data.crawl_time, now_str, existing_id,
                    ))
                elif item.url or item_guid:
                    insert_rows.append((
                        item.title, feed_id, item.url, item_guid,
                        item.published_at, item.summary, item.author,
                        data.crawl_time, data.crawl_time, now_str, now_str,
                    ))

            cursor.executemany("""
                UPDATE rss_items SET
                    title = ?,
                    url = CASE WHEN ? != '' THEN ? ELSE url END,
                    guid = CASE WHEN ? != '' THEN ? ELSE guid END,
                    published_at = ?, summary = ?, author = ?,
                    last_crawl_time = ?, crawl_count = crawl_count + 1,
                    updated_at = ?
                WHERE id = ?
            """, update_rows)
            updated_count += len(update_rows)
            cursor.executemany("""
                INSERT OR IGNORE INTO rss_items
                (title, feed_id, url, guid, published_at, summary, author,
                 first_crawl_time, last_crawl_time, crawl_count, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """, insert_rows)
            new_count += max(cursor.rowcount, 0)

        total_items = new_count + updated_count
        cursor.execute("""
            INSERT OR REPLACE INTO rss_crawl_records
            (crawl_time, total_items, created_at) VALUES (?, ?, ?)
        """, (data.crawl_time, total_items, now_str))
        cursor.execute("SELECT id FROM rss_crawl_records WHERE crawl_time = ?", (data.crawl_time,))
        record_row = cursor.fetchone()
        if record_row:
            crawl_record_id = record_row[0]
            cursor.executemany("""
                INSERT OR IGNORE INTO rss_feeds (id, name, updated_at)
                VALUES (?, ?, ?)
            """, [(failed_id, failed_id, now_str) for failed_id in data.failed_ids])
            cursor.executemany("""
                INSERT OR REPLACE INTO rss_crawl_status
                (crawl_record_id, feed_id, status) VALUES (?, ?, ?)
            """, [(crawl_record_id, feed_id, "success") for feed_id in data.items.keys()]
               + [(crawl_record_id, failed_id, "failed") for failed_id in data.failed_ids])

        conn.commit()
        return True, new_count, updated_count

    @_db_operation("[存储] 读取 RSS 数据失败")
    def _get_rss_data_impl(self, date: Optional[str] = None) -> Optional[RSSData]:
        """
        获取指定日期的所有 RSS 数据

        Args:
            date: 日期字符串（YYYY-MM-DD），默认为今天

        Returns:
            RSSData 对象，如果没有数据返回 None
        """
        conn = self._get_connection(date, db_type="rss")
        cursor = conn.cursor()

        # 获取所有 RSS 数据
        cursor.execute("""
            SELECT i.id, i.title, i.feed_id, f.name as feed_name,
                   i.url, i.published_at, i.summary, i.author,
                   i.first_crawl_time, i.last_crawl_time, i.crawl_count
            FROM rss_items i
            LEFT JOIN rss_feeds f ON i.feed_id = f.id
            ORDER BY i.published_at DESC
        """)

        rows = cursor.fetchall()
        if not rows:
            return None

        items: Dict[str, List[RSSItem]] = {}
        id_to_name: Dict[str, str] = {}
        crawl_date = self._format_date_folder(date)

        for row in rows:
            feed_id = row[2]
            feed_name = row[3] or feed_id

            id_to_name[feed_id] = feed_name

            if feed_id not in items:
                items[feed_id] = []

            items[feed_id].append(RSSItem(
                title=row[1],
                feed_id=feed_id,
                feed_name=feed_name,
                url=row[4] or "",
                published_at=row[5] or "",
                summary=row[6] or "",
                author=row[7] or "",
                crawl_time=row[9],
                first_time=row[8],
                last_time=row[9],
                count=row[10],
            ))

        # 获取最新的抓取时间
        cursor.execute("""
            SELECT crawl_time FROM rss_crawl_records
            ORDER BY crawl_time DESC
            LIMIT 1
        """)
        time_row = cursor.fetchone()
        crawl_time = time_row[0] if time_row else self._format_time_filename()

        # 获取失败的源
        cursor.execute("""
            SELECT DISTINCT cs.feed_id
            FROM rss_crawl_status cs
            JOIN rss_crawl_records cr ON cs.crawl_record_id = cr.id
            WHERE cs.status = 'failed'
        """)
        failed_ids = [row[0] for row in cursor.fetchall()]

        return RSSData(
            date=crawl_date,
            crawl_time=crawl_time,
            items=items,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
        )

    @_db_operation("[存储] 检测新 RSS 条目失败", default=dict)
    def _detect_new_rss_items_impl(self, current_data: RSSData) -> Dict[str, List[RSSItem]]:
        """
        检测新增的 RSS 条目（增量模式）

        该方法比较当前抓取数据与历史数据，找出新增的 RSS 条目。
        关键逻辑：只有在历史批次中从未出现过的 URL 才算新增。

        Args:
            current_data: 当前抓取的 RSS 数据

        Returns:
            新增的 RSS 条目 {feed_id: [RSSItem, ...]}
        """
        # 获取历史数据
        historical_data = self._get_rss_data_impl(current_data.date)

        if not historical_data:
            # 没有历史数据，所有都是新的
            return current_data.items.copy()

        # 获取当前批次时间
        current_time = current_data.crawl_time

        # 收集历史 URL（first_time < current_time 的条目）
        historical_urls: Dict[str, set] = {}
        for feed_id, rss_list in historical_data.items.items():
            historical_urls[feed_id] = set()
            for item in rss_list:
                first_time = item.first_time or item.crawl_time
                if first_time < current_time:
                    if item.url:
                        historical_urls[feed_id].add(item.url)

        # 检查是否有早于当前批次的历史数据
        has_historical_data = any(len(urls) > 0 for urls in historical_urls.values())
        if not has_historical_data:
            # 当天第一次抓取，所有条目都是新增
            return current_data.items.copy()

        # 检测新增
        new_items: Dict[str, List[RSSItem]] = {}
        for feed_id, rss_list in current_data.items.items():
            hist_set = historical_urls.get(feed_id, set())
            for item in rss_list:
                # 通过 URL 判断是否新增
                if item.url and item.url not in hist_set:
                    if feed_id not in new_items:
                        new_items[feed_id] = []
                    new_items[feed_id].append(item)

        return new_items

    @_db_operation("[存储] 获取最新 RSS 数据失败")
    def _get_latest_rss_data_impl(self, date: Optional[str] = None) -> Optional[RSSData]:
        """
        获取最新一次抓取的 RSS 数据（当前榜单模式）

        Args:
            date: 日期字符串（YYYY-MM-DD），默认为今天

        Returns:
            最新抓取的 RSS 数据，如果没有数据返回 None
        """
        conn = self._get_connection(date, db_type="rss")
        cursor = conn.cursor()

        # 获取最新的抓取时间
        cursor.execute("""
            SELECT crawl_time FROM rss_crawl_records
            ORDER BY crawl_time DESC
            LIMIT 1
        """)

        time_row = cursor.fetchone()
        if not time_row:
            return None

        latest_time = time_row[0]

        # 获取该时间的 RSS 数据
        cursor.execute("""
            SELECT i.id, i.title, i.feed_id, f.name as feed_name,
                   i.url, i.published_at, i.summary, i.author,
                   i.first_crawl_time, i.last_crawl_time, i.crawl_count
            FROM rss_items i
            LEFT JOIN rss_feeds f ON i.feed_id = f.id
            WHERE i.last_crawl_time = ?
            ORDER BY i.published_at DESC
        """, (latest_time,))

        rows = cursor.fetchall()
        if not rows:
            return None

        items: Dict[str, List[RSSItem]] = {}
        id_to_name: Dict[str, str] = {}
        crawl_date = self._format_date_folder(date)

        for row in rows:
            feed_id = row[2]
            feed_name = row[3] or feed_id

            id_to_name[feed_id] = feed_name

            if feed_id not in items:
                items[feed_id] = []

            items[feed_id].append(RSSItem(
                title=row[1],
                feed_id=feed_id,
                feed_name=feed_name,
                url=row[4] or "",
                published_at=row[5] or "",
                summary=row[6] or "",
                author=row[7] or "",
                crawl_time=row[9],
                first_time=row[8],
                last_time=row[9],
                count=row[10],
            ))

        # 获取失败的源（针对最新一次抓取）
        cursor.execute("""
            SELECT cs.feed_id
            FROM rss_crawl_status cs
            JOIN rss_crawl_records cr ON cs.crawl_record_id = cr.id
            WHERE cr.crawl_time = ? AND cs.status = 'failed'
        """, (latest_time,))

        failed_ids = [row[0] for row in cursor.fetchall()]

        return RSSData(
            date=crawl_date,
            crawl_time=latest_time,
            items=items,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
        )

    # ========================================
    # AI 智能筛选 - 标签管理
    # ========================================

    @_db_operation("[AI筛选] 获取标签失败", default=list)
    def _get_active_tags_impl(self, date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> List[Dict[str, Any]]:
        """获取指定兴趣文件的 active 标签列表"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, tag, description, version, prompt_hash, priority
            FROM ai_filter_tags
            WHERE status = 'active' AND interests_file = ?
            ORDER BY priority ASC, id ASC
        """, (interests_file,))

        return [
            {
                "id": row[0], "tag": row[1], "description": row[2],
                "version": row[3], "prompt_hash": row[4], "priority": row[5],
            }
            for row in cursor.fetchall()
        ]

    @_db_operation("[AI筛选] 获取 prompt_hash 失败")
    def _get_latest_prompt_hash_impl(self, date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> Optional[str]:
        """获取指定兴趣文件最新版本标签的 prompt_hash"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT prompt_hash FROM ai_filter_tags
            WHERE status = 'active' AND interests_file = ?
            ORDER BY version DESC
            LIMIT 1
        """, (interests_file,))
        row = cursor.fetchone()
        return row[0] if row else None

    @_db_operation("[AI筛选] 获取版本号失败", default=0)
    def _get_latest_tag_version_impl(self, date: Optional[str] = None) -> int:
        """获取最新版本号"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT MAX(version) FROM ai_filter_tags
        """)
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else 0

    @_db_operation("[AI筛选] 废弃标签失败", default=0)
    def _deprecate_all_tags_impl(self, date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> int:
        """将指定兴趣文件的 active 标签和关联的分类结果标记为 deprecated"""
        conn = self._get_connection(date)
        cursor = conn.cursor()
        now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

        # 获取该兴趣文件的 active 标签 id
        cursor.execute(
            "SELECT id FROM ai_filter_tags WHERE status = 'active' AND interests_file = ?",
            (interests_file,)
        )
        tag_ids = [row[0] for row in cursor.fetchall()]

        if not tag_ids:
            return 0

        # 废弃标签
        placeholders = ",".join("?" * len(tag_ids))
        cursor.execute(f"""
            UPDATE ai_filter_tags
            SET status = 'deprecated', deprecated_at = ?
            WHERE id IN ({placeholders})
        """, [now_str] + tag_ids)
        tag_count = cursor.rowcount

        # 废弃关联的分类结果
        placeholders = ",".join("?" * len(tag_ids))
        cursor.execute(f"""
            UPDATE ai_filter_results
            SET status = 'deprecated', deprecated_at = ?
            WHERE tag_id IN ({placeholders}) AND status = 'active'
        """, [now_str] + tag_ids)

        conn.commit()
        print(f"[AI筛选] 已废弃 {tag_count} 个标签及关联分类结果")
        return tag_count

    @_db_operation("[AI筛选] 保存标签失败", default=0)
    def _save_tags_impl(
        self, date: Optional[str], tags: List[Dict], version: int, prompt_hash: str,
        interests_file: str = "ai_interests.txt"
    ) -> int:
        """保存新提取的标签"""
        conn = self._get_connection(date)
        cursor = conn.cursor()
        now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

        rows = []
        for idx, tag_data in enumerate(tags, start=1):
            priority = tag_data.get("priority", idx)
            try:
                priority = int(priority)
            except (TypeError, ValueError):
                priority = idx
            rows.append((
                tag_data["tag"], tag_data.get("description", ""), priority,
                version, prompt_hash, interests_file, now_str,
            ))

        cursor.executemany("""
                INSERT INTO ai_filter_tags
                (tag, description, priority, version, prompt_hash, interests_file, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rows)

        conn.commit()
        return len(rows)

    @_db_operation("[AI筛选] 废弃指定标签失败", default=0)
    def _deprecate_specific_tags_impl(
        self, date: Optional[str], tag_ids: List[int]
    ) -> int:
        """废弃指定 ID 的标签及其关联分类结果（增量更新时使用）"""
        if not tag_ids:
            return 0
        conn = self._get_connection(date)
        cursor = conn.cursor()
        now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

        placeholders = ",".join("?" * len(tag_ids))

        cursor.execute(f"""
            UPDATE ai_filter_tags
            SET status = 'deprecated', deprecated_at = ?
            WHERE id IN ({placeholders})
        """, [now_str] + tag_ids)
        tag_count = cursor.rowcount

        cursor.execute(f"""
            UPDATE ai_filter_results
            SET status = 'deprecated', deprecated_at = ?
            WHERE tag_id IN ({placeholders}) AND status = 'active'
        """, [now_str] + tag_ids)

        conn.commit()
        return tag_count

    @_db_operation("[AI筛选] 更新标签 hash 失败", default=0)
    def _update_tags_hash_impl(
        self, date: Optional[str], interests_file: str, new_hash: str
    ) -> int:
        """更新指定兴趣文件所有 active 标签的 prompt_hash（增量更新时使用）"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE ai_filter_tags
            SET prompt_hash = ?
            WHERE interests_file = ? AND status = 'active'
        """, (new_hash, interests_file))
        count = cursor.rowcount

        conn.commit()
        return count

    # ========================================
    # AI 智能筛选 - 分类结果管理
    # ========================================

    @_db_operation("[AI筛选] 更新标签描述失败", default=0)
    def _update_tag_descriptions_impl(
        self, date: Optional[str], tag_updates: List[Dict],
        interests_file: str = "ai_interests.txt"
    ) -> int:
        """按 tag 名匹配，更新 active 标签的 description 字段"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        rows = [
            (t.get("description", ""), t["tag"], interests_file)
            for t in tag_updates if t.get("tag", "")
        ]
        cursor.executemany("""
            UPDATE ai_filter_tags
            SET description = ?
            WHERE tag = ? AND interests_file = ? AND status = 'active'
        """, rows)
        count = max(cursor.rowcount, 0) if rows else 0

        conn.commit()
        return count

    @_db_operation("[AI筛选] 更新标签优先级失败", default=0)
    def _update_tag_priorities_impl(
        self, date: Optional[str], tag_priorities: List[Dict],
        interests_file: str = "ai_interests.txt"
    ) -> int:
        """按 tag 名匹配，更新 active 标签的 priority 字段"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        rows = []
        for t in tag_priorities:
            tag_name = t.get("tag", "")
            if not tag_name:
                continue
            try:
                priority = int(t.get("priority"))
            except (TypeError, ValueError):
                continue
            rows.append((priority, tag_name, interests_file))

        cursor.executemany("""
            UPDATE ai_filter_tags
            SET priority = ?
            WHERE tag = ? AND interests_file = ? AND status = 'active'
        """, rows)
        count = max(cursor.rowcount, 0) if rows else 0

        conn.commit()
        return count

    # ========================================
    # AI 智能筛选 - 已分析新闻追踪
    # ========================================

    @_db_operation("[AI筛选] 保存已分析记录失败", default=0)
    def _save_analyzed_news_impl(
        self, date: Optional[str], news_ids: List[int], source_type: str,
        interests_file: str, prompt_hash: str, matched_ids: set
    ) -> int:
        """批量记录已分析的新闻（匹配与不匹配都记录）"""
        conn = self._get_connection(date)
        cursor = conn.cursor()
        now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

        rows = [
            (nid, source_type, interests_file, prompt_hash,
             1 if nid in matched_ids else 0, now_str)
            for nid in news_ids
        ]
        cursor.executemany("""
            INSERT OR REPLACE INTO ai_filter_analyzed_news
            (news_item_id, source_type, interests_file, prompt_hash, matched, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, rows)

        conn.commit()
        return len(rows)

    @_db_operation("[AI筛选] 获取已分析ID失败", critical=True)
    def _get_analyzed_news_ids_impl(
        self, date: Optional[str] = None, source_type: str = "hotlist",
        interests_file: str = "ai_interests.txt"
    ) -> set:
        """获取已分析过的新闻 ID 集合（用于去重）"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT news_item_id FROM ai_filter_analyzed_news
            WHERE source_type = ? AND interests_file = ?
        """, (source_type, interests_file))

        return {row[0] for row in cursor.fetchall()}

    @_db_operation("[AI筛选] 清除已分析记录失败", default=0)
    def _clear_analyzed_news_impl(
        self, date: Optional[str] = None, interests_file: str = "ai_interests.txt"
    ) -> int:
        """清除指定兴趣文件的所有已分析记录（全量重分类时使用）"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        cursor.execute("""
            DELETE FROM ai_filter_analyzed_news
            WHERE interests_file = ?
        """, (interests_file,))

        count = cursor.rowcount
        conn.commit()
        return count

    @_db_operation("[AI筛选] 清除不匹配记录失败", default=0)
    def _clear_unmatched_analyzed_news_impl(
        self, date: Optional[str] = None, interests_file: str = "ai_interests.txt"
    ) -> int:
        """清除不匹配的已分析记录，让这些新闻有机会被新标签重新分析"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        cursor.execute("""
            DELETE FROM ai_filter_analyzed_news
            WHERE interests_file = ? AND matched = 0
        """, (interests_file,))

        count = cursor.rowcount
        conn.commit()
        return count

    # ========================================
    # AI 智能筛选 - 分类结果管理（原有）
    # ========================================

    @_db_operation("[AI筛选] 保存分类结果失败", default=0)
    def _save_filter_results_impl(
        self, date: Optional[str], results: List[Dict]
    ) -> int:
        """批量保存分类结果"""
        conn = self._get_connection(date)
        cursor = conn.cursor()
        now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

        rows = [
            (r["news_item_id"], r.get("source_type", "hotlist"), r["tag_id"],
             r.get("relevance_score", 0.0), now_str)
            for r in results
        ]
        cursor.executemany("""
            INSERT OR IGNORE INTO ai_filter_results
            (news_item_id, source_type, tag_id, relevance_score, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, rows)
        count = max(cursor.rowcount, 0) if rows else 0

        conn.commit()
        return count

    @_db_operation("[AI筛选] 获取分类结果失败", default=list)
    def _get_active_filter_results_impl(self, date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> List[Dict[str, Any]]:
        """获取指定兴趣文件的 active 分类结果，JOIN news_items 获取新闻详情"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        # 热榜结果
        cursor.execute("""
            SELECT
                r.news_item_id, r.source_type, r.tag_id, r.relevance_score,
                t.tag, t.description as tag_description, t.priority,
                n.title, n.platform_id as source_id, p.name as source_name,
                n.url, n.mobile_url, n.rank,
                n.first_crawl_time, n.last_crawl_time, n.crawl_count
            FROM ai_filter_results r
            JOIN ai_filter_tags t ON r.tag_id = t.id
            JOIN news_items n ON r.news_item_id = n.id
            LEFT JOIN platforms p ON n.platform_id = p.id
            WHERE r.status = 'active' AND r.source_type = 'hotlist'
                AND t.status = 'active' AND t.interests_file = ?
            ORDER BY t.priority ASC, t.id ASC, r.relevance_score DESC
        """, (interests_file,))

        results = []
        hotlist_news_ids = []
        for row in cursor.fetchall():
            results.append({
                "news_item_id": row[0], "source_type": row[1],
                "tag_id": row[2], "relevance_score": row[3],
                "tag": row[4], "tag_description": row[5], "tag_priority": row[6],
                "title": row[7], "source_id": row[8],
                "source_name": row[9] or row[8],
                "url": row[10] or "", "mobile_url": row[11] or "",
                "rank": row[12],
                "first_time": row[13], "last_time": row[14],
                "count": row[15],
            })
            hotlist_news_ids.append(row[0])

        # 批量查排名历史（热榜）
        ranks_map: Dict[int, List[int]] = {}
        rank_timeline_map: Dict[int, List[Dict[str, Any]]] = {}
        if hotlist_news_ids:
            rh_rows = execute_chunked_in(cursor, """
                SELECT news_item_id, rank, crawl_time FROM rank_history
                WHERE news_item_id IN ({placeholders})
                ORDER BY news_item_id, crawl_time
            """, hotlist_news_ids)
            for rh_row in rh_rows:
                nid, rank, crawl_time = rh_row[0], rh_row[1], rh_row[2]

                if not crawl_time:
                    continue

                if nid not in ranks_map:
                    ranks_map[nid] = []
                if rank != 0 and rank not in ranks_map[nid]:
                    ranks_map[nid].append(rank)

                if nid not in rank_timeline_map:
                    rank_timeline_map[nid] = []
                try:
                    time_part = crawl_time.split()[1][:5] if ' ' in crawl_time else crawl_time[:5]
                except (IndexError, AttributeError):
                    time_part = "??:??"
                rank_timeline_map[nid].append({
                    "time": time_part,
                    "rank": rank if rank != 0 else None
                })

        for item in results:
            item["ranks"] = ranks_map.get(item["news_item_id"], [item["rank"]])
            item["rank_timeline"] = rank_timeline_map.get(item["news_item_id"], [])

        # RSS 结果（如果有 rss 库）
        try:
            rss_conn = self._get_connection(date, db_type="rss")
            rss_cursor = rss_conn.cursor()

            # 从 news 库获取 rss 类型的分类结果 ID
            cursor.execute("""
                SELECT r.news_item_id, r.tag_id, r.relevance_score,
                       t.tag, t.description, t.priority
                FROM ai_filter_results r
                JOIN ai_filter_tags t ON r.tag_id = t.id
                WHERE r.status = 'active' AND r.source_type = 'rss'
                    AND t.status = 'active' AND t.interests_file = ?
                ORDER BY t.priority ASC, t.id ASC, r.relevance_score DESC
            """, (interests_file,))

            rss_filter_rows = cursor.fetchall()
            if rss_filter_rows:
                rss_ids = [row[0] for row in rss_filter_rows]
                rss_rows = execute_chunked_in(rss_cursor, """
                    SELECT i.id, i.title, i.feed_id, f.name as feed_name,
                           i.url, i.published_at
                    FROM rss_items i
                    LEFT JOIN rss_feeds f ON i.feed_id = f.id
                    WHERE i.id IN ({placeholders})
                """, rss_ids)

                rss_info = {row[0]: row for row in rss_rows}

                for fr_row in rss_filter_rows:
                    rss_id = fr_row[0]
                    info = rss_info.get(rss_id)
                    if info:
                        results.append({
                            "news_item_id": rss_id,
                            "source_type": "rss",
                            "tag_id": fr_row[1],
                            "relevance_score": fr_row[2],
                            "tag": fr_row[3],
                            "tag_description": fr_row[4],
                            "tag_priority": fr_row[5],
                            "title": info[1],
                            "source_id": info[2],
                            "source_name": info[3] or info[2],
                            "url": info[4] or "",
                            "mobile_url": "",
                            "rank": 0,
                            "ranks": [],
                            "first_time": info[5] or "",
                            "last_time": info[5] or "",
                            "count": 1,
                        })
        except sqlite3.Error as e:
            logger.debug("[AI筛选] 跳过 RSS 分类结果: %s", e)

        return results

    @_db_operation("[AI筛选] 获取新闻列表失败", default=list)
    def _get_all_news_ids_impl(self, date: Optional[str] = None) -> List[Dict]:
        """获取当日所有新闻的 id 和标题（用于 AI 筛选分类）"""
        conn = self._get_connection(date)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT n.id, n.title, n.platform_id, p.name as platform_name
            FROM news_items n
            LEFT JOIN platforms p ON n.platform_id = p.id
            ORDER BY n.id
        """)

        return [
            {
                "id": row[0], "title": row[1],
                "source_id": row[2], "source_name": row[3] or row[2],
            }
            for row in cursor.fetchall()
        ]

    @_db_operation("[AI筛选] 获取 RSS 列表失败", default=list)
    def _get_all_rss_ids_impl(self, date: Optional[str] = None) -> List[Dict]:
        """获取当日所有 RSS 条目的 id 和标题（用于 AI 筛选分类）"""
        conn = self._get_connection(date, db_type="rss")
        cursor = conn.cursor()

        cursor.execute("""
            SELECT i.id, i.title, i.feed_id, f.name as feed_name, i.published_at
            FROM rss_items i
            LEFT JOIN rss_feeds f ON i.feed_id = f.id
            ORDER BY i.id
        """)

        return [
            {
                "id": row[0], "title": row[1],
                "source_id": row[2], "source_name": row[3] or row[2],
                "published_at": row[4] or "",
            }
            for row in cursor.fetchall()
        ]
