# coding=utf-8
"""
消息发送器模块

将报告数据发送到各种通知渠道：
- 飞书 (Feishu/Lark)
- 钉钉 (DingTalk)
- 企业微信 (WeCom/WeWork)
- Telegram
- 邮件 (Email)
- ntfy
- Bark
- Slack

每个发送函数都支持分批发送，并通过参数化配置实现与 CONFIG 的解耦。
"""

import logging
import smtplib
import time
import json
from datetime import datetime
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse

import requests
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .batch import add_batch_headers, get_max_batch_header_size
from .formatters import convert_markdown_to_mrkdwn, strip_markdown

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_WAIT_MIN = 1
RETRY_WAIT_MAX = 10


class TransientSendError(Exception):
    """可重试的发送错误（网络异常、429、5xx）"""


def _build_proxies(proxy_url: Optional[str]) -> Optional[Dict[str, str]]:
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


def _post_once(
    url: str,
    request_kwargs: Dict[str, Any],
    check_response: Callable[[requests.Response], Optional[str]],
) -> Optional[str]:
    """发送一次请求；成功返回 None，业务失败返回错误信息，瞬时错误抛出 TransientSendError"""
    try:
        response = requests.post(url, timeout=REQUEST_TIMEOUT, **request_kwargs)
    except requests.RequestException as e:
        raise TransientSendError(f"{type(e).__name__}: {e}") from e

    if response.status_code == 429 or response.status_code >= 500:
        raise TransientSendError(f"状态码：{response.status_code}")

    return check_response(response)


def _post_batches(
    url: str,
    batches: list,
    build_request: Callable[[str, int, int], Dict[str, Any]],
    check_response: Callable[[requests.Response], Optional[str]],
    *,
    log_prefix: str,
    report_type: str,
    batch_interval: float,
    reverse: bool = False,
    stop_on_failure: bool = True,
) -> bool:
    """
    统一的分批 HTTP 发送循环

    Args:
        url: 请求地址
        batches: 已添加批次头部的内容列表
        build_request: (batch_content, batch_num, total) -> requests.post 的额外参数（json/data/headers/proxies）
        check_response: 非瞬时错误的响应校验；成功返回 None，否则返回错误信息
        log_prefix: 日志前缀
        report_type: 报告类型
        batch_interval: 批次间间隔（秒）
        reverse: 是否倒序推送（ntfy/Bark 客户端最新消息在上）
        stop_on_failure: 单批失败时是否中止；否则继续并按成功数判断

    Returns:
        bool: stop_on_failure 时全部成功才为 True；否则至少一批成功即为 True
    """
    total = len(batches)
    print(f"{log_prefix}消息分为 {total} 批次发送 [{report_type}]")
    if reverse:
        print(f"{log_prefix}将按反向顺序推送（最后批次先推送），确保客户端显示顺序正确")

    order = list(range(total - 1, -1, -1)) if reverse else list(range(total))

    retrying = Retrying(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
        retry=retry_if_exception_type(TransientSendError),
        reraise=True,
    )

    success_count = 0
    for idx, batch_index in enumerate(order, 1):
        batch_num = batch_index + 1
        batch_content = batches[batch_index]
        content_size = len(batch_content.encode("utf-8"))
        print(
            f"发送{log_prefix}第 {batch_num}/{total} 批次，大小：{content_size} 字节 [{report_type}]"
        )

        try:
            request_kwargs = build_request(batch_content, batch_num, total)
            error = retrying(_post_once, url, request_kwargs, check_response)
        except TransientSendError as e:
            error = f"重试 {RETRY_ATTEMPTS} 次后放弃：{e}"
        except Exception:
            logger.exception(
                "%s第 %d/%d 批次发送异常 [%s]", log_prefix, batch_num, total, report_type
            )
            error = "未预期的异常"

        if error is None:
            success_count += 1
            print(f"{log_prefix}第 {batch_num}/{total} 批次发送成功 [{report_type}]")
        else:
            logger.warning(
                "%s第 %d/%d 批次发送失败 [%s]：%s", log_prefix, batch_num, total, report_type, error
            )
            if stop_on_failure:
                return False

        if idx < total:
            time.sleep(batch_interval)

    if success_count == total:
        print(f"{log_prefix}所有 {total} 批次发送完成 [{report_type}]")
        return True
    if success_count > 0:
        logger.warning(
            "%s部分发送成功：%d/%d 批次 [%s]", log_prefix, success_count, total, report_type
        )
        return True
    logger.warning("%s发送完全失败 [%s]", log_prefix, report_type)
    return False


def _check_json_field(
    field: str, expected: Any, error_fields: tuple
) -> Callable[[requests.Response], Optional[str]]:
    """构建 JSON 响应校验器：field == expected 视为成功，否则返回 error_fields 中的首个错误信息"""

    def check(response: requests.Response) -> Optional[str]:
        if response.status_code != 200:
            return f"状态码：{response.status_code}"
        try:
            result = response.json()
        except ValueError:
            logger.debug("%s", response.text)
            return "响应不是有效 JSON"
        if result.get(field) == expected:
            return None
        for name in error_fields:
            if result.get(name):
                return str(result[name])
        return "未知错误"

    return check


def _extract_ai_stats(ai_analysis) -> Optional[Dict]:
    """从 AI 分析结果中提取统计数据"""
    if not ai_analysis or not getattr(ai_analysis, "success", False):
        return None
    return {
        "total_news": getattr(ai_analysis, "total_news", 0),
        "analyzed_news": getattr(ai_analysis, "analyzed_news", 0),
        "max_news_limit": getattr(ai_analysis, "max_news_limit", 0),
        "hotlist_count": getattr(ai_analysis, "hotlist_count", 0),
        "rss_count": getattr(ai_analysis, "rss_count", 0),
        "hotlist_analyzed": getattr(ai_analysis, "hotlist_analyzed", 0),
        "rss_analyzed": getattr(ai_analysis, "rss_analyzed", 0),
        "standalone_analyzed": getattr(ai_analysis, "standalone_analyzed", 0),
        "ai_mode": getattr(ai_analysis, "ai_mode", ""),
        "include_rss": getattr(ai_analysis, "include_rss", True),
        "include_standalone": getattr(ai_analysis, "include_standalone", False),
    }


def _render_ai_analysis(ai_analysis: Any, channel: str) -> str:
    """渲染 AI 分析内容为指定渠道格式"""
    if not ai_analysis:
        return ""

    try:
        from trendradar.ai.formatter import get_ai_analysis_renderer
        renderer = get_ai_analysis_renderer(channel)
        return renderer(ai_analysis)
    except ImportError:
        return ""


# === SMTP 邮件配置 ===
SMTP_CONFIGS = {
    # Gmail（使用 STARTTLS）
    "gmail.com": {"server": "smtp.gmail.com", "port": 587, "encryption": "TLS"},
    # QQ邮箱（使用 SSL，更稳定）
    "qq.com": {"server": "smtp.qq.com", "port": 465, "encryption": "SSL"},
    # Outlook（使用 STARTTLS）
    "outlook.com": {"server": "smtp-mail.outlook.com", "port": 587, "encryption": "TLS"},
    "hotmail.com": {"server": "smtp-mail.outlook.com", "port": 587, "encryption": "TLS"},
    "live.com": {"server": "smtp-mail.outlook.com", "port": 587, "encryption": "TLS"},
    # 网易邮箱（使用 SSL，更稳定）
    "163.com": {"server": "smtp.163.com", "port": 465, "encryption": "SSL"},
    "126.com": {"server": "smtp.126.com", "port": 465, "encryption": "SSL"},
    # 新浪邮箱（使用 SSL）
    "sina.com": {"server": "smtp.sina.com", "port": 465, "encryption": "SSL"},
    # 搜狐邮箱（使用 SSL）
    "sohu.com": {"server": "smtp.sohu.com", "port": 465, "encryption": "SSL"},
    # 天翼邮箱（使用 SSL）
    "189.cn": {"server": "smtp.189.cn", "port": 465, "encryption": "SSL"},
    # 阿里云邮箱（使用 TLS）
    "aliyun.com": {"server": "smtp.aliyun.com", "port": 465, "encryption": "TLS"},
    # Yandex邮箱（使用 TLS）
    "yandex.com": {"server": "smtp.yandex.com", "port": 465, "encryption": "TLS"},
    # iCloud邮箱（使用 SSL）
    "icloud.com": {"server": "smtp.mail.me.com", "port": 587, "encryption": "SSL"},
}


def send_to_feishu(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 29000,
    batch_interval: float = 1.0,
    split_content_func: Callable = None,
    get_time_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到飞书（支持分批发送，支持热榜+RSS合并+独立展示区）

    Args:
        webhook_url: 飞书 Webhook URL
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        get_time_func: 获取当前时间的函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    headers = {"Content-Type": "application/json"}
    proxies = _build_proxies(proxy_url)

    # 日志前缀
    log_prefix = f"飞书{account_label}" if account_label else "飞书"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "feishu") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 预留批次头部空间，避免添加头部后超限
    header_reserve = get_max_batch_header_size("feishu")
    batches = split_content_func(
        report_data,
        "feishu",
        update_info,
        max_bytes=batch_size - header_reserve,
        mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "feishu", batch_size)

    # 根据 webhook 域名选择 payload 格式
    # www.feishu.cn 使用纯文本格式，其他域名（open.feishu.cn/open.larksuite.com）使用卡片 2.0
    def build_request(batch_content: str, _num: int, _total: int) -> Dict[str, Any]:
        if "www.feishu.cn" in webhook_url:
            payload = {"msg_type": "text", "content": {"text": batch_content}}
        else:
            payload = {
                "msg_type": "interactive",
                "card": {
                    "schema": "2.0",
                    "body": {
                        "elements": [
                            {"tag": "markdown", "content": batch_content}
                        ]
                    },
                },
            }
        return {"headers": headers, "json": payload, "proxies": proxies}

    def check_response(response: requests.Response) -> Optional[str]:
        if response.status_code != 200:
            return f"状态码：{response.status_code}"
        result = response.json()
        if result.get("StatusCode") == 0 or result.get("code") == 0:
            return None
        return result.get("msg") or result.get("StatusMessage", "未知错误")

    return _post_batches(
        webhook_url,
        batches,
        build_request,
        check_response,
        log_prefix=log_prefix,
        report_type=report_type,
        batch_interval=batch_interval,
    )


def send_to_dingtalk(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 20000,
    batch_interval: float = 1.0,
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到钉钉（支持分批发送，支持热榜+RSS合并+独立展示区）

    Args:
        webhook_url: 钉钉 Webhook URL
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    headers = {"Content-Type": "application/json"}
    proxies = _build_proxies(proxy_url)

    # 日志前缀
    log_prefix = f"钉钉{account_label}" if account_label else "钉钉"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "dingtalk") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 预留批次头部空间，避免添加头部后超限
    header_reserve = get_max_batch_header_size("dingtalk")
    batches = split_content_func(
        report_data,
        "dingtalk",
        update_info,
        max_bytes=batch_size - header_reserve,
        mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "dingtalk", batch_size)

    def build_request(batch_content: str, _num: int, _total: int) -> Dict[str, Any]:
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"TrendRadar 热点分析报告 - {report_type}",
                "text": batch_content,
            },
        }
        return {"headers": headers, "json": payload, "proxies": proxies}

    return _post_batches(
        webhook_url,
        batches,
        build_request,
        _check_json_field("errcode", 0, ("errmsg",)),
        log_prefix=log_prefix,
        report_type=report_type,
        batch_interval=batch_interval,
    )


def send_to_wework(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 4000,
    batch_interval: float = 1.0,
    msg_type: str = "markdown",
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到企业微信（支持分批发送，支持 markdown 和 text 两种格式，支持热榜+RSS合并+独立展示区）

    Args:
        webhook_url: 企业微信 Webhook URL
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        msg_type: 消息类型 (markdown/text)
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    headers = {"Content-Type": "application/json"}
    proxies = _build_proxies(proxy_url)

    # 日志前缀
    log_prefix = f"企业微信{account_label}" if account_label else "企业微信"

    # 获取消息类型配置（markdown 或 text）
    is_text_mode = msg_type.lower() == "text"

    if is_text_mode:
        print(f"{log_prefix}使用 text 格式（个人微信模式）[{report_type}]")
    else:
        print(f"{log_prefix}使用 markdown 格式（群机器人模式）[{report_type}]")

    # text 模式使用 wework_text，markdown 模式使用 wework
    header_format_type = "wework_text" if is_text_mode else "wework"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "wework") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容，预留批次头部空间
    header_reserve = get_max_batch_header_size(header_format_type)
    batches = split_content_func(
        report_data, "wework", update_info, max_bytes=batch_size - header_reserve, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, header_format_type, batch_size)

    # text 格式去除 markdown 语法；markdown 格式保持原样
    if is_text_mode:
        batches = [strip_markdown(b) for b in batches]

    def build_request(batch_content: str, _num: int, _total: int) -> Dict[str, Any]:
        if is_text_mode:
            payload = {"msgtype": "text", "text": {"content": batch_content}}
        else:
            payload = {"msgtype": "markdown", "markdown": {"content": batch_content}}
        return {"headers": headers, "json": payload, "proxies": proxies}

    return _post_batches(
        webhook_url,
        batches,
        build_request,
        _check_json_field("errcode", 0, ("errmsg",)),
        log_prefix=log_prefix,
        report_type=report_type,
        batch_interval=batch_interval,
    )


def send_to_telegram(
    bot_token: str,
    chat_id: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 4000,
    batch_interval: float = 1.0,
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到 Telegram（支持分批发送，支持热榜+RSS合并+独立展示区）

    Args:
        bot_token: Telegram Bot Token
        chat_id: Telegram Chat ID
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    headers = {"Content-Type": "application/json"}
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    proxies = _build_proxies(proxy_url)

    # 日志前缀
    log_prefix = f"Telegram{account_label}" if account_label else "Telegram"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "telegram") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容，预留批次头部空间
    header_reserve = get_max_batch_header_size("telegram")
    batches = split_content_func(
        report_data, "telegram", update_info, max_bytes=batch_size - header_reserve, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "telegram", batch_size)

    def build_request(batch_content: str, _num: int, _total: int) -> Dict[str, Any]:
        payload = {
            "chat_id": chat_id,
            "text": batch_content,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        return {"headers": headers, "json": payload, "proxies": proxies}

    return _post_batches(
        url,
        batches,
        build_request,
        _check_json_field("ok", True, ("description",)),
        log_prefix=log_prefix,
        report_type=report_type,
        batch_interval=batch_interval,
    )


def send_to_email(
    from_email: str,
    password: str,
    to_email: str,
    report_type: str,
    html_file_path: str,
    custom_smtp_server: Optional[str] = None,
    custom_smtp_port: Optional[int] = None,
    *,
    get_time_func: Callable = None,
) -> bool:
    """
    发送邮件通知

    Args:
        from_email: 发件人邮箱
        password: 邮箱密码/授权码
        to_email: 收件人邮箱（多个用逗号分隔）
        report_type: 报告类型
        html_file_path: HTML 报告文件路径
        custom_smtp_server: 自定义 SMTP 服务器（可选）
        custom_smtp_port: 自定义 SMTP 端口（可选）
        get_time_func: 获取当前时间的函数

    Returns:
        bool: 发送是否成功

    Note:
        AI 分析内容已在 HTML 生成时嵌入，无需再追加
    """
    try:
        if not html_file_path or not Path(html_file_path).exists():
            print(f"错误：HTML文件不存在或未提供: {html_file_path}")
            return False

        print(f"使用HTML文件: {html_file_path}")
        with open(html_file_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        domain = from_email.split("@")[-1].lower()

        if custom_smtp_server and custom_smtp_port:
            # 使用自定义 SMTP 配置
            smtp_server = custom_smtp_server
            smtp_port = int(custom_smtp_port)
            # 根据端口判断加密方式：465=SSL, 587=TLS
            if smtp_port == 465:
                use_tls = False  # SSL 模式（SMTP_SSL）
            elif smtp_port == 587:
                use_tls = True  # TLS 模式（STARTTLS）
            else:
                # 其他端口优先尝试 TLS（更安全，更广泛支持）
                use_tls = True
        elif domain in SMTP_CONFIGS:
            # 使用预设配置
            config = SMTP_CONFIGS[domain]
            smtp_server = config["server"]
            smtp_port = config["port"]
            use_tls = config["encryption"] == "TLS"
        else:
            print(f"未识别的邮箱服务商: {domain}，使用通用 SMTP 配置")
            smtp_server = f"smtp.{domain}"
            smtp_port = 587
            use_tls = True

        msg = MIMEMultipart("alternative")

        # 严格按照 RFC 标准设置 From header
        sender_name = "TrendRadar"
        msg["From"] = formataddr((sender_name, from_email))

        # 设置收件人
        recipients = [addr.strip() for addr in to_email.split(",")]
        if len(recipients) == 1:
            msg["To"] = recipients[0]
        else:
            msg["To"] = ", ".join(recipients)

        # 设置邮件主题
        now = get_time_func() if get_time_func else datetime.now()
        subject = f"TrendRadar 热点分析报告 - {report_type} - {now.strftime('%m月%d日 %H:%M')}"
        msg["Subject"] = Header(subject, "utf-8")

        # 设置其他标准 header
        msg["MIME-Version"] = "1.0"
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = make_msgid()

        # 添加纯文本部分（作为备选）
        text_content = f"""
TrendRadar 热点分析报告
========================
报告类型：{report_type}
生成时间：{now.strftime('%Y-%m-%d %H:%M:%S')}

请使用支持HTML的邮件客户端查看完整报告内容。
        """
        text_part = MIMEText(text_content, "plain", "utf-8")
        msg.attach(text_part)

        html_part = MIMEText(html_content, "html", "utf-8")
        msg.attach(html_part)

        print(f"正在发送邮件到 {to_email}...")
        print(f"SMTP 服务器: {smtp_server}:{smtp_port}")
        print(f"发件人: {from_email}")

        try:
            if use_tls:
                # TLS 模式
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.set_debuglevel(0)  # 设为1可以查看详细调试信息
                server.ehlo()
                server.starttls()
                server.ehlo()
            else:
                # SSL 模式
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30)
                server.set_debuglevel(0)
                server.ehlo()

            # 登录
            server.login(from_email, password)

            # 发送邮件
            server.send_message(msg)
            server.quit()

            print(f"邮件发送成功 [{report_type}] -> {to_email}")
            return True

        except smtplib.SMTPServerDisconnected:
            print("邮件发送失败：服务器意外断开连接，请检查网络或稍后重试")
            return False

    except smtplib.SMTPAuthenticationError as e:
        print("邮件发送失败：认证错误，请检查邮箱和密码/授权码")
        print(f"详细错误: {str(e)}")
        return False
    except smtplib.SMTPRecipientsRefused as e:
        print(f"邮件发送失败：收件人地址被拒绝 {e}")
        return False
    except smtplib.SMTPSenderRefused as e:
        print(f"邮件发送失败：发件人地址被拒绝 {e}")
        return False
    except smtplib.SMTPDataError as e:
        print(f"邮件发送失败：邮件数据错误 {e}")
        return False
    except smtplib.SMTPConnectError as e:
        print(f"邮件发送失败：无法连接到 SMTP 服务器 {smtp_server}:{smtp_port}")
        print(f"详细错误: {str(e)}")
        return False
    except Exception as e:
        print(f"邮件发送失败 [{report_type}]：{e}")
        import traceback
        traceback.print_exc()
        return False


def send_to_ntfy(
    server_url: str,
    topic: str,
    token: Optional[str],
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 3800,
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到 ntfy（支持分批发送，严格遵守4KB限制，支持热榜+RSS合并+独立展示区）

    Args:
        server_url: ntfy 服务器 URL
        topic: ntfy 主题
        token: ntfy 访问令牌（可选）
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    # 日志前缀
    log_prefix = f"ntfy{account_label}" if account_label else "ntfy"

    # 避免 HTTP header 编码问题
    report_type_en_map = {
        "全天汇总": "Daily Summary",
        "当前榜单": "Current Ranking",
        "增量分析": "Incremental Update",
        "通知连通性测试": "Notification Test",
    }
    report_type_en = report_type_en_map.get(report_type, "News Report")

    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "Markdown": "yes",
        "Title": report_type_en,
        "Priority": "default",
        "Tags": "news",
    }

    if token:
        headers["Authorization"] = f"Bearer {token}"

    # 构建完整URL，确保格式正确
    base_url = server_url.rstrip("/")
    if not base_url.startswith(("http://", "https://")):
        base_url = f"https://{base_url}"
    url = f"{base_url}/{topic}"
    proxies = _build_proxies(proxy_url)

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "ntfy") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容，预留批次头部空间
    header_reserve = get_max_batch_header_size("ntfy")
    batches = split_content_func(
        report_data, "ntfy", update_info, max_bytes=batch_size - header_reserve, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "ntfy", batch_size)

    # ntfy 严格限制 4KB
    for num, batch_content in enumerate(batches, 1):
        content_size = len(batch_content.encode("utf-8"))
        if content_size > 4096:
            logger.warning(
                "%s第 %d 批次消息过大（%d 字节），可能被拒绝", log_prefix, num, content_size
            )

    def build_request(batch_content: str, num: int, total: int) -> Dict[str, Any]:
        current_headers = headers.copy()
        if total > 1:
            current_headers["Title"] = f"{report_type_en} ({num}/{total})"
        return {
            "headers": current_headers,
            "data": batch_content.encode("utf-8"),
            "proxies": proxies,
        }

    def check_response(response: requests.Response) -> Optional[str]:
        if response.status_code == 200:
            return None
        logger.debug("%s", response.text)
        if response.status_code == 413:
            return "消息过大被拒绝（413）"
        return f"状态码：{response.status_code}"

    # 公共服务器建议 2-3 秒，自托管可以更短
    interval = 2 if "ntfy.sh" in server_url else 1

    # ntfy 客户端最新消息在上，反向推送以保证显示顺序
    return _post_batches(
        url,
        batches,
        build_request,
        check_response,
        log_prefix=log_prefix,
        report_type=report_type,
        batch_interval=interval,
        reverse=True,
        stop_on_failure=False,
    )


def send_to_bark(
    bark_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 3600,
    batch_interval: float = 1.0,
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到 Bark（支持分批发送，使用 markdown 格式，支持热榜+RSS合并+独立展示区）

    Args:
        bark_url: Bark URL（包含 device_key）
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    # 日志前缀
    log_prefix = f"Bark{account_label}" if account_label else "Bark"
    proxies = _build_proxies(proxy_url)

    # 解析 Bark URL，提取 device_key 和 API 端点
    # Bark URL 格式: https://api.day.app/device_key 或 https://bark.day.app/device_key
    parsed_url = urlparse(bark_url)
    device_key = parsed_url.path.strip('/').split('/')[0] if parsed_url.path else None

    if not device_key:
        print(f"{log_prefix} URL 格式错误，无法提取 device_key: {bark_url}")
        return False

    # 构建正确的 API 端点
    api_endpoint = f"{parsed_url.scheme}://{parsed_url.netloc}/push"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "bark") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容，预留批次头部空间
    header_reserve = get_max_batch_header_size("bark")
    batches = split_content_func(
        report_data, "bark", update_info, max_bytes=batch_size - header_reserve, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "bark", batch_size)

    # Bark 使用 APNs，限制 4KB
    for num, batch_content in enumerate(batches, 1):
        content_size = len(batch_content.encode("utf-8"))
        if content_size > 4096:
            logger.warning(
                "%s第 %d 批次消息过大（%d 字节），可能被拒绝", log_prefix, num, content_size
            )

    def build_request(batch_content: str, _num: int, _total: int) -> Dict[str, Any]:
        payload = {
            "title": report_type,
            "markdown": batch_content,
            "device_key": device_key,
            "sound": "default",
            "group": "TrendRadar",
            "action": "none",  # 点击推送跳到 APP 不弹出弹框,方便阅读
        }
        return {"json": payload, "proxies": proxies}

    # Bark 客户端最新消息在上，反向推送以保证显示顺序
    return _post_batches(
        api_endpoint,
        batches,
        build_request,
        _check_json_field("code", 200, ("message",)),
        log_prefix=log_prefix,
        report_type=report_type,
        batch_interval=batch_interval,
        reverse=True,
        stop_on_failure=False,
    )


def send_to_slack(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 4000,
    batch_interval: float = 1.0,
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到 Slack（支持分批发送，使用 mrkdwn 格式，支持热榜+RSS合并+独立展示区）

    Args:
        webhook_url: Slack Webhook URL
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    headers = {"Content-Type": "application/json"}
    proxies = _build_proxies(proxy_url)

    # 日志前缀
    log_prefix = f"Slack{account_label}" if account_label else "Slack"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "slack") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容，预留批次头部空间
    header_reserve = get_max_batch_header_size("slack")
    batches = split_content_func(
        report_data, "slack", update_info, max_bytes=batch_size - header_reserve, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "slack", batch_size)

    # 转换 Markdown 到 mrkdwn 格式
    batches = [convert_markdown_to_mrkdwn(b) for b in batches]

    def build_request(batch_content: str, _num: int, _total: int) -> Dict[str, Any]:
        return {"headers": headers, "json": {"text": batch_content}, "proxies": proxies}

    # Slack Incoming Webhooks 成功时返回 "ok" 文本
    def check_response(response: requests.Response) -> Optional[str]:
        if response.status_code == 200 and response.text == "ok":
            return None
        return response.text if response.text else f"状态码：{response.status_code}"

    return _post_batches(
        webhook_url,
        batches,
        build_request,
        check_response,
        log_prefix=log_prefix,
        report_type=report_type,
        batch_interval=batch_interval,
    )


def send_to_generic_webhook(
    webhook_url: str,
    payload_template: Optional[str],
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 4000,
    batch_interval: float = 1.0,
    split_content_func: Optional[Callable] = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到通用 Webhook（支持分批发送，支持自定义 JSON 模板，支持热榜+RSS合并+独立展示区）

    Args:
        webhook_url: Webhook URL
        payload_template: JSON 模板字符串，支持 {title} 和 {content} 占位符
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    if split_content_func is None:
        raise ValueError("split_content_func is required")

    headers = {"Content-Type": "application/json"}
    proxies = _build_proxies(proxy_url)

    # 日志前缀
    log_prefix = f"通用Webhook{account_label}" if account_label else "通用Webhook"

    # 渲染 AI 分析内容并提取统计数据（通用 Webhook 使用 markdown 格式）
    ai_content = _render_ai_analysis(ai_analysis, "wework") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容
    # 使用 'wework' 作为 format_type 以获取 markdown 格式的通用输出
    # 预留一定空间给模板外壳
    template_overhead = 200
    batches = split_content_func(
        report_data, "wework", update_info, max_bytes=batch_size - template_overhead, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部
    batches = add_batch_headers(batches, "wework", batch_size)

    def build_request(batch_content: str, _num: int, _total: int) -> Dict[str, Any]:
        payload = {"title": report_type, "content": batch_content}
        if payload_template:
            # content 可能包含 JSON 特殊字符，先转义后去掉首尾引号再替换
            json_content = json.dumps(batch_content)[1:-1]
            json_title = json.dumps(report_type)[1:-1]
            payload_str = payload_template.replace("{content}", json_content).replace(
                "{title}", json_title
            )
            try:
                payload = json.loads(payload_str)
            except json.JSONDecodeError as e:
                logger.warning("%s JSON 模板解析失败，回退到默认格式: %s", log_prefix, e)
        return {"headers": headers, "json": payload, "proxies": proxies}

    def check_response(response: requests.Response) -> Optional[str]:
        if 200 <= response.status_code < 300:
            return None
        return f"状态码：{response.status_code}, 响应: {response.text}"

    return _post_batches(
        webhook_url,
        batches,
        build_request,
        check_response,
        log_prefix=log_prefix,
        report_type=report_type,
        batch_interval=batch_interval,
    )
