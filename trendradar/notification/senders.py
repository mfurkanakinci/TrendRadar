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
