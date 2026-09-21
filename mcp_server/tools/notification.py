# coding=utf-8
"""
通知推送工具

支持向已配置的通知渠道发送消息，自动检测 config.yaml 和 .env 中的渠道配置。
接受 markdown 格式内容，内部按各渠道要求自动转换格式后发送。
"""

import json
import logging
import os
import re
import smtplib
import time
from datetime import datetime
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
import yaml

from trendradar.core.loader import _load_webhook_config, _load_notification_config
from trendradar.notification.batch import (
    truncate_to_bytes,
    get_batch_header,
    get_max_batch_header_size,
    add_batch_headers,
)
from trendradar.notification.formatters import strip_markdown
from trendradar.notification.senders import SMTP_CONFIGS

from ..utils.errors import MCPError, InvalidParameterError

LOGGER = logging.getLogger(__name__)

# 配置读取失败时的可预期异常。预期之外的错误继续抛出。
# Expected config-load failures. Anything else propagates.
_CONFIG_LOAD_ERRORS = (OSError, yaml.YAMLError, AttributeError, TypeError, ValueError)
