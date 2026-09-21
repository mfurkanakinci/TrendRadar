"""
参数验证工具

提供统一的参数验证功能。
支持 MCP 客户端将参数序列化为字符串的情况。
"""

from datetime import datetime
from typing import List, Optional, Union
import logging
import os
import json
import yaml
import ast

from .errors import ConfigurationError, InvalidParameterError
from .date_parser import DateParser

log = logging.getLogger(__name__)


# ==================== 延迟加载支持的平台列表之前的注释保留 ====================
PLACEHOLDER