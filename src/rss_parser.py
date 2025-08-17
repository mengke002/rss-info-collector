"""
RSS解析器模块
"""
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Any, Optional
import html
import re

from .logger import logger

class RSSParser:
    """RSS解析器"""
    
    def __init__(self, timeout: int = 30):
        """初始化解析器"""
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
    
    def parse_feed(self, url: str) -> List[Dict[str, Any]]:
        """解析RSS源"""
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            # 处理可能的编码问题
            content = response.content.decode('utf-8')
            
            # 解析XML
            root = ET.fromstring(content)
            
            # 处理命名空间
            namespaces = {
                'atom': 'http://www.w3.org/2005/Atom',
                'content': 'http://purl.org/rss/1.0/modules/content/',
                'dc': 'http://purl.org/dc/elements/1.1/',
                'media': 'http://search.yahoo.com/mrss/'
            }
            
            # 确定格式并提取条目
            items = []
            
            if root.tag.endswith('rss'):
                # RSS格式
                channel = root.find('channel')
                if channel is not None:
                    for item in channel.findall('item'):
                        parsed_item = self._parse_rss_item(item)
                        if parsed_item:
                            items.append(parsed_item)
            
            elif root.tag.endswith('feed'):
                # Atom格式
                for entry in root.findall('atom:entry', namespaces):
                    parsed_item = self._parse_atom_item(entry, namespaces)
                    if parsed_item:
                        items.append(parsed_item)
            
            logger.info(f"成功解析 {url}: {len(items)} 条记录")
            return items
            
        except Exception as e:
            logger.error(f"解析RSS源失败 {url}: {e}")
            return []
    
    def _parse_rss_item(self, item: ET.Element) -> Optional[Dict[str, Any]]:
        """解析RSS条目"""
        try:
            data = {}
            
            # 标题
            title_elem = item.find('title')
            data['title'] = html.unescape(title_elem.text.strip()) if title_elem is not None and title_elem.text else "No Title"
            
            # 链接
            link_elem = item.find('link')
            if link_elem is not None:
                if link_elem.text:
                    data['link'] = link_elem.text.strip()
                else:
                    # 处理属性形式的link
                    data['link'] = link_elem.get('href', '')
            else:
                data['link'] = ""
            
            # GUID
            guid_elem = item.find('guid')
            if guid_elem is not None and guid_elem.text:
                data['guid'] = guid_elem.text.strip()
            else:
                # 如果没有guid，使用link作为guid
                data['guid'] = data['link']
            
            # 描述/摘要
            desc_elem = item.find('description')
            if desc_elem is not None and desc_elem.text:
                # 清理HTML标签
                summary = self._clean_html(desc_elem.text.strip())
                data['summary'] = summary[:1000]  # 限制长度
            else:
                data['summary'] = ""
            
            # 发布日期
            pub_date_elem = item.find('pubDate')
            if pub_date_elem is not None and pub_date_elem.text:
                published_at = self._parse_date(pub_date_elem.text.strip())
                data['published_at'] = published_at
            else:
                data['published_at'] = datetime.now()
            
            # 作者
            author_elem = item.find('author')
            if author_elem is not None and author_elem.text:
                data['author'] = html.unescape(author_elem.text.strip())
            else:
                creator_elem = item.find('dc:creator')
                if creator_elem is not None and creator_elem.text:
                    data['author'] = html.unescape(creator_elem.text.strip())
                else:
                    data['author'] = "Unknown"
            
            # 分类
            category_elem = item.find('category')
            if category_elem is not None and category_elem.text:
                data['category'] = html.unescape(category_elem.text.strip())
            else:
                data['category'] = ""
            
            return data
            
        except Exception as e:
            logger.error(f"解析RSS条目失败: {e}")
            return None
    
    def _parse_atom_item(self, entry: ET.Element, namespaces: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """解析Atom条目"""
        try:
            data = {}
            
            # 标题
            title_elem = entry.find('atom:title', namespaces)
            data['title'] = html.unescape(title_elem.text.strip()) if title_elem is not None and title_elem.text else "No Title"
            
            # 链接
            link_elem = entry.find('atom:link', namespaces)
            if link_elem is not None:
                href = link_elem.get('href', '')
                data['link'] = href
            else:
                data['link'] = ""
            
            # GUID (使用link作为guid)
            data['guid'] = data['link']
            
            # 摘要
            summary_elem = entry.find('atom:summary', namespaces)
            if summary_elem is not None and summary_elem.text:
                summary = self._clean_html(summary_elem.text.strip())
                data['summary'] = summary[:1000]
            else:
                data['summary'] = ""
            
            # 发布日期
            published_elem = entry.find('atom:published', namespaces)
            if published_elem is not None and published_elem.text:
                published_at = self._parse_date(published_elem.text.strip())
                data['published_at'] = published_at
            else:
                updated_elem = entry.find('atom:updated', namespaces)
                if updated_elem is not None and updated_elem.text:
                    published_at = self._parse_date(updated_elem.text.strip())
                    data['published_at'] = published_at
                else:
                    data['published_at'] = datetime.now()
            
            # 作者
            author_elem = entry.find('atom:author', namespaces)
            if author_elem is not None:
                name_elem = author_elem.find('atom:name', namespaces)
                if name_elem is not None and name_elem.text:
                    data['author'] = html.unescape(name_elem.text.strip())
                else:
                    data['author'] = "Unknown"
            else:
                data['author'] = "Unknown"
            
            # 分类（Atom中可能为category元素）
            category_elem = entry.find('atom:category', namespaces)
            if category_elem is not None:
                term = category_elem.get('term', '')
                data['category'] = html.unescape(term) if term else ""
            else:
                data['category'] = ""
            
            return data
            
        except Exception as e:
            logger.error(f"解析Atom条目失败: {e}")
            return None
    
    def _clean_html(self, html_text: str) -> str:
        """清理HTML标签"""
        if not html_text:
            return ""
        
        # 移除HTML标签
        clean_text = re.sub(r'<[^>]*>', '', html_text)
        
        # 解码HTML实体
        clean_text = html.unescape(clean_text)
        
        # 移除多余的空白
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        return clean_text
    
    def _parse_date(self, date_str: str) -> datetime:
        """解析日期字符串"""
        try:
            from dateutil import parser
            return parser.parse(date_str)
        except Exception:
            logger.warning(f"无法解析日期: {date_str}")
            return datetime.now()
    
    def extract_visit_url(self, link: str, feed_type: str) -> str:
        """提取特殊URL（如BetaList的visit_url）"""
        if feed_type == 'betalist' and link:
            # BetaList的特殊处理：在链接后添加/visit
            if not link.endswith('/'):
                link += '/'
            return link + 'visit'
        return link

# 全局解析器实例
rss_parser = RSSParser()