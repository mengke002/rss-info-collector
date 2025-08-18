"""
RSS解析器模块
"""
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Any, Optional
import html
import re
import io
import asyncio
from crawl4ai import AsyncWebCrawler

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
    
    def parse_feed(self, feed_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """解析RSS源"""
        url = feed_config['rss_url']
        strategy = feed_config.get('strategy', 'requests')

        if strategy == 'crawl4ai':
            return asyncio.run(self._parse_with_crawl4ai(url))
        else:
            return self._parse_with_requests(url)

    def _parse_with_requests(self, url: str) -> List[Dict[str, Any]]:
        """使用requests解析RSS源"""
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            content = response.content.decode('utf-8', errors='ignore')
            return self._parse_xml_content(content, url)
        except Exception as e:
            logger.error(f"Failed to parse RSS feed {url} with requests: {e}")
            return []

    async def _parse_with_crawl4ai(self, url: str) -> List[Dict[str, Any]]:
        """使用crawl4ai解析RSS源（异步）"""
        try:
            from bs4 import BeautifulSoup
            async with AsyncWebCrawler() as crawler:
                result = await crawler.arun(url=url)
            
            soup = BeautifulSoup(result.html, 'html.parser')
            xml_div = soup.find('div', {'id': 'webkit-xml-viewer-source-xml'})
            if xml_div:
                rss_tag = xml_div.find('rss')
                if rss_tag:
                    xml_content = str(rss_tag)
                    return self._parse_xml_content(xml_content, url)
            
            # If the div or rss tag is not found, try to parse the whole html
            return self._parse_xml_content(result.html, url)
        except Exception as e:
            logger.error(f"Failed to parse RSS feed {url} with crawl4ai: {e}")
            return []

    def _parse_xml_content(self, content: str, url: str) -> List[Dict[str, Any]]:
        """从XML内容解析条目"""
        root = ET.fromstring(content)
        namespaces = self._get_namespaces(content)

        items = []
        if root.tag.endswith('rss'):
            channel = root.find('channel')
            if channel is not None:
                for item in channel.findall('item'):
                    parsed_item = self._parse_rss_item(item, namespaces, url)
                    if parsed_item:
                        items.append(parsed_item)
        elif root.tag.endswith('feed'):
            for entry in root.findall('atom:entry', namespaces):
                parsed_item = self._parse_atom_item(entry, namespaces)
                if parsed_item:
                    items.append(parsed_item)

        logger.info(f"Successfully parsed {url}: {len(items)} items")
        return items

    def _get_namespaces(self, xml_content: str) -> Dict[str, str]:
        """从XML内容中提取命名空间"""
        namespaces = dict([
            node for _, node in ET.iterparse(
                io.StringIO(xml_content), events=['start-ns']
            )
        ])
        if 'atom' not in namespaces:
            namespaces['atom'] = 'http://www.w3.org/2005/Atom'
        if 'dc' not in namespaces:
            namespaces['dc'] = 'http://purl.org/dc/elements/1.1/'
        return namespaces

    def _parse_rss_item(self, item: ET.Element, namespaces: Dict[str, str], url: str) -> Optional[Dict[str, Any]]:
        """解析RSS条目"""
        try:
            data = {}
            data['title'] = self._get_element_text(item, 'title', namespaces)
            data['link'] = self._get_element_text(item, 'link', namespaces)
            data['guid'] = self._get_element_text(item, 'guid', namespaces, data['link'])
            
            description_html = self._get_element_text(item, 'description', namespaces)
            data['summary'] = self._clean_html(description_html)
            data['image_url'] = self._extract_image_from_html(description_html)

            data['published_at'] = self._parse_date(self._get_element_text(item, 'pubDate', namespaces))
            
            author = self._get_element_text(item, 'dc:creator', namespaces)
            if author:
                data['author'] = author

            if 'techcrunch' not in url:
                categories = [self._clean_html(c.text) for c in item.findall('category')]
                data['category'] = ', '.join(categories) if categories else ""
            
            return data
        except Exception as e:
            logger.error(f"Failed to parse RSS item: {e}")
            return None

    def _parse_atom_item(self, entry: ET.Element, namespaces: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """解析Atom条目"""
        try:
            data = {}
            data['title'] = self._get_element_text(entry, 'atom:title', namespaces)
            data['link'] = entry.find('atom:link', namespaces).get('href') if entry.find('atom:link', namespaces) is not None else ''
            data['guid'] = self._get_element_text(entry, 'atom:id', namespaces, data['link'])

            summary_html = self._get_element_text(entry, 'atom:summary', namespaces)
            content_html = self._get_element_text(entry, 'atom:content', namespaces)
            
            data['summary'] = self._clean_html(summary_html or content_html)
            data['image_url'] = self._extract_image_from_html(content_html or summary_html)

            data['published_at'] = self._parse_date(self._get_element_text(entry, 'atom:published', namespaces))
            data['updated_at'] = self._parse_date(self._get_element_text(entry, 'atom:updated', namespaces))

            author_elem = entry.find('atom:author', namespaces)
            if author_elem is not None:
                author = self._get_element_text(author_elem, 'atom:name', namespaces)
                if author:
                    data['author'] = author
            
            categories = [c.get('term') for c in entry.findall('atom:category', namespaces)]
            if categories:
                data['category'] = ', '.join(filter(None, categories))

            return data
        except Exception as e:
            logger.error(f"Failed to parse Atom item: {e}")
            return None

    def _get_element_text(self, element: ET.Element, tag: str, namespaces: Dict[str, str], default: str = "") -> str:
        """安全地获取元素的文本内容"""
        elem = element.find(tag, namespaces)
        if elem is not None and elem.text:
            return html.unescape(elem.text.strip())
        return default

    def _extract_image_from_html(self, html_content: str) -> Optional[str]:
        """从HTML内容中提取第一张图片的URL"""
        if not html_content:
            return None
        match = re.search(r'<img[^>]+src="([^"]+)"', html_content)
        if match:
            return match.group(1)
        return None

    def _clean_html(self, html_text: str) -> str:
        """清理HTML标签"""
        if not html_text:
            return ""
        
        # 使用html2text进行更可靠的清理
        try:
            import html2text
            h = html2text.HTML2Text()
            h.ignore_links = True
            h.ignore_images = True
            clean_text = h.handle(html_text)
        except ImportError:
            # Fallback to regex if html2text is not available
            clean_text = re.sub(r'<[^>]*>', '', html_text)
            clean_text = html.unescape(clean_text)

        # 移除多余的空白
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        return clean_text

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """解析日期字符串"""
        if not date_str:
            return None
        try:
            from dateutil import parser
            return parser.parse(date_str)
        except Exception:
            logger.warning(f"Could not parse date: {date_str}")
            return None
    
    def extract_visit_url(self, guid: str, feed_type: str) -> str:
        """提取特殊URL（如BetaList的visit_url）"""
        if feed_type == 'betalist' and guid:
            # BetaList的特殊处理：在链接后添加/visit
            if not guid.endswith('/'):
                guid += '/'
            return guid + 'visit'
        return guid

# 全局解析器实例
rss_parser = RSSParser()