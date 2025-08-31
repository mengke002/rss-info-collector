"""
RSS解析器模块
"""
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import html
import re
import io
import asyncio
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup

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
        # Decohack解析相关的正则表达式
        self.vote_pattern = re.compile(r'🔺(\d+)')
        self.time_pattern = re.compile(r'(\d{4})年(\d{2})月(\d{2})日')
    
    def parse_feed(self, feed_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """解析RSS源"""
        url = feed_config['rss_url']
        strategy = feed_config.get('strategy', 'requests')

        if strategy == 'crawl4ai':
            return asyncio.run(self._parse_with_crawl4ai(url))
        else:
            return self._parse_with_requests(url)

    def _parse_with_requests(self, url: str) -> List[Dict[str, Any]]:
        """使用requests解析RSS源，并支持备用URL机制"""
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            content = response.content.decode('utf-8', errors='ignore')
            return self._parse_xml_content(content, url)
        except Exception as e:
            logger.warning(f"Failed to parse RSS feed {url} with requests: {e}")
            
            # 检查是否为RSSHub源，并尝试备用URL
            if "https://rsshub.rssforever.com/" in url:
                backup_url = url.replace("https://rsshub.rssforever.com/", "https://rsshub.app/")
                logger.info(f"Attempting to fetch from backup URL: {backup_url}")
                try:
                    response = self.session.get(backup_url, timeout=self.timeout)
                    response.raise_for_status()
                    content = response.content.decode('utf-8', errors='ignore')
                    return self._parse_xml_content(content, backup_url)
                except Exception as backup_e:
                    logger.error(f"Failed to parse RSS feed from backup URL {backup_url}: {backup_e}")
            
            return []

    async def _parse_with_crawl4ai(self, url: str) -> List[Dict[str, Any]]:
        """使用crawl4ai解析RSS源（异步），并支持备用URL机制"""
        try:
            return await self._fetch_and_parse_crawl4ai(url)
        except Exception as e:
            logger.warning(f"Failed to parse RSS feed {url} with crawl4ai: {e}")
            
            # 检查是否为RSSHub源，并尝试备用URL
            if "https://rsshub.rssforever.com/" in url:
                backup_url = url.replace("https://rsshub.rssforever.com/", "https://rsshub.app/")
                logger.info(f"Attempting to fetch from backup URL with crawl4ai: {backup_url}")
                try:
                    return await self._fetch_and_parse_crawl4ai(backup_url)
                except Exception as backup_e:
                    logger.error(f"Failed to parse RSS feed from backup URL {backup_url} with crawl4ai: {backup_e}")

            return []

    async def _fetch_and_parse_crawl4ai(self, url: str) -> List[Dict[str, Any]]:
        """crawl4ai的实际获取和解析逻辑"""
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url=url)
        
        # 直接从HTML中提取RSS内容
        html_content = result.html
        
        rss_start = html_content.find('<rss')
        rss_end = html_content.find('</rss>') + 6
        
        if rss_start != -1 and rss_end != -1:
            xml_content = html_content[rss_start:rss_end]
            xml_content = self._fix_broken_xml(xml_content)
            return self._parse_xml_content(xml_content, url)
        else:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            xml_div = soup.find('div', {'id': 'webkit-xml-viewer-source-xml'})
            if xml_div:
                rss_tag = xml_div.find('rss')
                if rss_tag:
                    xml_content = str(rss_tag)
                    xml_content = self._fix_broken_xml(xml_content)
                    return self._parse_xml_content(xml_content, url)
        
        # 回退到requests
        return self._parse_with_requests(url)

    def _parse_xml_content(self, content: str, url: str) -> List[Dict[str, Any]]:
        """从XML内容解析条目"""
        try:
            content = content.replace('\x00', '').strip()
            content = self._sanitize_xml_entities(content)
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
        except ET.ParseError as e:
            logger.error(f"XML解析失败 {url}: {e}")
            # 尝试清理XML内容后重新解析
            try:
                # 移除无效字符
                clean_content = re.sub(r'[^\x09\x0A\x0D\x20-\x7E\x85\xA0-\xFF]', '', content)
                clean_content = self._sanitize_xml_entities(clean_content)
                root = ET.fromstring(clean_content)
                # 重新解析...
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
                logger.info(f"清理后解析成功 {url}: {len(items)} items")
                return items
            except Exception as e2:
                logger.error(f"清理后解析仍然失败 {url}: {e2}")
                return []
        except Exception as e:
            logger.error(f"解析RSS失败 {url}: {e}")
            return []

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
        if 'content' not in namespaces:
            namespaces['content'] = 'http://purl.org/rss/1.0/modules/content/'
        return namespaces

    def _parse_rss_item(self, item: ET.Element, namespaces: Dict[str, str], url: str) -> Optional[Dict[str, Any]]:
        """解析RSS条目"""
        try:
            data = {}
            data['title'] = self._get_element_text(item, 'title', namespaces) or "无标题"
            data['link'] = self._get_element_text(item, 'link', namespaces) or ""
            data['guid'] = self._get_element_text(item, 'guid', namespaces, data['link']) or data['link'] or f"rss-{hash(str(item))}"
            
            # 检测是否为ycombinator RSS
            is_ycombinator = 'ycombinator' in url or 'hackernews' in url
            
            # 获取描述内容
            description_html = self._get_element_text(item, 'description', namespaces)
            if not description_html:
                description_html = self._get_element_text(item, 'content:encoded', namespaces)
            if not description_html:
                description_html = self._get_element_text(item, 'summary', namespaces)
            
            # 对于ycombinator，不设置summary字段，直接从link获取内容
            if is_ycombinator and description_html and "Comments on Hacker News" in description_html:
                # ycombinator不需要summary字段，将在后续通过link爬取完整内容
                pass
            else:
                data['summary'] = self._clean_html(description_html or "")
            
            data['image_url'] = self._extract_image_from_html(description_html or "")

            pub_date = self._get_element_text(item, 'pubDate', namespaces)
            if not pub_date:
                pub_date = self._get_element_text(item, 'dc:date', namespaces)
            data['published_at'] = self._parse_date(pub_date)
            
            author = self._get_element_text(item, 'dc:creator', namespaces)
            if not author:
                author = self._get_element_text(item, 'author', namespaces)
            if author:
                data['author'] = author

            if 'techcrunch' not in url:
                categories = []
                for cat in item.findall('category'):
                    if cat.text:
                        categories.append(self._clean_html(cat.text))
                data['category'] = ', '.join(categories) if categories else ""
            
            # 处理ycombinator和indiehackers的特殊情况
            is_indiehackers = 'indiehackers' in url
            
            if is_ycombinator or (is_indiehackers and (not data['summary'] or len(data['summary']) < 50)):
                # 标记需要后续爬取完整内容
                data['full_content'] = None
                data['content_fetched_at'] = None
            else:
                data['full_content'] = data['summary']
                data['content_fetched_at'] = datetime.now()

            # 针对ezindie，提取封面图
            if 'ezindie' in url:
                enclosure = item.find('enclosure')
                if enclosure is not None and 'url' in enclosure.attrib:
                    data['cover_image_url'] = enclosure.attrib['url']

            # 针对decohack，跳过旧的解析逻辑，标记为需要特殊处理
            if 'decohack' in url:
                # 标记为decohack源，后续会用专门的解析器处理
                data['is_decohack_source'] = True
                content_encoded = self._get_element_text(item, 'content:encoded', namespaces)
                if content_encoded:
                    data['full_content_html'] = content_encoded  # 保存原始HTML用于后续解析
                
                # 重新解析分类
                categories = []
                for cat in item.findall('category'):
                    if cat.text:
                        categories.append(self._clean_html(cat.text))
                if categories:
                    data['category'] = ', '.join(categories)

            # 确保必要字段不为空
            if not data['link'] and not data['guid']:
                return None
                
            return data
        except Exception as e:
            logger.error(f"Failed to parse RSS item: {e}")
            return None

    def _parse_atom_item(self, entry: ET.Element, namespaces: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """解析Atom条目"""
        try:
            data = {}
            data['title'] = self._get_element_text(entry, 'atom:title', namespaces) or "无标题"
            data['link'] = entry.find('atom:link', namespaces).get('href') if entry.find('atom:link', namespaces) is not None else ''
            data['guid'] = self._get_element_text(entry, 'atom:id', namespaces, data['link']) or data['link'] or f"atom-{hash(str(entry))}"

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

            # 处理indiehackers的特殊情况
            if 'indiehackers' in str(entry) and (not data['summary'] or len(data['summary']) < 50):
                # 标记需要后续爬取完整内容
                data['full_content'] = None
                data['content_fetched_at'] = None
            else:
                data['full_content'] = data['summary']
                data['content_fetched_at'] = datetime.now()

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
        if not html_text:
            return ""
        try:
            import html2text
            h = html2text.HTML2Text()
            h.ignore_links = True
            h.ignore_images = True
            clean_text = h.handle(html_text)
        except ImportError:
            clean_text = re.sub(r'<[^>]*>', '', html_text)
            clean_text = html.unescape(clean_text)
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
    
    def _fix_broken_xml(self, xml_content: str) -> str:
        import re
        s = re.sub(r'<(\w+)/>\s*([^<]+?)(?=<|$)', r'<\1>\2</\1>', xml_content)
        open_tags = re.findall(r'<(\w+)(?:\s[^>]*)?>(?!</\1>)', s)
        for tag in open_tags:
            if f'</{tag}>' not in s:
                s = re.sub(f'(<{tag}[^>]*>)([^<]*?)(?=<|$)', f'\\1\\2</{tag}>', s)
        s = self._sanitize_xml_entities(s)
        return s
     
    def extract_visit_url(self, guid: str, feed_type: str) -> str:
        """提取特殊URL（如BetaList的visit_url）"""
        if feed_type == 'betalist' and guid:
            # BetaList的特殊处理：在链接后添加/visit
            if not guid.endswith('/'):
                guid += '/'
            return guid + 'visit'
        return guid

    def _sanitize_xml_entities(self, s: str) -> str:
        def repl_named(m):
            name = m.group(1)
            low = name.lower()
            if low in ('amp', 'lt', 'gt', 'quot', 'apos'):
                return m.group(0)
            import html as _html
            try:
                val = _html.entities.html5.get(low + ';')
                if isinstance(val, str):
                    return val
            except Exception:
                pass
            return m.group(0)
        s = re.sub(r'&([A-Za-z][A-Za-z0-9]+);', repl_named, s)
        s = re.sub(r'&(?!#\d+;|#x[0-9a-fA-F]+;|amp;|lt;|gt;|quot;|apos;|[A-Za-z][A-Za-z0-9]+;)', '&amp;', s)
        return s

    def parse_decohack_products(self, content_html: str, crawl_date: date) -> List[Dict[str, Any]]:
        """解析Decohack每日热榜中的产品信息"""
        if not content_html:
            return []
        
        try:
            soup = BeautifulSoup(content_html, 'html.parser')
            products = []
            
            # 查找所有产品条目 (以h2标签开始的产品块)
            product_sections = soup.find_all('h2')
            
            for i, h2 in enumerate(product_sections):
                try:
                    product_data = self._parse_single_decohack_product(h2, crawl_date)
                    if product_data:
                        products.append(product_data)
                except Exception as e:
                    logger.warning(f"解析第{i+1}个产品时出错: {e}")
                    continue
            
            logger.info(f"成功解析{len(products)}个产品")
            return products
            
        except Exception as e:
            logger.error(f"解析Decohack产品列表失败: {e}")
            return []
    
    def _parse_single_decohack_product(self, h2_element, crawl_date: date) -> Optional[Dict[str, Any]]:
        """解析单个Decohack产品信息"""
        try:
            # 获取产品名称和PH链接
            product_link = h2_element.find('a')
            if not product_link:
                return None
                
            product_name = self._extract_product_name(product_link.get_text())
            ph_url = product_link.get('href', '')
            
            # 获取产品所在的段落容器
            current = h2_element
            product_info = {}
            
            # 遍历h2后面的元素直到下一个h2或hr
            while current and current.next_sibling:
                current = current.next_sibling
                if hasattr(current, 'name'):
                    if current.name == 'h2':
                        break
                    elif current.name == 'hr':
                        break
                    elif current.name == 'p':
                        self._parse_decohack_product_paragraph(current, product_info)
            
            # 构建产品数据
            return {
                'product_name': product_name[:100],  # 限制长度
                'tagline': product_info.get('tagline', '')[:200],
                'description': product_info.get('description', '')[:800],
                'product_url': product_info.get('product_url', '')[:400],
                'ph_url': ph_url[:400],
                'image_url': product_info.get('image_url', '')[:400],
                'vote_count': product_info.get('vote_count', 0),
                'is_featured': product_info.get('is_featured', False),
                'keywords': product_info.get('keywords', '')[:300],
                'ph_publish_date': product_info.get('ph_publish_date'),
                'crawl_date': crawl_date
            }
            
        except Exception as e:
            logger.error(f"解析单个产品失败: {e}")
            return None
    
    def _extract_product_name(self, text: str) -> str:
        """提取产品名称(去掉序号)"""
        # 移除开头的数字序号，如 "1. Creem 1.0" -> "Creem 1.0"
        cleaned = re.sub(r'^\d+\.\s*', '', text.strip())
        return cleaned
    
    def _parse_decohack_product_paragraph(self, p_element, product_info: Dict[str, Any]):
        """解析Decohack产品段落信息"""
        text = p_element.get_text()
        
        # 解析标语
        tagline_match = re.search(r'标语：(.+?)(?:\n|介绍：)', text)
        if tagline_match:
            product_info['tagline'] = tagline_match.group(1).strip()
        
        # 解析介绍
        desc_match = re.search(r'介绍：(.+?)(?:\n|产品网站：)', text)
        if desc_match:
            product_info['description'] = desc_match.group(1).strip()
        
        # 解析关键词
        keywords_match = re.search(r'关键词：(.+?)(?:\n|票数：)', text)
        if keywords_match:
            product_info['keywords'] = keywords_match.group(1).strip()
        
        # 解析票数
        vote_match = self.vote_pattern.search(text)
        if vote_match:
            try:
                product_info['vote_count'] = int(vote_match.group(1))
            except ValueError:
                product_info['vote_count'] = 0
        
        # 解析是否精选
        if '是否精选：是' in text:
            product_info['is_featured'] = True
        
        # 解析发布时间
        time_match = self.time_pattern.search(text)
        if time_match:
            try:
                year, month, day = map(int, time_match.groups())
                product_info['ph_publish_date'] = date(year, month, day)
            except ValueError:
                pass
        
        # 解析产品网站链接
        product_links = p_element.find_all('a')
        for link in product_links:
            link_text = link.get_text().strip()
            if '立即访问' in link_text or '产品网站' in link_text:
                product_info['product_url'] = link.get('href', '')
                break
        
        # 解析产品图片
        img = p_element.find('img')
        if img and img.get('src'):
            product_info['image_url'] = img.get('src')

# 全局解析器实例
rss_parser = RSSParser()