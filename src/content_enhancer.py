"""
内容增强模块 - 用于处理description缺失时的crawl4ai爬取
"""
import asyncio
import re
import random
from datetime import datetime
from typing import Optional, Dict, Any
from crawl4ai import AsyncWebCrawler
from .logger import logger

class ContentEnhancer:
    """内容增强器 - 处理缺失的description"""
    
    def __init__(self):
        """初始化内容增强器"""
        self.crawler = None
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.crawler = AsyncWebCrawler()
        await self.crawler.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self.crawler:
            await self.crawler.__aexit__(exc_type, exc_val, exc_tb)
    
    async def fetch_full_content(self, url: str, feed_type: str, max_retries: int = 2) -> Optional[str]:
        """
        使用 crawl4ai 获取给定URL的完整Markdown内容，支持重试。
        针对特定feed类型应用不同的策略。
        """
        # 针对 indiehackers 使用更强的重试策略
        if feed_type == 'indiehackers':
            max_retries = 4

        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    # 指数退避 + 随机抖动
                    delay = (2 ** (attempt - 1)) + random.uniform(0.5, 1.5)
                    logger.info(f"Retrying ({attempt}/{max_retries}) for: {url} after {delay:.2f}s delay")
                    await asyncio.sleep(delay)
                else:
                    logger.info(f"Fetching full content for: {url}")
                
                result = await self.crawler.arun(url=url)
                
                if result.success and result.markdown:
                    logger.info(f"Successfully fetched content for: {url}")
                    return result.markdown
                
                # 检查是否是速率限制错误
                if not result.success and "1015" in getattr(result, 'error', ''):
                    logger.warning(f"Rate limit error detected for {url}. Retrying...")
                    # 如果是最后一次尝试，则返回错误信息
                    if attempt == max_retries:
                        return f"# Error 1015\nRate limited: {url}"
                    continue

                elif result.success:
                    logger.warning(f"Content fetch succeeded but no markdown was returned for: {url}")
                    if attempt == max_retries:
                        return None
                else:
                    error_msg = getattr(result, 'error', 'Unknown error')
                    logger.error(f"Failed to fetch content for {url}: {error_msg}")
                    if attempt == max_retries:
                        # 返回错误信息以便于调试
                        return f"# Fetch Error\nFailed to fetch {url}: {error_msg}"
            except Exception as e:
                logger.error(f"Exception during content fetching for {url} (attempt {attempt + 1}): {e}")
                if attempt == max_retries:
                    return f"# Fetch Exception\nException for {url}: {e}"
        
        return None
    
    def _normalize_indiehackers_url(self, link: str) -> Optional[str]:
        if not link:
            return None
        m = re.search(r'[?&]post=([^&#/]+)', link, re.IGNORECASE)
        if m:
            pid = m.group(1)
            return f"https://www.indiehackers.com/post/{pid}"
        if '/post/' in link:
            return link if link.startswith('http') else f"https://www.indiehackers.com{link}"
        return None
    
    def _extract_main_content(self, markdown: str) -> str:
        if not markdown:
            return ""
        m = re.search(r'(?m)^#\s+.+$', markdown)
        start = m.start() if m else 0
        text = markdown[start:]
        cut_points = []
        for pat in [
            r'Stay informed as an indie hacker\.',
            r'(?m)^Subscribe\s*$',
            r'©\s*Indie Hackers',
            r'(?m)^####\s*\[Community\]',
            r'(?m)^####\s*\[Products\]',
            r'(?m)^####\s*\[Databases\]']:
            m2 = re.search(pat, text)
            if m2:
                cut_points.append(m2.start())
        if cut_points:
            text = text[:min(cut_points)]
        text = re.sub(r'(?m)^\[.*?\]\(https://www\.indiehackers\.com/(sign-up|sign-in)[^)]*\)\s*$', '', text)
        text = re.sub(r'(?m)^\s*Share\s*$', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _clean_techcrunch_content(self, content: str) -> str:
        """
        清洗TechCrunch文章内容的冗余信息
        """
        if not content:
            return ""
        
        text = str(content)

        # 1. 定位文章主体内容
        match = re.search(r'(?m)^#\s+.+$', text)
        if not match:
            return text.strip()
        
        text = text[match.start():]

        # 2. 找到文章内容的结束点
        end_patterns = [
            r'\n_We’re always looking to evolve, and by providing some insight.*',
            r'\nTopics\n\n',
            r'\n## Most Popular',
            r'\n!\[Event Logo\]',
            r'\nLoading the next article'
        ]
        
        cut_off_point = len(text)
        for pattern in end_patterns:
            end_match = re.search(pattern, text, re.DOTALL)
            if end_match:
                cut_off_point = min(cut_off_point, end_match.start())
                
        text = text[:cut_off_point]

        # 3. 清理文章主体内部的残留噪声
        text = re.sub(r'(?m)^\[ \]\(https?://(www\.)?(facebook|twitter|linkedin|reddit)\.com/.*\)\s*$\n?', '', text)
        text = re.sub(r'(?m)^!\[.*?\]\(.*?\)\*\*Image Credits:.*$', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        
        return text
    
    async def _fetch_batch(self, items_batch: list, enhancer, feed_type: str) -> list:
        fetch_tasks = []
        items_refs = []
        norm_links = []
        batch_results = []
        for item in items_batch:
            link = item.get('link')
            if not link:
                e = item.copy()
                e['full_content'] = "缺少链接信息，无法获取完整内容"
                e['content_fetched_at'] = None
                batch_results.append(e)
                continue
            
            fetch_link = link
            if feed_type == 'indiehackers':
                nl = enhancer._normalize_indiehackers_url(link)
                fetch_link = nl or link
            
            norm_links.append(fetch_link)
            fetch_tasks.append(enhancer.fetch_full_content(fetch_link, feed_type=feed_type))
            items_refs.append(item)

        if fetch_tasks:
            logger.info(f"处理批次: {len(fetch_tasks)} 个链接...")
            contents = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            for (item, fetch_link, content) in zip(items_refs, norm_links, contents):
                e = item.copy()
                if feed_type in ('indiehackers', 'techcrunch'):
                    e['link'] = fetch_link

                if isinstance(content, Exception):
                    e['full_content'] = f"无法获取完整内容，请访问原链接: {item.get('link')}"
                    e['content_fetched_at'] = None
                    logger.warning(f"内容抓取失败: {item.get('title', 'N/A')[:50]}... - {str(content)}")
                elif content:
                    # 检查是否是抓取失败后返回的错误信息
                    is_error_content = content.startswith(('# Error', '# Fetch'))
                    
                    if feed_type == 'indiehackers' and not is_error_content:
                        content = enhancer._extract_main_content(content)
                    elif feed_type == 'techcrunch' and not is_error_content:
                        content = enhancer._clean_techcrunch_content(content)
                    
                    e['full_content'] = content
                    e['content_fetched_at'] = datetime.now()
                    
                    if not is_error_content:
                        logger.info(f"内容抓取成功: {item.get('title', 'N/A')[:50]}...")
                    else:
                        logger.warning(f"内容抓取失败(有错误信息): {item.get('title', 'N/A')[:50]}...")
                else:
                    e['full_content'] = f"无法获取完整内容，请访问原链接: {item.get('link')}"
                    e['content_fetched_at'] = None
                    logger.warning(f"内容为空: {item.get('title', 'N/A')[:50]}...")
                batch_results.append(e)
                
        return batch_results

    async def enhance_items(self, items: list, feed_type: str, batch_size: int = 5, batch_delay: float = 2.0) -> list:
        enhanced_items = []
        if feed_type not in ('ycombinator', 'indiehackers', 'techcrunch'):
            for item in items:
                e = item.copy()
                e['full_content'] = item.get('summary', '')
                e['content_fetched_at'] = datetime.now()
                enhanced_items.append(e)
            return enhanced_items

        # 为 indiehackers 使用更保守的批处理策略
        if feed_type == 'indiehackers':
            batch_size = 3  # 减小批次大小
            batch_delay = 5.0  # 增加批次间延迟

        async with self as enhancer:
            total_items = len(items)
            logger.info(f"开始为 {feed_type} 分批爬取 {total_items} 个项目，每批 {batch_size} 个，批次间延迟 {batch_delay} 秒")
            for i in range(0, total_items, batch_size):
                batch = items[i:i + batch_size]
                batch_results = await self._fetch_batch(batch, enhancer, feed_type)
                enhanced_items.extend(batch_results)
                if i + batch_size < total_items:
                    logger.info(f"批次完成，等待 {batch_delay} 秒...")
                    await asyncio.sleep(batch_delay)
        logger.info(f"所有批次处理完成，共处理 {len(enhanced_items)} 个项目")
        return enhanced_items

# 全局实例
content_enhancer = ContentEnhancer()