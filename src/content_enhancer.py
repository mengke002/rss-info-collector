"""
内容增强模块 - 用于处理description缺失时的crawl4ai爬取
"""
import asyncio
import re
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
    
    async def fetch_full_content(self, url: str, max_retries: int = 2) -> Optional[str]:
        """
        使用 crawl4ai 获取给定URL的完整Markdown内容，支持重试。
        """
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    logger.info(f"Retrying ({attempt}/{max_retries}) for: {url}")
                    # 重试前等待一段时间
                    await asyncio.sleep(2 * attempt)
                else:
                    logger.info(f"Fetching full content for: {url}")
                
                result = await self.crawler.arun(url=url)
                
                if result.success and result.markdown:
                    logger.info(f"Successfully fetched content for: {url}")
                    return result.markdown
                elif result.success:
                    logger.warning(f"Content fetch succeeded but no markdown was returned for: {url}")
                    if attempt == max_retries:
                        return None
                else:
                    logger.error(f"Failed to fetch content for {url}: {getattr(result, 'error', 'Unknown error')}")
                    if attempt == max_retries:
                        return None
            except Exception as e:
                logger.error(f"Exception during content fetching for {url} (attempt {attempt + 1}): {e}")
                if attempt == max_retries:
                    return None
        
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
            fetch_tasks.append(enhancer.fetch_full_content(fetch_link))
            items_refs.append(item)
        if fetch_tasks:
            logger.info(f"处理批次: {len(fetch_tasks)} 个链接...")
            contents = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            for (item, fetch_link, content) in zip(items_refs, norm_links, contents):
                e = item.copy()
                if feed_type in ('indiehackers', 'techcrunch'):
                    e['link'] = fetch_link
                if isinstance(content, Exception):
                    e['full_content'] = f"无法获取完整内容，请访问原链接: {e.get('link')}"
                    e['content_fetched_at'] = None
                    logger.warning(f"内容抓取失败: {e['title'][:50]}... - {str(content)}")
                elif content:
                    if feed_type == 'indiehackers':
                        content = enhancer._extract_main_content(content)
                    e['full_content'] = content
                    e['content_fetched_at'] = datetime.now()
                    logger.info(f"内容抓取成功: {e['title'][:50]}...")
                else:
                    e['full_content'] = f"无法获取完整内容，请访问原链接: {e.get('link')}"
                    e['content_fetched_at'] = None
                    logger.warning(f"内容为空: {e['title'][:50]}...")
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
        async with self as enhancer:
            total_items = len(items)
            logger.info(f"开始分批爬取 {total_items} 个项目，每批 {batch_size} 个，批次间延迟 {batch_delay} 秒")
            for i in range(0, total_items, batch_size):
                batch = items[i:i + batch_size]
                batch_results = await self._fetch_batch(batch, enhancer, feed_type)
                enhanced_items.extend(batch_results)
                if i + batch_size < total_items:
                    await asyncio.sleep(batch_delay)
        logger.info(f"所有批次处理完成，共处理 {len(enhanced_items)} 个项目")
        return enhanced_items

# 全局实例
content_enhancer = ContentEnhancer()