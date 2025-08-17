#!/usr/bin/env python3
"""
RSS Feed Format Analysis Script
用于分析各个RSS源的格式结构
"""
import requests
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# RSS源列表
RSS_SOURCES = {
    'betalist': 'https://feeds.feedburner.com/BetaList',
    'producthunt': 'https://www.producthunt.com/feed',
    'ycombinator': 'https://rsshub.app/hackernews',
    'techcrunch': 'https://rsshub.app/techcrunch/news',
    'theverge': 'https://www.theverge.com/rss/ai-artificial-intelligence/index.xml',
    'indiehackers_alltime': 'https://ihrss.io/top/all-time',
    'indiehackers_month': 'https://ihrss.io/top/month',
    'indiehackers_week': 'https://ihrss.io/top/week',
    'indiehackers_today': 'https://ihrss.io/top/today',
    'indiehackers_growth': 'https://ihrss.io/group/growth',
    'indiehackers_developers': 'https://ihrss.io/group/developers',
    'indiehackers_saas': 'https://ihrss.io/group/saas-marketing'
}

def analyze_rss_feed(name, url):
    """分析RSS源格式"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # 解析XML
        root = ET.fromstring(response.content)
        
        # 处理命名空间
        namespaces = {
            'atom': 'http://www.w3.org/2005/Atom',
            'content': 'http://purl.org/rss/1.0/modules/content/',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'media': 'http://search.yahoo.com/mrss/'
        }
        
        # 确定根元素
        if root.tag.endswith('rss'):
            channel = root.find('channel')
        elif root.tag.endswith('feed'):
            # Atom格式
            channel = root
        else:
            channel = root
        
        # 提取频道信息
        channel_info = {}
        if channel is not None:
            title = channel.find('title')
            if title is not None:
                channel_info['title'] = title.text
            
            description = channel.find('description')
            if description is not None:
                channel_info['description'] = description.text
            elif channel.find('subtitle') is not None:
                channel_info['description'] = channel.find('subtitle').text
        
        # 提取条目信息
        items = []
        if root.tag.endswith('rss'):
            items = channel.findall('item')
        elif root.tag.endswith('feed'):
            items = root.findall('atom:entry', namespaces)
        else:
            items = root.findall('.//item')
        
        # 分析前3个条目的结构
        sample_items = []
        for i, item in enumerate(items[:3]):
            item_data = {}
            
            # RSS格式
            title = item.find('title')
            if title is not None:
                item_data['title'] = title.text
            
            link = item.find('link')
            if link is not None:
                if link.text:
                    item_data['link'] = link.text
                else:
                    # 处理Atom格式的link
                    link_elem = item.find('atom:link', namespaces)
                    if link_elem is not None:
                        item_data['link'] = link_elem.get('href')
            
            description = item.find('description')
            if description is not None:
                item_data['description'] = description.text[:200] + '...' if description.text and len(description.text) > 200 else description.text
            
            content = item.find('content:encoded', namespaces)
            if content is not None:
                item_data['content'] = content.text[:200] + '...' if content.text and len(content.text) > 200 else content.text
            
            pub_date = item.find('pubDate')
            if pub_date is not None:
                item_data['pub_date'] = pub_date.text
            else:
                published = item.find('atom:published', namespaces)
                if published is not None:
                    item_data['pub_date'] = published.text
            
            guid = item.find('guid')
            if guid is not None:
                item_data['guid'] = guid.text
            
            author = item.find('author')
            if author is not None:
                item_data['author'] = author.text
            elif item.find('dc:creator') is not None:
                item_data['author'] = item.find('dc:creator').text
            
            sample_items.append(item_data)
        
        return {
            'name': name,
            'url': url,
            'format': 'RSS' if root.tag.endswith('rss') else 'Atom',
            'channel_info': channel_info,
            'total_items': len(items),
            'sample_items': sample_items,
            'status': 'success'
        }
        
    except Exception as e:
        return {
            'name': name,
            'url': url,
            'error': str(e),
            'status': 'failed'
        }

def main():
    """主函数"""
    results = []
    
    for name, url in RSS_SOURCES.items():
        logger.info(f"正在分析 {name}: {url}")
        result = analyze_rss_feed(name, url)
        results.append(result)
        
        if result['status'] == 'success':
            logger.info(f"✅ {name}: 成功获取 {result['total_items']} 条记录")
        else:
            logger.error(f"❌ {name}: 失败 - {result['error']}")
    
    # 打印详细报告
    print("\n" + "="*80)
    print("RSS FEED ANALYSIS REPORT")
    print("="*80)
    
    for result in results:
        print(f"\n【{result['name']}】")
        print(f"URL: {result['url']}")
        
        if result['status'] == 'success':
            print(f"状态: ✅ 成功")
            print(f"格式: {result['format']}")
            print(f"频道: {result['channel_info'].get('title', 'N/A')}")
            print(f"总记录数: {result['total_items']}")
            
            for i, item in enumerate(result['sample_items']):
                print(f"\n  样本 {i+1}:")
                print(f"    标题: {item.get('title', 'N/A')}")
                print(f"    链接: {item.get('link', 'N/A')}")
                print(f"    发布: {item.get('pub_date', 'N/A')}")
                print(f"    作者: {item.get('author', 'N/A')}")
                if item.get('description'):
                    print(f"    描述: {item['description'][:100]}...")
        else:
            print(f"状态: ❌ 失败")
            print(f"错误: {result['error']}")
        
        print("-" * 50)

if __name__ == "__main__":
    main()