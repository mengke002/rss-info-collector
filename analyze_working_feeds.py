#!/usr/bin/env python3
"""
分析可用的RSS源并设计数据库结构
"""
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import json

# 可用的RSS源
WORKING_FEEDS = {
    'betalist': 'https://feeds.feedburner.com/BetaList',
    'theverge': 'https://www.theverge.com/rss/ai-artificial-intelligence/index.xml',
    'indiehackers_alltime': 'https://ihrss.io/top/all-time',
    'indiehackers_month': 'https://ihrss.io/top/month',
    'indiehackers_week': 'https://ihrss.io/top/week',
    'indiehackers_growth': 'https://ihrss.io/group/growth',
    'indiehackers_developers': 'https://ihrss.io/group/developers',
    'indiehackers_saas': 'https://ihrss.io/group/saas-marketing'
}

def analyze_feed_structure(name, url):
    """详细分析RSS结构"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; RSS-Analyzer/1.0)'}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        
        # 处理命名空间
        namespaces = {
            'atom': 'http://www.w3.org/2005/Atom',
            'content': 'http://purl.org/rss/1.0/modules/content/',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'media': 'http://search.yahoo.com/mrss/'
        }
        
        # 确定格式
        is_rss = root.tag.endswith('rss')
        is_atom = root.tag.endswith('feed')
        
        # 获取频道信息
        channel = root.find('channel') if is_rss else root
        
        # 获取所有条目
        if is_rss:
            items = channel.findall('item')
        elif is_atom:
            items = root.findall('atom:entry', namespaces)
        else:
            items = root.findall('.//item')
        
        # 分析字段
        field_analysis = {}
        if items:
            first_item = items[0]
            
            # 检查所有可能的字段
            possible_fields = [
                'title', 'link', 'description', 'pubDate', 'guid', 'author',
                'dc:creator', 'content:encoded', 'category', 'enclosure',
                'atom:link', 'atom:published', 'atom:updated', 'atom:summary'
            ]
            
            for field in possible_fields:
                if ':' in field:
                    # 命名空间字段
                    ns, tag = field.split(':')
                    elem = first_item.find(f"{ns}:{tag}", namespaces)
                else:
                    elem = first_item.find(field)
                
                if elem is not None:
                    if field == 'atom:link':
                        href = elem.get('href')
                        if href:
                            field_analysis[field] = href
                    else:
                        field_analysis[field] = elem.text[:100] if elem.text else ""
        
        return {
            'name': name,
            'url': url,
            'format': 'RSS' if is_rss else 'Atom' if is_atom else 'Unknown',
            'total_items': len(items),
            'available_fields': list(field_analysis.keys()),
            'sample_data': field_analysis,
            'channel_title': channel.find('title').text if channel.find('title') is not None else None
        }
        
    except Exception as e:
        return {'name': name, 'url': url, 'error': str(e)}

def generate_schema_recommendations(analysis):
    """基于分析结果生成数据库模式建议"""
    schemas = {}
    
    for feed in analysis:
        if 'error' in feed:
            continue
            
        name = feed['name']
        
        # 基础字段设计
        base_schema = {
            'id': 'INT AUTO_INCREMENT PRIMARY KEY',
            'title': 'VARCHAR(255) NOT NULL',
            'link': 'VARCHAR(512) NOT NULL',
            'guid': 'VARCHAR(255) UNIQUE',
            'published_at': 'DATETIME',
            'created_at': 'DATETIME DEFAULT CURRENT_TIMESTAMP',
            'updated_at': 'DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
        }
        
        # 根据可用字段调整
        optional_fields = {}
        
        if 'description' in feed['available_fields'] or 'atom:summary' in feed['available_fields']:
            optional_fields['summary'] = 'VARCHAR(1000)'
            
        if 'content:encoded' in feed['available_fields']:
            optional_fields['content'] = 'TEXT'
            
        if 'author' in feed['available_fields'] or 'dc:creator' in feed['available_fields']:
            optional_fields['author'] = 'VARCHAR(100)'
            
        if 'category' in feed['available_fields']:
            optional_fields['category'] = 'VARCHAR(50)'
        
        # 特殊处理
        special_fields = {}
        if name == 'betalist':
            special_fields['visit_url'] = 'VARCHAR(512)'  # 用于存储添加/vist后的URL
            
        # 合并所有字段
        final_schema = {**base_schema, **optional_fields, **special_fields}
        
        schemas[name] = {
            'table_name': f'rss_{name.replace("_", "_")}',
            'fields': final_schema,
            'indexes': [
                'INDEX idx_published (published_at)',
                'INDEX idx_link (link)',
                'INDEX idx_created (created_at)'
            ]
        }
    
    return schemas

def main():
    """主函数"""
    print("分析RSS源结构...")
    
    analysis_results = []
    for name, url in WORKING_FEEDS.items():
        print(f"分析 {name}...")
        result = analyze_feed_structure(name, url)
        analysis_results.append(result)
    
    # 生成数据库模式
    schemas = generate_schema_recommendations(analysis_results)
    
    # 输出结果
    print("\n" + "="*100)
    print("RSS源结构分析结果")
    print("="*100)
    
    for result in analysis_results:
        print(f"\n【{result['name']}】")
        if 'error' in result:
            print(f"错误: {result['error']}")
            continue
            
        print(f"格式: {result['format']}")
        print(f"频道: {result['channel_title']}")
        print(f"总记录数: {result['total_items']}")
        print(f"可用字段: {', '.join(result['available_fields'])}")
        
        print("样本数据:")
        for key, value in result['sample_data'].items():
            print(f"  {key}: {value}")
    
    print("\n" + "="*100)
    print("数据库模式建议")
    print("="*100)
    
    for name, schema in schemas.items():
        print(f"\n表: {schema['table_name']}")
        print("字段:")
        for field, definition in schema['fields'].items():
            print(f"  {field}: {definition}")
        print("索引:")
        for index in schema['indexes']:
            print(f"  {index}")
    
    # 保存到文件
    with open('rss_analysis_result.json', 'w', encoding='utf-8') as f:
        json.dump({
            'analysis': analysis_results,
            'schemas': schemas
        }, f, ensure_ascii=False, indent=2)
    
    print(f"\n分析结果已保存到 rss_analysis_result.json")

if __name__ == "__main__":
    main()