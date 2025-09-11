import asyncio
from playwright.async_api import async_playwright
from playwright_stealth import stealth
from bs4 import BeautifulSoup
import html2text
from urllib.parse import urljoin

BASE_URL = "https://www.indiehackers.com"
# Set a max number of clicks to prevent infinite loops in case of a bug
MAX_LOAD_MORE_CLICKS = 5 

async def get_html_with_playwright(url: str) -> str | None:
    """Fetches HTML content from a URL, handling 'Load More' buttons and using stealth."""
    print(f"Fetching {url} with Playwright in stealth mode...")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # 使用headless模式以提高性能
            page = await browser.new_page()
            
            # 应用stealth插件
            await stealth(page)
            
            # 访问页面，等待网络空闲以确保JavaScript加载完成
            await page.goto(url, wait_until='networkidle', timeout=90000)
            
            # 处理可能的cookie同意弹窗
            try:
                cookie_accept = page.locator('button:has-text("Accept All")')
                if await cookie_accept.is_visible():
                    print("发现Cookie同意弹窗，点击Accept All...")
                    await cookie_accept.click()
                    await page.wait_for_timeout(3000)
            except Exception as cookie_error:
                print(f"处理Cookie弹窗时出错（可忽略）: {cookie_error}")
            
            # 等待页面JavaScript应用完全加载
            # 对于SPA应用，需要等待更长时间让内容渲染
            print("等待JavaScript应用加载内容...")
            await page.wait_for_timeout(10000)  # 等待10秒让SPA加载
            
            # 尝试不同的产品容器选择器
            product_selectors = [
                'div[class*="product"]',
                'div[data-testid*="product"]', 
                'article',
                'div[class*="feed-item"]',
                'div[class*="card"]',
                'li[class*="product"]',
                '.ember-view div',  # Ember应用的通用选择器
            ]
            
            products_found = False
            for selector in product_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=5000)
                    count = await page.locator(selector).count()
                    if count > 0:
                        print(f"✅ 找到 {count} 个产品元素使用选择器: {selector}")
                        products_found = True
                        break
                except:
                    continue
            
            if not products_found:
                print("❌ 未找到产品元素，尝试等待更长时间...")
                await page.wait_for_timeout(5000)
                
            # 滚动页面以确保所有内容加载
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(3000)

            # 点击"Load More"按钮直到没有更多内容
            load_more_selectors = [
                'button:has-text("Load more")',
                'button:has-text("Show more")', 
                'button[class*="load"]',
                'a:has-text("Load more")',
                'div:has-text("Load more")',
                '.load-more',
                '[data-testid*="load"]'
            ]
            
            # 首先找到有效的Load More选择器
            active_selector = None
            for selector in load_more_selectors:
                try:
                    load_more_button = page.locator(selector)
                    if await load_more_button.is_visible():
                        active_selector = selector
                        print(f"找到Load More按钮，使用选择器: {selector}")
                        break
                except:
                    continue
            
            if active_selector:
                for i in range(MAX_LOAD_MORE_CLICKS):
                    try:
                        load_more_button = page.locator(active_selector)
                        if await load_more_button.is_visible():
                            print(f"点击 'Load More' 按钮... (尝试 {i + 1})")
                            await load_more_button.click()
                            await page.wait_for_load_state('networkidle', timeout=15000)
                            
                            # 等待内容加载并检查产品数量变化
                            await page.wait_for_timeout(2000)
                            current_html = await page.content()
                            from bs4 import BeautifulSoup
                            soup_check = BeautifulSoup(current_html, 'lxml')
                            current_top_products = soup_check.select('li.top-product')
                            print(f"当前顶部产品数量: {len(current_top_products)}")
                        else:
                            print("Load More按钮不再可见，停止点击")
                            break
                    except Exception as e:
                        print(f"点击Load More时出错: {e}")
                        break
            else:
                print("未找到Load More按钮")

            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        print(f"Error fetching {url} with Playwright: {e}")
        return None

def parse_products(html: str | None) -> list:
    """Parses the HTML of the Indie Hackers products page."""
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'lxml')
    posts = []
    h = html2text.HTML2Text()
    h.ignore_links = True

    # 根据实际HTML结构，使用正确的选择器
    # 1. 产品卡片 - div.product-card
    # 2. 顶部产品 - li.top-product
    product_selectors = [
        'div.product-card',  # 主要的产品卡片
        'li.top-product',    # 顶部产品列表
    ]
    
    all_products = []
    for selector in product_selectors:
        products = soup.select(selector)
        if products:
            print(f"使用选择器找到 {len(products)} 个产品元素: {selector}")
            all_products.extend(products)
    
    if not all_products:
        print("未找到任何产品元素...")
        return []

    print(f"总共找到 {len(all_products)} 个产品元素")

    for product in all_products:
        try:
            # 根据实际HTML结构解析标题
            title_element = None
            link = ''
            
            # 对于产品卡片 (div.product-card)
            if 'product-card' in product.get('class', []):
                # 标题在 span.product-card__name 中
                title_element = product.select_one('span.product-card__name')
                # 链接在 a.product-card__link 中
                link_element = product.select_one('a.product-card__link')
                if link_element and link_element.has_attr('href'):
                    link = urljoin(BASE_URL, link_element['href'])
                    
                # 描述在 span.product-card__tagline 中
                description_element = product.select_one('span.product-card__tagline')
                description = description_element.get_text(strip=True) if description_element else ''
                
                # 收入信息
                revenue_element = product.select_one('span.product-card__revenue-number')
                revenue_text = revenue_element.get_text(strip=True) if revenue_element else ''
                if revenue_text:
                    description = f"{description} (Revenue: {revenue_text})"
            
            # 对于顶部产品 (li.top-product)
            elif 'top-product' in product.get('class', []):
                # 链接和标题都在 a.top-product__link 中
                link_element = product.select_one('a.top-product__link')
                if link_element:
                    if link_element.has_attr('href'):
                        link = urljoin(BASE_URL, link_element['href'])
                    # 标题是链接的文本内容，但需要处理格式
                    title_text = link_element.get_text(strip=True)
                    # 移除数字前缀（如 "1AiDD..." -> "AiDD..."）
                    import re
                    title_match = re.match(r'^(\d+)(.+)', title_text)
                    if title_match:
                        title_text = title_match.group(2)
                    title_element = type('MockElement', (), {'get_text': lambda self, strip=False: title_text})()
                
                description = ''  # 顶部产品通常没有详细描述
            
            title = title_element.get_text(strip=True) if title_element else 'No Title'
            
            # 作者信息（Indie Hackers页面通常不显示单个作者）
            author_name = 'Indie Hackers'
            
            # 只有在找到有效标题时才添加到结果中
            if title and title != 'No Title' and len(title) > 1:
                posts.append({
                    'title': title,
                    'link': link,
                    'summary': description,
                    'author': author_name,
                    'published': None,
                    'source': 'Indie Hackers Products (Scraped)'
                })
        except Exception as e:
            print(f"Error parsing a product item: {e}")
            continue
            
    return posts

def parse_groups(html: str | None) -> list:
    """Parses the HTML of an Indie Hackers group page."""
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    posts = []
    h = html2text.HTML2Text()
    h.ignore_links = True

    # 尝试多种可能的群组选择器，适应SPA应用
    thread_selectors = [
        'div.feed-item--post',  # 原始选择器
        'div[class*="feed-item"]',  # 包含feed-item的class
        'article',  # 文章元素
        'div[class*="post"]',  # 包含post的class
        'li[class*="post"]',  # 列表项post
        'div[class*="thread"]',  # 包含thread的class
        'div.ember-view div',  # Ember应用的嵌套结构
        'div[data-testid*="post"]'  # testid相关
    ]
    
    threads = []
    for selector in thread_selectors:
        threads = soup.select(selector)
        if threads:
            print(f"使用选择器找到 {len(threads)} 个群组讨论元素: {selector}")
            break
    
    if not threads:
        print("未找到任何群组讨论元素...")
        return []

    for thread in threads:
        try:
            # 尝试多种标题选择器
            title_selectors = [
                'a.feed-item__title-link',  # 原始选择器
                'a[class*="title"]',  # 包含title的链接
                'h2 a', 'h3 a', 'h4 a',  # 各种标题级别的链接
                '.title a',  # title class下的链接
                'div[class*="title"] a',  # title div下的链接
                'a[class*="feed-item"]',  # feed-item相关链接
                'a[href*="/post/"]',  # 指向post的链接
                'a[href*="/thread/"]'  # 指向thread的链接
            ]
            
            title_element = None
            for selector in title_selectors:
                title_element = thread.select_one(selector)
                if title_element:
                    break
                    
            title = title_element.get_text(strip=True) if title_element else 'No Title'
            
            link = ''
            if title_element and title_element.has_attr('href'):
                link = urljoin(BASE_URL, title_element['href'])

            # 群组页面通常没有详细摘要，尝试获取简单描述
            summary = ''
            summary_selectors = [
                'p[class*="description"]',
                'div[class*="summary"]',
                '.content p',
                'p'
            ]
            
            for selector in summary_selectors:
                summary_element = thread.select_one(selector)
                if summary_element:
                    summary = summary_element.get_text(strip=True)[:200]  # 限制长度
                    break

            # 尝试多种作者选择器
            author_selectors = [
                'a.user-link--avatar-and-name',  # 原始选择器
                'a[class*="user"]',  # 包含user的链接
                '.user a',  # user class下的链接
                '.author a',  # author class下的链接
                'a[class*="author"]',  # 包含author的链接
                'div[class*="user"] a',  # user div下的链接
                'span[class*="user"]',  # user span
                'div[class*="author"]'  # author div
            ]
            
            author_element = None
            for selector in author_selectors:
                author_element = thread.select_one(selector)
                if author_element:
                    break
                    
            author_name = author_element.get_text(strip=True) if author_element else 'N/A'

            # 只有在找到标题时才添加到结果中
            if title and title != 'No Title':
                posts.append({
                    'title': title,
                    'link': link,
                    'summary': summary,
                    'author': author_name,
                    'published': None,
                    'source': 'Indie Hackers Groups (Scraped)'
                })
        except Exception as e:
            print(f"Error parsing a group item: {e}")
            continue
            
    return posts

async def scrape_products(period: str) -> list:
    """Scrapes Indie Hackers products based on a time period."""
    if period == 'today':
        period_path = 'day'
        url = f"{BASE_URL}/products?period={period_path}"
    elif period == 'all-time':
        url = f"{BASE_URL}/products"
    else:
        url = f"{BASE_URL}/products?period={period}"
        
    print(f"Scraping Indie Hackers products for period: {period} from {url}")
    html = await get_html_with_playwright(url)
    return parse_products(html)

async def scrape_group(group_name: str) -> list:
    """Scrapes an Indie Hackers group."""
    if group_name == 'saas-marketing':
        group_name = 'saas'
        
    url = f"{BASE_URL}/group/{group_name}"
    print(f"Scraping Indie Hackers group: {group_name} from {url}")
    html = await get_html_with_playwright(url)
    return parse_groups(html)

async def main():
    """Main function for testing the scraper."""
    print("--- Testing Product Scraping (week) ---")
    weekly_products = await scrape_products('week')
    if weekly_products:
        print(f"Found {len(weekly_products)} products.")
        print(f"First product: {weekly_products[0]}")
    else:
        print("No products found or error occurred.")

    print("\n--- Testing Group Scraping (developers) ---")
    dev_posts = await scrape_group('developers')
    if dev_posts:
        print(f"Found {len(dev_posts)} posts.")
        print(f"First post: {dev_posts[0]}")
    else:
        print("No posts found or error occurred.")

if __name__ == '__main__':
    asyncio.run(main())