
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import html2text
from urllib.parse import urljoin

BASE_URL = "https://www.indiehackers.com"
# Set a max number of clicks to prevent infinite loops in case of a bug
MAX_LOAD_MORE_CLICKS = 5 

async def get_html_with_playwright(url: str) -> str | None:
    """Fetches HTML content from a URL, handling 'Load More' buttons."""
    print(f"Fetching {url} with Playwright...")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto(url, wait_until='networkidle', timeout=60000)

            # Wait for the initial content to be present
            await page.wait_for_selector('div.products-product, div.feed-item--post', timeout=30000)

            # Click the "Load More" button until it's no longer visible or max clicks reached
            for i in range(MAX_LOAD_MORE_CLICKS):
                try:
                    load_more_button = page.locator('button:has-text("Load More")')
                    is_visible = await load_more_button.is_visible()
                    if is_visible:
                        print(f"Clicking 'Load More' button... (Attempt {i + 1})")
                        await load_more_button.click()
                        # Wait for the network to be idle, indicating new content has loaded
                        await page.wait_for_load_state('networkidle', timeout=30000)
                    else:
                        print("'Load More' button not visible. Assuming all content is loaded.")
                        break
                except Exception as e:
                    print(f"Could not find or click 'Load More' button: {e}. Proceeding with existing content.")
                    break

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

    products = soup.select('div.products-product')
    print(f"Found {len(products)} product elements to parse.")

    for product in products:
        try:
            title_element = product.select_one('h2.product-card__name a')
            title = title_element.get_text(strip=True) if title_element else 'No Title'
            
            link = title_element['href'] if title_element and title_element.has_attr('href') else ''
            absolute_link = urljoin(BASE_URL, link)

            description_element = product.select_one('p.product-card__description')
            description_html = description_element.decode_contents() if description_element else ''
            description = h.handle(description_html).strip()

            author_element = product.select_one('a.user-link--avatar-and-name')
            author_name = author_element.get_text(strip=True) if author_element else 'N/A'
            
            posts.append({
                'title': title,
                'link': absolute_link,
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

    threads = soup.select('div.feed-item--post')
    print(f"Found {len(threads)} group thread elements to parse.")

    for thread in threads:
        try:
            title_element = thread.select_one('a.feed-item__title-link')
            title = title_element.get_text(strip=True) if title_element else 'No Title'
            
            link = title_element['href'] if title_element and title_element.has_attr('href') else ''
            absolute_link = urljoin(BASE_URL, link)

            summary = ''
            author_element = thread.select_one('a.user-link--avatar-and-name')
            author_name = author_element.get_text(strip=True) if author_element else 'N/A'

            posts.append({
                'title': title,
                'link': absolute_link,
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
