"""
Notion API 客户端
用于将 RSS 收集系统的报告推送到 Notion 页面
支持多种报告类型：产品发现、科技新闻、社区洞察
"""
import logging
import requests
import json
import re
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from .config import config


class NotionClient:
    """Notion API 客户端，适配 RSS 收集系统的多种报告类型"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://api.notion.com/v1"
        self.version = "2022-06-28"

        # 从配置获取Notion设置
        notion_config = config.get_notion_config()
        self.integration_token = notion_config.get('integration_token')
        self.parent_page_id = notion_config.get('parent_page_id')

        if not self.integration_token:
            self.logger.warning("Notion集成token未配置")
        if not self.parent_page_id:
            self.logger.warning("Notion父页面ID未配置")

    def _get_headers(self) -> Dict[str, str]:
        """获取API请求头"""
        return {
            "Authorization": f"Bearer {self.integration_token}",
            "Content-Type": "application/json",
            "Notion-Version": self.version
        }

    def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict[str, Any]:
        """发送API请求"""
        url = f"{self.base_url}/{endpoint}"
        headers = self._get_headers()

        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, timeout=30)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, json=data, timeout=30)
            elif method.upper() == "PATCH":
                response = requests.patch(url, headers=headers, json=data, timeout=30)
            else:
                raise ValueError(f"不支持的HTTP方法: {method}")

            response.raise_for_status()
            return {"success": True, "data": response.json()}

        except requests.exceptions.RequestException as e:
            error_msg = str(e)

            # 尝试获取更详细的错误信息
            try:
                if hasattr(e, 'response') and e.response is not None:
                    error_detail = e.response.json()
                    if 'message' in error_detail:
                        error_msg = f"{e}: {error_detail['message']}"
                    elif 'error' in error_detail:
                        error_msg = f"{e}: {error_detail['error']}"
            except:
                pass

            self.logger.error(f"Notion API请求失败: {error_msg}")
            return {"success": False, "error": error_msg}

    def get_page_children(self, page_id: str) -> Dict[str, Any]:
        """获取页面的子页面"""
        return self._make_request("GET", f"blocks/{page_id}/children")

    def create_page(self, parent_id: str, title: str, content_blocks: List[Dict] = None) -> Dict[str, Any]:
        """创建新页面"""
        data = {
            "parent": {"page_id": parent_id},
            "properties": {
                "title": {
                    "title": [
                        {
                            "text": {
                                "content": title
                            }
                        }
                    ]
                }
            }
        }

        if content_blocks:
            data["children"] = content_blocks

        return self._make_request("POST", "pages", data)

    def find_or_create_year_page(self, year: str) -> Optional[str]:
        """查找或创建年份页面"""
        try:
            # 获取父页面的子页面
            children_result = self.get_page_children(self.parent_page_id)
            if not children_result.get("success"):
                self.logger.error(f"获取父页面子页面失败: {children_result.get('error')}")
                return None

            # 查找年份页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == year:
                        return child["id"]

            # 创建年份页面
            self.logger.info(f"创建年份页面: {year}")
            create_result = self.create_page(self.parent_page_id, year)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                self.logger.error(f"创建年份页面失败: {create_result.get('error')}")
                return None

        except Exception as e:
            self.logger.error(f"查找或创建年份页面时出错: {e}")
            return None

    def find_or_create_month_page(self, year_page_id: str, month: str) -> Optional[str]:
        """查找或创建月份页面"""
        try:
            # 获取年份页面的子页面
            children_result = self.get_page_children(year_page_id)
            if not children_result.get("success"):
                self.logger.error(f"获取年份页面子页面失败: {children_result.get('error')}")
                return None

            # 查找月份页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == month:
                        return child["id"]

            # 创建月份页面
            self.logger.info(f"创建月份页面: {month}")
            create_result = self.create_page(year_page_id, month)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                self.logger.error(f"创建月份页面失败: {create_result.get('error')}")
                return None

        except Exception as e:
            self.logger.error(f"查找或创建月份页面时出错: {e}")
            return None

    def find_or_create_day_page(self, month_page_id: str, day: str) -> Optional[str]:
        """查找或创建日期页面"""
        try:
            # 获取月份页面的子页面
            children_result = self.get_page_children(month_page_id)
            if not children_result.get("success"):
                self.logger.error(f"获取月份页面子页面失败: {children_result.get('error')}")
                return None

            # 查找日期页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == day:
                        return child["id"]

            # 创建日期页面
            self.logger.info(f"创建日期页面: {day}")
            create_result = self.create_page(month_page_id, day)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                self.logger.error(f"创建日期页面失败: {create_result.get('error')}")
                return None

        except Exception as e:
            self.logger.error(f"查找或创建日期页面时出错: {e}")
            return None

    def check_report_exists(self, day_page_id: str, report_title: str) -> Optional[Dict[str, Any]]:
        """检查报告是否已经存在"""
        try:
            # 获取日期页面的子页面
            children_result = self.get_page_children(day_page_id)
            if not children_result.get("success"):
                return None

            # 查找同名报告
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == report_title:
                        page_id = child["id"]
                        page_url = f"https://www.notion.so/{page_id.replace('-', '')}"
                        return {
                            "exists": True,
                            "page_id": page_id,
                            "page_url": page_url
                        }

            return {"exists": False}

        except Exception as e:
            self.logger.error(f"检查报告是否存在时出错: {e}")
            return None

    def _extract_page_title(self, page_data: Dict) -> str:
        """从页面数据中提取标题"""
        try:
            if page_data.get("type") == "child_page":
                title_data = page_data.get("child_page", {}).get("title", "")
                return title_data
            return ""
        except Exception:
            return ""

    def _parse_rich_text(self, text: str) -> List[Dict]:
        """解析文本中的Markdown格式，支持链接、粗体等"""
        import re

        # 检查是否包含Source引用 - RSS报告中可能没有这种格式
        source_pattern = r'\[Sources?:\s*([T\d\s,]+)\]'
        source_matches = list(re.finditer(source_pattern, text))

        if not source_matches:
            # 没有Source引用，直接处理链接和格式
            return self._parse_links_and_formatting(text)

        # 有Source引用，需要分段处理
        rich_text = []
        last_end = 0

        for match in source_matches:
            # 添加Source引用前的普通文本
            if match.start() > last_end:
                before_text = text[last_end:match.start()]
                if before_text:
                    rich_text.extend(self._parse_links_and_formatting(before_text))

            # 添加Source引用（带特殊格式和提示）
            source_text = match.group(0)  # 完整的 [Source: T1] 文本
            rich_text.append({
                "type": "text",
                "text": {"content": f"📎 {source_text}"},
                "annotations": {
                    "italic": True,
                    "color": "blue",
                    "bold": False
                }
            })

            last_end = match.end()

        # 添加剩余的普通文本
        if last_end < len(text):
            remaining_text = text[last_end:]
            if remaining_text:
                rich_text.extend(self._parse_links_and_formatting(remaining_text))

        return rich_text

    def _parse_links_and_formatting(self, text: str) -> List[Dict]:
        """解析链接和格式，不包括Source引用"""
        import re

        rich_text = []

        # 支持标准的Markdown链接格式
        link_pattern = r'\[([^\]]+)\]\((https?://[^)]+)\)'

        last_end = 0
        for match in re.finditer(link_pattern, text):
            # 添加链接前的普通文本
            if match.start() > last_end:
                before_text = text[last_end:match.start()]
                if before_text:
                    rich_text.extend(self._parse_text_formatting(before_text))

            # 添加链接
            link_text = match.group(1)
            link_url = match.group(2)
            rich_text.append({
                "type": "text",
                "text": {
                    "content": link_text,
                    "link": {"url": link_url}
                }
            })

            last_end = match.end()

        # 添加剩余的普通文本
        if last_end < len(text):
            remaining_text = text[last_end:]
            if remaining_text:
                rich_text.extend(self._parse_text_formatting(remaining_text))

        # 如果没有找到任何链接，处理整个文本
        if not rich_text:
            rich_text = self._parse_text_formatting(text)

        return rich_text

    def _parse_text_formatting(self, text: str) -> List[Dict]:
        """解析文本格式（粗体、斜体等）"""
        import re

        # 按优先级处理格式：粗体 -> 斜体 -> 普通文本
        # 使用更复杂的解析来支持嵌套格式

        # 创建格式化片段列表 [(start, end, format_type, content)]
        format_segments = []

        # 查找粗体 **text**
        bold_pattern = r'\*\*([^*]+)\*\*'
        for match in re.finditer(bold_pattern, text):
            format_segments.append((match.start(), match.end(), 'bold', match.group(1)))

        # 查找斜体 *text* (但要避免与粗体冲突)
        italic_pattern = r'(?<!\*)\*([^*]+)\*(?!\*)'
        for match in re.finditer(italic_pattern, text):
            # 检查是否与已有的粗体格式重叠
            overlaps = any(
                match.start() >= seg[0] and match.end() <= seg[1]
                for seg in format_segments if seg[2] == 'bold'
            )
            if not overlaps:
                format_segments.append((match.start(), match.end(), 'italic', match.group(1)))

        # 按位置排序
        format_segments.sort(key=lambda x: x[0])

        # 构建rich_text
        rich_text = []
        last_end = 0

        for start, end, format_type, content in format_segments:
            # 添加格式前的普通文本
            if start > last_end:
                before_text = text[last_end:start]
                if before_text:
                    rich_text.append({
                        "type": "text",
                        "text": {"content": before_text}
                    })

            # 添加格式化文本
            annotations = {}
            if format_type == 'bold':
                annotations["bold"] = True
            elif format_type == 'italic':
                annotations["italic"] = True

            rich_text.append({
                "type": "text",
                "text": {"content": content},
                "annotations": annotations
            })

            last_end = end

        # 添加剩余的普通文本
        if last_end < len(text):
            remaining_text = text[last_end:]
            if remaining_text:
                rich_text.append({
                    "type": "text",
                    "text": {"content": remaining_text}
                })

        # 如果没有找到任何格式，返回普通文本
        if not rich_text:
            rich_text = [{
                "type": "text",
                "text": {"content": text}
            }]

        return rich_text

    def markdown_to_notion_blocks(self, markdown_content: str) -> tuple[List[Dict], List[Dict]]:
        """将Markdown内容转换为Notion块，支持链接和格式"""
        blocks = []
        tables_to_add = []  # 用于跟踪需要添加的表格
        lines = markdown_content.split('\n')

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            if not line:
                i += 1
                continue

            try:
                # 标题处理
                if line.startswith('# '):
                    blocks.append({
                        "object": "block",
                        "type": "heading_1",
                        "heading_1": {
                            "rich_text": self._parse_rich_text(line[2:])
                        }
                    })
                elif line.startswith('## '):
                    blocks.append({
                        "object": "block",
                        "type": "heading_2",
                        "heading_2": {
                            "rich_text": self._parse_rich_text(line[3:])
                        }
                    })
                elif line.startswith('### '):
                    blocks.append({
                        "object": "block",
                        "type": "heading_3",
                        "heading_3": {
                            "rich_text": self._parse_rich_text(line[4:])
                        }
                    })
                # 分割线
                elif line.startswith('---'):
                    blocks.append({
                        "object": "block",
                        "type": "divider",
                        "divider": {}
                    })
                # 列表项 - 支持多层嵌套
                elif line.startswith(('- ', '* ')) or (line.startswith(' ') and line.lstrip().startswith(('- ', '* '))):
                    # 计算缩进级别
                    indent_level = 0
                    stripped_line = line.lstrip()

                    # 计算前导空格数来确定层级
                    leading_spaces = len(line) - len(stripped_line)
                    if leading_spaces > 0:
                        indent_level = min(leading_spaces // 2, 2)  # Notion最多支持3级嵌套(0,1,2)

                    # 移除列表标记
                    list_content = stripped_line[2:]  # 移除 '- ' 或 '* '

                    if indent_level == 0:
                        # 顶级列表项
                        blocks.append({
                            "object": "block",
                            "type": "bulleted_list_item",
                            "bulleted_list_item": {
                                "rich_text": self._parse_rich_text(list_content)
                            }
                        })
                    else:
                        # 嵌套列表项 - 通过children实现
                        # 但由于Notion API的限制，我们先转换为包含缩进标记的普通列表项
                        indent_marker = "  " * indent_level + "• "
                        formatted_content = indent_marker + list_content

                        blocks.append({
                            "object": "block",
                            "type": "bulleted_list_item",
                            "bulleted_list_item": {
                                "rich_text": self._parse_rich_text(formatted_content)
                            }
                        })
                # 表格处理 - RSS报告中有表格内容
                elif '|' in line and line.count('|') >= 2:
                    # 收集完整的表格
                    table_lines = []
                    table_start = i

                    # 收集所有表格行
                    while i < len(lines):
                        current_line = lines[i].strip()
                        if '|' in current_line and current_line.count('|') >= 2:
                            table_lines.append(current_line)
                        elif current_line == '':
                            # 空行，继续收集
                            pass
                        else:
                            # 非表格行，退出
                            break
                        i += 1

                    # 回退一行，因为外层循环会自增
                    i -= 1

                    # 处理收集到的表格
                    if table_lines:
                        self._process_table_to_blocks(table_lines, blocks, tables_to_add)

                    continue
                # 普通段落
                else:
                    # 处理可能的多行段落
                    paragraph_lines = [line]
                    j = i + 1
                    while j < len(lines) and lines[j].strip() and not lines[j].startswith(('#', '---')) and not (lines[j].startswith(('- ', '* ')) or (lines[j].startswith(' ') and lines[j].lstrip().startswith(('- ', '* ')))) and '|' not in lines[j]:
                        paragraph_lines.append(lines[j].strip())
                        j += 1

                    paragraph_text = ' '.join(paragraph_lines)
                    if paragraph_text:
                        blocks.append({
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": self._parse_rich_text(paragraph_text)
                            }
                        })

                    i = j - 1

            except Exception as e:
                # 如果解析失败，添加为普通文本
                self.logger.warning(f"解析Markdown行失败，使用普通文本: {line[:50]}... 错误: {e}")
                blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": line}}]
                    }
                })

            i += 1

        return blocks, tables_to_add

    def _process_table_to_blocks(self, table_lines: List[str], blocks: List[Dict], tables_to_add: List[Dict]):
        """将表格行转换为 Notion 真实表格"""
        if not table_lines:
            return

        # 解析表格数据
        table_rows = []
        headers = None

        for line in table_lines:
            # 清理表格行
            cleaned_line = line.strip()
            if not cleaned_line:
                continue

            # 跳过分隔行 (如 |---|---|---|)
            # 检查是否所有单元格都只包含 - 字符
            cells_check = [cell.strip() for cell in cleaned_line.split('|')[1:-1]]
            is_separator = True
            for cell in cells_check:
                if cell and not all(c in '-: ' for c in cell):
                    is_separator = False
                    break

            if is_separator and cells_check:
                continue

            # 分割单元格
            cells = [cell.strip() for cell in cleaned_line.split('|')[1:-1]]  # 去掉首尾空元素

            if cells and any(cell for cell in cells):  # 确保不是空行
                if headers is None:
                    headers = cells
                else:
                    table_rows.append(cells)

        # 如果没有有效数据，跳过
        if not headers or not table_rows:
            return

        # 确定表格宽度
        table_width = len(headers)

        # 对于大表格（>99行），分块处理（Notion API限制每个表格最多100行包括标题）
        if len(table_rows) > 99:
            self.logger.info(f"表格行数({len(table_rows)})超过Notion限制，将分块显示")
            self._create_chunked_tables(headers, table_rows, blocks, 99, tables_to_add)
        else:
            # 创建单个表格
            self._create_single_notion_table(headers, table_rows, blocks, tables_to_add)

    def _create_single_notion_table(self, headers: List[str], table_rows: List[List[str]], blocks: List[Dict], tables_to_add: List[Dict]):
        """创建单个Notion原生表格，使用分步方法

        1. 先记录表格信息，稍后通过API添加
        2. 目前先添加占位符
        """
        try:
            self.logger.info(f"准备创建Notion真实表格（{len(table_rows)}行数据）")

            # 添加表格占位符
            table_placeholder = {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{
                        "type": "text",
                        "text": {"content": f"📊 表格数据（{len(table_rows)}行，{len(headers)}列）"},
                        "annotations": {"bold": True, "color": "blue"}
                    }]
                }
            }
            blocks.append(table_placeholder)

            # 记录表格信息到单独的列表中
            tables_to_add.append({
                "headers": headers,
                "rows": table_rows,
                "placeholder_index": len(blocks) - 1  # 记录占位符在blocks中的位置
            })

        except Exception as e:
            self.logger.error(f"准备表格创建失败: {e}")
            # 回退到代码块方案
            self.logger.info("回退到代码块方案")
            self._create_table_as_code_block(headers, table_rows, blocks)

    def _create_table_as_code_block(self, headers: List[str], table_rows: List[List[str]], blocks: List[Dict]):
        """将表格转换为代码块显示（回退方案）"""
        try:
            # 重建表格文本
            table_text = ""

            # 添加标题行
            header_line = "| " + " | ".join(headers) + " |"
            separator_line = "|" + "|".join(["-" * (len(h) + 2) for h in headers]) + "|"
            table_text += header_line + "\n" + separator_line + "\n"

            # 添加数据行
            for row in table_rows:
                # 确保行有足够的单元格
                while len(row) < len(headers):
                    row.append("")

                # 限制单元格长度
                display_row = []
                for cell in row[:len(headers)]:
                    cell_content = cell or ""
                    if len(cell_content) > 100:  # 代码块中长度限制
                        cell_content = cell_content[:97] + "..."
                    display_row.append(cell_content)

                row_line = "| " + " | ".join(display_row) + " |"
                table_text += row_line + "\n"

            # 添加代码块
            blocks.append({
                "object": "block",
                "type": "code",
                "code": {
                    "caption": [],
                    "rich_text": [{
                        "type": "text",
                        "text": {"content": table_text}
                    }],
                    "language": "markdown"
                }
            })

            # 添加说明
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{
                        "type": "text",
                        "text": {"content": f"📋 表格包含 {len(table_rows)} 行数据（代码块格式）"},
                        "annotations": {"italic": True, "color": "gray"}
                    }]
                }
            })

        except Exception as e:
            self.logger.error(f"创建表格代码块失败: {e}")
            # 最终回退：添加错误提示
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{
                        "type": "text",
                        "text": {"content": "⚠️ 表格数据处理失败，请查看原始报告"},
                        "annotations": {"italic": True, "color": "red"}
                    }]
                }
            })

    def _create_chunked_tables(self, headers: List[str], table_rows: List[List[str]], blocks: List[Dict], chunk_size: int, tables_to_add: List[Dict]):
        """将大表格分成多个小表格显示"""
        total_rows = len(table_rows)
        chunks = [table_rows[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]

        # 添加说明
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{
                    "type": "text",
                    "text": {"content": f"📊 表格包含 {total_rows} 行，分为 {len(chunks)} 个部分显示："},
                    "annotations": {"bold": True}
                }]
            }
        })

        for chunk_idx, chunk in enumerate(chunks):
            # 为每个分块添加标题
            blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {
                    "rich_text": [{
                        "type": "text",
                        "text": {"content": f"第 {chunk_idx + 1} 部分 (第 {chunk_idx * chunk_size + 1}-{min((chunk_idx + 1) * chunk_size, total_rows)} 行)"}
                    }]
                }
            })

            # 创建这个分块的表格
            self._create_single_notion_table(headers, chunk, blocks, tables_to_add)

    def _parse_table_cell_content(self, cell_content: str) -> List[Dict]:
        """解析表格单元格内容，支持链接和格式"""
        if not cell_content:
            return [{"type": "text", "text": {"content": ""}}]

        # 检查是否包含Markdown链接
        import re
        link_pattern = r'\[([^\]]+)\]\((https?://[^)]+)\)'

        rich_text = []
        last_end = 0

        for match in re.finditer(link_pattern, cell_content):
            # 添加链接前的普通文本
            if match.start() > last_end:
                before_text = cell_content[last_end:match.start()]
                if before_text:
                    rich_text.append({
                        "type": "text",
                        "text": {"content": before_text}
                    })

            # 添加链接
            link_text = match.group(1)
            link_url = match.group(2)
            rich_text.append({
                "type": "text",
                "text": {
                    "content": link_text,
                    "link": {"url": link_url}
                }
            })

            last_end = match.end()

        # 添加剩余的普通文本
        if last_end < len(cell_content):
            remaining_text = cell_content[last_end:]
            if remaining_text:
                rich_text.append({
                    "type": "text",
                    "text": {"content": remaining_text}
                })

        # 如果没有找到任何链接，返回普通文本
        if not rich_text:
            rich_text = [{
                "type": "text",
                "text": {"content": cell_content}
            }]

        return rich_text

    def _create_large_content_page(self, parent_page_id: str, page_title: str,
                                  content_blocks: List[Dict]) -> Dict[str, Any]:
        """创建大内容页面，分批添加内容块"""
        try:
            self.logger.info(f"创建大内容页面，总共 {len(content_blocks)} 个块，需要分批处理")

            # 第一步：创建空页面，只包含前100个块
            initial_blocks = content_blocks[:100]
            create_result = self.create_page(parent_page_id, page_title, initial_blocks)

            if not create_result.get("success"):
                return create_result

            page_id = create_result["data"]["id"]
            self.logger.info(f"页面创建成功，开始添加剩余 {len(content_blocks) - 100} 个块")

            # 第二步：分批添加剩余的块
            remaining_blocks = content_blocks[100:]
            batch_size = 100

            for i in range(0, len(remaining_blocks), batch_size):
                batch = remaining_blocks[i:i + batch_size]
                batch_num = (i // batch_size) + 2

                self.logger.info(f"添加第 {batch_num} 批内容: {len(batch)} 个块")

                # 使用 PATCH 方法添加子块
                append_result = self._append_blocks_to_page(page_id, batch)

                if not append_result.get("success"):
                    self.logger.warning(f"第 {batch_num} 批内容添加失败: {append_result.get('error')}")
                    # 继续尝试添加其他批次
                else:
                    self.logger.info(f"第 {batch_num} 批内容添加成功")

                # 添加延迟避免API限制
                import time
                time.sleep(0.5)

            page_url = f"https://www.notion.so/{page_id.replace('-', '')}"
            return {
                "success": True,
                "data": {"id": page_id},
                "page_url": page_url,
                "total_blocks": len(content_blocks)
            }

        except Exception as e:
            self.logger.error(f"创建大内容页面时出错: {e}")
            return {"success": False, "error": str(e)}

    def _append_blocks_to_page(self, page_id: str, blocks: List[Dict]) -> Dict[str, Any]:
        """向页面追加内容块"""
        try:
            data = {
                "children": blocks
            }

            return self._make_request("PATCH", f"blocks/{page_id}/children", data)

        except Exception as e:
            self.logger.error(f"追加内容块时出错: {e}")
            return {"success": False, "error": str(e)}

    def _add_real_table_to_page(self, page_id: str, headers: List[str], table_rows: List[List[str]]) -> bool:
        """向已创建的页面添加真实表格

        Args:
            page_id: 页面ID
            headers: 表格标题行
            table_rows: 表格数据行

        Returns:
            是否成功添加表格
        """
        try:
            import requests
            import json

            # 限制表格大小，避免API请求过大（Notion限制表格最多100行包括标题）
            max_rows = 99  # 99行数据 + 1行标题 = 100行总计
            if len(table_rows) > max_rows:
                self.logger.info(f"表格行数({len(table_rows)})超过Notion限制({max_rows})，只添加前{max_rows}行")
                table_rows = table_rows[:max_rows]

            # 构建表格行数据
            table_children = []

            # 添加标题行
            header_cells = []
            for header in headers:
                # 标题行也支持链接解析（虽然不太常见）
                header_rich_text = self._parse_table_cell_content(header or "")
                header_cells.append(header_rich_text)

            table_children.append({
                "type": "table_row",
                "table_row": {
                    "cells": header_cells
                }
            })

            # 添加数据行
            for row in table_rows:
                # 确保行有足够的单元格
                while len(row) < len(headers):
                    row.append("")

                # 准备单元格数据
                row_cells = []
                for cell in row[:len(headers)]:  # 确保不超过表格宽度
                    # 处理空值和长内容
                    cell_content = cell or ""
                    if len(cell_content) > 200:  # 限制单元格内容长度
                        cell_content = cell_content[:197] + "..."

                    # 解析单元格内容，支持链接
                    cell_rich_text = self._parse_table_cell_content(cell_content)
                    row_cells.append(cell_rich_text)

                table_children.append({
                    "type": "table_row",
                    "table_row": {
                        "cells": row_cells
                    }
                })

            # 构建API请求
            url = f"{self.base_url}/blocks/{page_id}/children"
            headers_req = self._get_headers()

            table_block = {
                "children": [
                    {
                        "object": "block",
                        "type": "table",
                        "table": {
                            "table_width": len(headers),
                            "has_column_header": True,
                            "has_row_header": False,
                            "children": table_children
                        }
                    }
                ]
            }

            # 发送PATCH请求
            response = requests.patch(url, headers=headers_req, data=json.dumps(table_block), timeout=30)
            response.raise_for_status()

            self.logger.info(f"真实表格添加成功 ({len(table_rows)}行数据)")
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"添加真实表格失败: {e}")
            try:
                if hasattr(e, 'response') and e.response is not None:
                    error_detail = e.response.json()
                    self.logger.error(f"API错误详情: {error_detail}")
            except:
                pass
            return False
        except Exception as e:
            self.logger.error(f"添加真实表格时出现异常: {e}")
            return False

    def _extract_report_date_and_type(self, report_title: str, report_content: str) -> tuple[datetime, str]:
        """从报告标题和内容中提取报告日期和类型"""
        # 根据标题判断报告类型并提取日期
        report_type = "未知类型"
        report_date = datetime.now(timezone.utc) + timedelta(hours=8)  # 默认为当前北京时间

        # 产品发现报告
        if "产品发现" in report_title:
            report_type = "产品发现"
            # 提取日期，格式如: 产品发现周报 (2025-09-01)
            date_match = re.search(r'\((\d{4}-\d{2}-\d{2})\)', report_title)
            if date_match:
                try:
                    report_date = datetime.strptime(date_match.group(1), '%Y-%m-%d')
                    report_date = report_date.replace(tzinfo=timezone(timedelta(hours=8)))
                except ValueError:
                    pass

        # 科技新闻报告
        elif "科技新闻" in report_title:
            report_type = "科技新闻"
            # 提取日期，格式如: 科技新闻洞察报告 (2025-09-01)
            date_match = re.search(r'\((\d{4}-\d{2}-\d{2})\)', report_title)
            if date_match:
                try:
                    report_date = datetime.strptime(date_match.group(1), '%Y-%m-%d')
                    report_date = report_date.replace(tzinfo=timezone(timedelta(hours=8)))
                except ValueError:
                    pass

        # 独立开发者社区报告
        elif "独立开发者" in report_title or "社区洞察" in report_title:
            report_type = "社区洞察"
            # 提取日期范围，格式如: 独立开发者社区洞察周报 (2025-08-30 - 2025-09-01)
            date_range_match = re.search(r'\((\d{4}-\d{2}-\d{2})\s*-\s*(\d{4}-\d{2}-\d{2})\)', report_title)
            if date_range_match:
                try:
                    # 使用结束日期作为报告日期
                    end_date = date_range_match.group(2)
                    report_date = datetime.strptime(end_date, '%Y-%m-%d')
                    report_date = report_date.replace(tzinfo=timezone(timedelta(hours=8)))
                except ValueError:
                    pass

        return report_date, report_type

    def create_report_page(self, report_title: str, report_content: str,
                          report_date: datetime = None) -> Dict[str, Any]:
        """创建报告页面，按年/月/日层级组织，支持多种报告类型"""
        try:
            if not self.integration_token or not self.parent_page_id:
                return {
                    "success": False,
                    "error": "Notion配置不完整"
                }

            # 从报告标题和内容中提取日期和类型
            if report_date is None:
                report_date, report_type = self._extract_report_date_and_type(report_title, report_content)
            else:
                # 如果提供了日期，仍需要提取类型
                _, report_type = self._extract_report_date_and_type(report_title, report_content)

            # 格式化报告标题，添加类型前缀
            # 对于可能有重复的报告类型，添加时间戳避免冲突
            if report_type in ["科技新闻", "社区洞察"]:
                # 这些类型每天可能有多个报告，需要添加时间戳
                beijing_time_str = report_date.strftime('%H:%M')
                formatted_title = f"[{report_type}] {report_title} [{beijing_time_str}]"
            elif report_type == "产品发现":
                # 产品发现报告：检查是否包含周报/日报标识，如果没有则添加时间戳
                if "周报" in report_title or "日报" in report_title or "weekly" in report_title.lower() or "daily" in report_title.lower():
                    # 已经有周期标识，不需要时间戳
                    formatted_title = f"[{report_type}] {report_title}"
                else:
                    # 没有周期标识，添加时间戳避免冲突
                    beijing_time_str = report_date.strftime('%H:%M')
                    formatted_title = f"[{report_type}] {report_title} [{beijing_time_str}]"
            else:
                # 其他类型正常处理
                formatted_title = f"[{report_type}] {report_title}"

            year = str(report_date.year)
            month = f"{report_date.month:02d}月"
            day = f"{report_date.day:02d}日"

            self.logger.info(f"开始创建报告页面: {year}/{month}/{day} - {formatted_title}")

            # 1. 查找或创建年份页面
            year_page_id = self.find_or_create_year_page(year)
            if not year_page_id:
                return {"success": False, "error": "无法创建年份页面"}

            # 2. 查找或创建月份页面
            month_page_id = self.find_or_create_month_page(year_page_id, month)
            if not month_page_id:
                return {"success": False, "error": "无法创建月份页面"}

            # 3. 查找或创建日期页面
            day_page_id = self.find_or_create_day_page(month_page_id, day)
            if not day_page_id:
                return {"success": False, "error": "无法创建日期页面"}

            # 3.5. 检查报告是否已经存在
            existing_report = self.check_report_exists(day_page_id, formatted_title)
            if existing_report and existing_report.get("exists"):
                self.logger.info(f"报告已存在，跳过创建: {existing_report.get('page_url')}")
                return {
                    "success": True,
                    "page_id": existing_report.get("page_id"),
                    "page_url": existing_report.get("page_url"),
                    "path": f"{year}/{month}/{day}/{formatted_title}",
                    "skipped": True,
                    "reason": "报告已存在"
                }

            # 4. 在日期页面下创建报告页面
            content_blocks, tables_to_add = self.markdown_to_notion_blocks(report_content)

            # 虽然API单次请求限制100块，但我们可以分批处理更多内容
            max_blocks = 1000
            if len(content_blocks) > max_blocks:
                self.logger.warning(f"报告内容过长({len(content_blocks)}个块)，截断到{max_blocks}个块")
                content_blocks = content_blocks[:max_blocks]

                # 添加截断提示
                content_blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{
                            "type": "text",
                            "text": {"content": "⚠️ 内容过长已截断，完整内容请查看数据库记录"},
                            "annotations": {"italic": True, "color": "gray"}
                        }]
                    }
                })
            else:
                self.logger.info(f"报告内容包含 {len(content_blocks)} 个块，在限制范围内")

            # 验证每个块的内容长度
            validated_blocks = []
            for i, block in enumerate(content_blocks):
                try:
                    # 检查rich_text内容长度
                    if block.get("type") in ["paragraph", "heading_1", "heading_2", "heading_3", "bulleted_list_item"]:
                        block_type = block["type"]
                        rich_text = block[block_type].get("rich_text", [])

                        # 限制每个rich_text项的长度（Notion Plus用户可以支持更长内容）
                        for text_item in rich_text:
                            if text_item.get("text", {}).get("content"):
                                content = text_item["text"]["content"]
                                if len(content) > 2000:  # Notion API限制
                                    original_length = len(content)
                                    text_item["text"]["content"] = content[:1997] + "..."
                                    self.logger.debug(f"块{i+1}文本被截断: {original_length} -> 2000字符")

                    validated_blocks.append(block)
                except Exception as e:
                    self.logger.warning(f"验证块{i+1}时出错，跳过: {e}")
                    continue

            self.logger.info(f"内容验证完成: {len(validated_blocks)}/{len(content_blocks)} 个块通过验证")

            # Notion API限制：单次创建页面最多100个子块
            # 需要分批处理大内容
            if len(validated_blocks) <= 100:
                # 小内容，直接创建
                create_result = self.create_page(day_page_id, formatted_title, validated_blocks)
            else:
                # 大内容，分批创建
                create_result = self._create_large_content_page(day_page_id, formatted_title, validated_blocks)

            if create_result.get("success"):
                page_id = create_result["data"]["id"]
                page_url = f"https://www.notion.so/{page_id.replace('-', '')}"

                # 检查是否有需要添加的表格
                if tables_to_add:
                    self.logger.info(f"页面创建成功，开始添加 {len(tables_to_add)} 个真实表格")
                    success_count = 0
                    for i, table_info in enumerate(tables_to_add):
                        try:
                            if self._add_real_table_to_page(page_id, table_info["headers"], table_info["rows"]):
                                success_count += 1
                                self.logger.info(f"真实表格 {i+1}/{len(tables_to_add)} 添加成功")
                            else:
                                self.logger.warning(f"真实表格 {i+1}/{len(tables_to_add)} 添加失败，但页面已创建")
                        except Exception as e:
                            self.logger.error(f"添加真实表格 {i+1} 时出错: {e}")

                    if success_count > 0:
                        self.logger.info(f"成功添加 {success_count}/{len(tables_to_add)} 个真实表格")

                self.logger.info(f"报告页面创建成功: {page_url}")
                return {
                    "success": True,
                    "page_id": page_id,
                    "page_url": page_url,
                    "path": f"{year}/{month}/{day}/{formatted_title}"
                }
            else:
                self.logger.error(f"创建报告页面失败: {create_result.get('error')}")
                return {"success": False, "error": create_result.get("error")}

        except Exception as e:
            self.logger.error(f"创建报告页面时出错: {e}")
            return {"success": False, "error": str(e)}


# 全局Notion客户端实例
notion_client = NotionClient()