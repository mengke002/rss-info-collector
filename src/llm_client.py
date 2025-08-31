"""
LLM客户端模块
支持双层模型策略：fast_model (快速信息提取) 和 smart_model (深度分析)
"""
import logging
import json
from typing import Dict, Any, Optional
import httpx

from .config import config

logger = logging.getLogger(__name__)


class LLMClient:
    """统一的LLM客户端，支持快速和智能两种模型类型"""

    def __init__(self):
        """初始化LLM客户端"""
        self.logger = logging.getLogger(__name__)
        
        # 获取LLM配置
        try:
            self.llm_config = config.get_llm_config()
        except ValueError as e:
            self.logger.error(f"LLM配置获取失败: {e}")
            raise
        
        # 创建HTTP客户端
        self.http_client = httpx.Client(
            base_url=self.llm_config['openai_base_url'],
            headers={
                "Authorization": f"Bearer {self.llm_config['openai_api_key']}",
                "Content-Type": "application/json",
            },
            timeout=240.0
        )
        
        self.logger.info("LLM客户端初始化成功")
        self.logger.info(f"Fast Model: {self.llm_config['fast_model_name']}")
        self.logger.info(f"Smart Model: {self.llm_config['smart_model_name']}")

    def call_llm(self, prompt: str, model_type: str = 'fast', temperature: float = 0.3) -> Dict[str, Any]:
        """
        调用LLM进行分析
        
        Args:
            prompt: 输入的提示词
            model_type: 模型类型，'fast' 或 'smart'
            temperature: 生成温度
            
        Returns:
            LLM响应结果字典
        """
        if model_type not in ['fast', 'smart']:
            raise ValueError("model_type 必须是 'fast' 或 'smart'")
            
        # 根据模型类型选择模型
        if model_type == 'fast':
            model_name = self.llm_config['fast_model_name']
        else:
            model_name = self.llm_config['smart_model_name']
            
        return self._make_request(prompt, model_name, temperature)

    def call_fast_model(self, prompt: str, temperature: float = 0.1) -> Dict[str, Any]:
        """
        调用快速模型进行信息提取
        适用于：结构化信息提取、分类等快速任务
        """
        return self.call_llm(prompt, 'fast', temperature)

    def call_smart_model(self, prompt: str, temperature: float = 0.5) -> Dict[str, Any]:
        """
        调用智能模型进行深度分析
        适用于：报告生成、深度洞察、综合分析等复杂任务
        """
        return self.call_llm(prompt, 'smart', temperature)

    def _make_request(self, prompt: str, model_name: str, temperature: float) -> Dict[str, Any]:
        """
        执行具体的LLM请求，带有重试机制
        
        Args:
            prompt: 提示词
            model_name: 模型名称
            temperature: 生成温度
            
        Returns:
            响应结果字典
        """
        import time
        
        # 重试配置
        max_retries = 3
        base_delay = 2.0  # 基本延迟秒数
        
        for attempt in range(max_retries + 1):
            try:
                self.logger.info(f"调用LLM: {model_name} (尝试 {attempt + 1}/{max_retries + 1})")
                self.logger.info(f"Prompt长度: {len(prompt)} 字符")
                
                request_data = {
                    "model": model_name,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ],
                    "stream": True,
                    "temperature": temperature,
                }

                full_response_content = ""
                chunk_count = 0
                
                self.logger.debug("开始streaming响应处理...")
                with self.http_client.stream("POST", "/chat/completions", json=request_data) as response:
                    # 检查响应状态
                    if response.status_code != 200:
                        response.raise_for_status()
                    
                    # 处理流式响应
                    for line in response.iter_text():
                        if line.strip():
                            for sub_line in line.split('\n'):
                                if sub_line.startswith('data: '):
                                    line_data = sub_line[len('data: '):]
                                    if line_data.strip() == '[DONE]':
                                        break
                                    try:
                                        chunk = json.loads(line_data)
                                        if 'choices' in chunk and chunk['choices']:
                                            delta = chunk['choices'][0].get('delta', {})
                                            content_part = delta.get('content')
                                            if content_part:
                                                full_response_content += content_part
                                                chunk_count += 1
                                    except json.JSONDecodeError:
                                        # 忽略无法解析的chunk，继续处理
                                        continue

                self.logger.info(f"LLM调用完成 - 处理了 {chunk_count} 个chunks")
                self.logger.info(f"响应内容长度: {len(full_response_content)} 字符")
                
                return {
                    'success': True,
                    'content': full_response_content.strip(),
                    'model': model_name,
                    'provider': 'openai_compatible'
                }

            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                
                # 尝试读取错误响应体
                try:
                    error_body = e.response.text
                except Exception:
                    error_body = "无法读取响应体"
                
                # 对于可重试的错误，进行重试
                if status_code in [429, 502, 503, 504] and attempt < max_retries:
                    delay = base_delay * (2 ** attempt)  # 指数退避
                    self.logger.warning(f"LLM API请求失败 ({status_code})，{delay}秒后重试... (尝试 {attempt + 1}/{max_retries + 1})")
                    time.sleep(delay)
                    continue
                else:
                    error_msg = f"LLM API请求失败: {status_code} - {error_body}"
                    self.logger.error(error_msg)
                    return {
                        'success': False, 
                        'error': error_msg,
                        'model': model_name
                    }
                    
            except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadError) as e:
                # 网络相关错误，可重试
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    self.logger.warning(f"网络错误: {str(e)}，{delay}秒后重试... (尝试 {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    error_msg = f"网络连接错误: {str(e)}"
                    self.logger.error(error_msg)
                    return {
                        'success': False, 
                        'error': error_msg,
                        'model': model_name
                    }
                    
            except Exception as e:
                # 其他不可重试的错误
                error_msg = f"LLM客户端错误: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                return {
                    'success': False, 
                    'error': error_msg,
                    'model': model_name
                }
        
        # 所有重试都失败
        error_msg = f"经过 {max_retries + 1} 次尝试后 LLM 调用仍然失败"
        self.logger.error(error_msg)
        return {
            'success': False,
            'error': error_msg,
            'model': model_name
        }

    def extract_json_from_response(self, response_content: str) -> Optional[Dict[str, Any]]:
        """
        从LLM响应中提取JSON内容
        
        Args:
            response_content: LLM响应的文本内容
            
        Returns:
            提取的JSON字典，如果解析失败则返回None
        """
        # 尝试直接解析
        try:
            return json.loads(response_content.strip())
        except json.JSONDecodeError:
            pass
        
        # 尝试提取代码块中的JSON
        import re
        json_pattern = r'```json\s*(.*?)\s*```'
        match = re.search(json_pattern, response_content, re.DOTALL | re.IGNORECASE)
        
        if match:
            try:
                return json.loads(match.group(1).strip())
            except json.JSONDecodeError:
                pass
        
        # 尝试提取花括号内的内容
        brace_pattern = r'\{.*\}'
        match = re.search(brace_pattern, response_content, re.DOTALL)
        
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        
        self.logger.warning("无法从LLM响应中提取有效JSON")
        return None

    def __del__(self):
        """清理资源"""
        if hasattr(self, 'http_client'):
            self.http_client.close()


# 创建全局LLM客户端实例
def get_llm_client() -> Optional[LLMClient]:
    """获取LLM客户端实例"""
    try:
        return LLMClient()
    except Exception as e:
        logger.warning(f"LLM客户端初始化失败: {e}")
        return None


# 便捷函数
def call_llm(prompt: str, model_type: str = 'fast', temperature: float = 0.3) -> Dict[str, Any]:
    """
    便捷的LLM调用函数
    
    Args:
        prompt: 提示词
        model_type: 模型类型 ('fast' 或 'smart')
        temperature: 生成温度
        
    Returns:
        LLM响应结果
    """
    client = get_llm_client()
    if not client:
        return {'success': False, 'error': 'LLM客户端初始化失败'}
    
    return client.call_llm(prompt, model_type, temperature)