import os
import hashlib
import sqlite3
import requests
from typing import Optional


class RAGFlowCli:
    def __init__(self, auth_token: str, base_url: str = "http://172.30.58.252", db_path: str = "documents.db"):
        self.base_url = base_url
        self.upload_url = f"{self.base_url}/v1/document/upload"
        self.search_url = f"{self.base_url}/v1/document/list"
        self.download_url = f"{self.base_url}/v1/document/get"
        self.headers = {
            'Accept': 'application/json',
            'Accept-Language': 'zh-CN',
            'Origin': self.base_url,
            'Connection': 'keep-alive',
            'Authorization': auth_token,
        }
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """初始化SQLite数据库"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS documents
                    (doc_id TEXT PRIMARY KEY, 
                     kb_id TEXT,
                     name TEXT,
                     file_hash TEXT,
                     create_date TEXT)''')
        conn.commit()
        conn.close()

    def _calculate_file_hash(self, file_path: str) -> str:
        """计算文件的SHA256哈希值"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _download_and_hash(self, doc_id: str) -> Optional[str]:
        """下载文档并计算哈希值"""
        try:
            response = requests.get(
                f"{self.download_url}/{doc_id}",
                headers=self.headers,
                stream=True
            )
            
            if response.status_code == 200:
                # 创建临时文件
                temp_path = f"temp_{doc_id}.pdf"
                sha256_hash = hashlib.sha256()
                
                # 边下载边计算哈希值
                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            sha256_hash.update(chunk)
                
                # 删除临时文件
                os.remove(temp_path)
                return sha256_hash.hexdigest()
            return None
        except Exception as e:
            print(f"下载文件时发生错误: {str(e)}")
            return None

    def sync(self, kb_id: str):
        """更新本地数据库中的文档哈希值"""
        docs = self.get_all_documents(kb_id)
        
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        for doc in docs:
            doc_id = doc.get('id')
            # 检查是否已经有哈希值
            c.execute('SELECT file_hash FROM documents WHERE doc_id = ?', (doc_id,))
            if not c.fetchone():
                print(f"正在处理文档: {doc.get('name')}")
                file_hash = self._download_and_hash(doc_id)
                if file_hash:
                    c.execute('''INSERT OR REPLACE INTO documents 
                                (doc_id, kb_id, name, file_hash, create_date)
                                VALUES (?, ?, ?, ?, ?)''',
                             (doc_id, kb_id, doc.get('name'), 
                              file_hash, doc.get('create_date')))
                    conn.commit()
        
        conn.close()

    def check_file_exists(self, kb_id: str, file_path: str) -> bool:
        """
        检查文件是否已经存在于知识库中（基于文件哈希值）
        """
        try:
            file_hash = self._calculate_file_hash(file_path)
            
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('SELECT doc_id FROM documents WHERE kb_id = ? AND file_hash = ?',
                     (kb_id, file_hash))
            result = c.fetchone()
            conn.close()
            
            return result is not None
        except Exception as e:
            print(f"检查文件哈希值时发生错误: {str(e)}")
            return False

    def get_all_documents(self, kb_id: str):
        """
        获取知识库中的所有文档
        :param kb_id: 知识库ID
        :return: 文档列表
        """
        all_docs = []
        page = 1
        while True:
            params = {
                'kb_id': kb_id,
                'page_size': 100,  # 每页获取100条记录
                'page': page
            }

            response = requests.get(
                self.search_url,
                headers=self.headers,
                params=params
            )

            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    docs = result.get('data', {}).get('docs', [])
                    if not docs:  # 如果没有更多文档了
                        break
                    all_docs.extend(docs)
                    page += 1
                else:
                    print(f"获取文档列表失败: {result.get('message')}")
                    break
            else:
                print(f"请求失败，状态码: {response.status_code}")
                break

        return all_docs

    def upload_file(self, kb_id: str, file_path: str):
        """
        上传文件
        :param kb_id: 知识库ID
        :param file_path: 文件路径
        :return:
        """
        try:
            if not os.path.exists(file_path):
                return {"success": False, "message": "文件不存在"}

            real_filename = os.path.basename(file_path)

            # 检查文件是否已存在
            if self.check_file_exists(kb_id, real_filename):
                return {"success": False, "message": "文件已存在，跳过上传"}

            files = {
                'file': (real_filename, open(file_path, 'rb'), 'application/pdf')
            }

            response = requests.post(
                self.upload_url,
                headers=self.headers,
                data={
                    "kb_id": kb_id,
                },
                files=files
            )

            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 9 and result.get('data') is True:
                    return {"success": True, "message": "文件上传成功"}
                else:
                    return {"success": False, "message": f"上传失败: {result.get('message')}"}
            else:
                return {"success": False, "message": f"上传失败，状态码: {response.status_code}"}

        except Exception as e:
            return {"success": False, "message": f"上传过程发生错误: {str(e)}"}

    def upload_directory(self, kb_id: str, directory_path: str):
        """
        上传指定目录下的所有PDF文件
        :param kb_id: 知识库ID
        :param directory_path: 目录路径
        :return: 上传统计结果
        """
        if not os.path.exists(directory_path):
            return {"success": False, "message": "目录不存在"}

        stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "failed_files": []
        }

        # 遍历目录下的所有文件
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                if file.lower().endswith('.pdf'):
                    file_path = os.path.join(root, file)
                    stats["total"] += 1

                    print(f"正在上传: {file}")
                    result = self.upload_file(kb_id, file_path)

                    if result["success"]:
                        stats["success"] += 1
                    else:
                        stats["failed"] += 1
                        stats["failed_files"].append({
                            "file": file_path,
                            "error": result["message"]
                        })

        # 生成上传报告
        report = (
            f"上传完成！\n"
            f"总文件数: {stats['total']}\n"
            f"成功: {stats['success']}\n"
            f"失败: {stats['failed']}\n"
        )

        if stats["failed"] > 0:
            report += "\n失败文件列表:\n"
            for failed in stats["failed_files"]:
                report += f"文件: {failed['file']}\n"
                report += f"错误: {failed['error']}\n"

        return {"success": True, "message": report, "stats": stats}
