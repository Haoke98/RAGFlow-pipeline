import json
import logging
import os
import hashlib
import sqlite3
import sys

import requests
from typing import Optional

from RAGFlowSDK import logger
from RAGFlowSDK.constants import APP_CONFIG_DIR


class RAGFlowCli:
    def __init__(self, auth_token: str = None, base_url: str = None, db_path: str = "documents.db"):
        # 初始化logger
        logger.init("RAGFlowCli")
        if auth_token is None:
            _auth_token = os.environ["RAGFLOW_AUTH_TOKEN"]
        else:
            _auth_token = auth_token
        if base_url is None:
            self.base_url = os.environ["RAGFLOW_BASE_URL"]
        else:
            self.base_url = base_url
        self.upload_url = f"{self.base_url}/v1/document/upload"
        self.search_url = f"{self.base_url}/v1/document/list"
        self.download_url = f"{self.base_url}/v1/document/get"
        self.headers = {
            'Accept': 'application/json',
            'Accept-Language': 'zh-CN',
            'Origin': self.base_url,
            'Connection': 'keep-alive',
            'Authorization': _auth_token,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"
        }
        if not os.path.exists(APP_CONFIG_DIR):
            os.makedirs(APP_CONFIG_DIR)
        self.db_path = os.path.join(APP_CONFIG_DIR, "documents.db")
        self._init_db()

    def _init_db(self):
        """初始化SQLite数据库"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # 检查是否需要更新表结构
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='documents'")
        table_exists = c.fetchone() is not None

        if table_exists:
            # 获取现有表的列信息
            c.execute('PRAGMA table_info(documents)')
            columns = {row[1] for row in c.fetchall()}

            # 如果是旧版表结构，则删除旧表
            if 'update_date' not in columns:
                c.execute('DROP TABLE documents')
                table_exists = False

        if not table_exists:
            c.execute('''CREATE TABLE documents
                        (doc_id TEXT PRIMARY KEY, 
                         kb_id TEXT,
                         name TEXT,
                         file_hash TEXT,
                         create_date TEXT,
                         status TEXT,           -- 文档处理状态
                         process_msg TEXT,      -- 处理信息
                         process TEXT,          -- 处理进度
                         size INTEGER,          -- 文件大小
                         source_type TEXT,      -- 来源类型
                         chunk_num INTEGER,     -- 分块数量
                         update_date TEXT)      -- 更新时间
                         ''')
            print("数据库表结构已更新")

        conn.commit()
        conn.close()

    def __do_request__(self, method: str, url: str, **kwargs) -> dict:
        """
        统一处理HTTP请求

        Args:
            method: HTTP方法 ('GET', 'POST' 等)
            url: 请求URL
            **kwargs: 请求的其他参数(params, json, data, files等)

        Returns:
            dict: 响应结果
        """
        # 确保headers存在
        if 'headers' not in kwargs:
            kwargs['headers'] = self.headers

        response = requests.request(method, url, **kwargs)

        if response.status_code == 200:
            content_type = response.headers.get('content-type')
            if content_type == 'application/json':
                result = response.json()
                if result['code'] == 401:
                    logging.error(result["message"] + f"\nHeaders: {response.request.headers}")
                    sys.exit(-1)
                elif result['code'] != 0:
                    raise Exception(result['message'])
                return {
                    'success': True,
                    'data': result,
                    'status_code': response.status_code
                }
            else:
                return {
                    'success': True,
                    'data': response,
                    'status_code': response.status_code
                }

        else:
            logging.error(
                f"请求失败，状态码: {response.status_code}\n"
                f"{method}: {url}\n"
                f"Headers: {kwargs.get('headers')}\n"
                f"Body: {kwargs.get('json') or kwargs.get('data')}"
            )
            return {
                'success': False,
                'error': f'HTTP错误: {response.status_code}',
                'status_code': response.status_code
            }
        # except Exception as e:
        #     error_msg = f"请求发生错误: {str(e)}"
        #     logging.error(error_msg)
        #     return {
        #         'success': False,
        #         'error': error_msg,
        #         'status_code': None
        #     }

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
            result = self.__do_request__(
                'GET',
                f"{self.download_url}/{doc_id}",
                stream=True
            )

            if result['success']:
                # 创建临时文件
                temp_path = f"temp_{doc_id}.pdf"
                sha256_hash = hashlib.sha256()

                # 边下载边计算哈希值
                with open(temp_path, 'wb') as f:
                    for chunk in result['data'].iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            sha256_hash.update(chunk)

                # 删除临时文件
                os.remove(temp_path)
                return sha256_hash.hexdigest()
            return None
        except Exception as e:
            logging.error(f"下载文件时发生错误: {str(e)}", exc_info=True)
            return None

    def sync(self, kb_id: str):
        """更新本地数据库中的文档信息和哈希值"""
        docs = self.get_all_documents(kb_id)

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        total = len(docs)

        for i,doc in enumerate(docs,1):
            p = i/total*100
            doc_id = doc.get('id')
            # 检查是否需要更新文档信息
            c.execute('SELECT update_date FROM documents WHERE doc_id = ?', (doc_id,))
            result = c.fetchone()
            doc_update_date = doc.get('update_date')

            if not result or result[0] != doc_update_date:
                logging.info(f"{p:.2f}% ({i}/{total}) 正在处理文档: {doc.get('name')}")
                # 计算文件哈希值（如果需要）
                file_hash = None
                if not result:  # 新文档才需要下载计算哈希值
                    file_hash = self._download_and_hash(doc_id)

                if file_hash or result:  # 新文档有哈希值或是更新已有文档
                    c.execute('''INSERT OR REPLACE INTO documents 
                                (doc_id, kb_id, name, file_hash, create_date,
                                 status, process_msg, process, size, source_type,
                                 chunk_num, update_date)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                              (doc_id, kb_id, doc.get('name'),
                               file_hash if file_hash else result[0],  # 保留原有哈希值
                               doc.get('create_date'),
                               str(doc.get('status')),
                               doc.get('progress_msg'),
                               str(doc.get('progress', 0)),
                               doc.get('size'),
                               doc.get('source_type'),
                               doc.get('chunk_num'),
                               doc_update_date))
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
                'page_size': 100,
                'page': page
            }

            result = self.__do_request__('GET', self.search_url, params=params)

            if result['success']:
                response_data = result['data']
                if response_data.get('code') == 0:
                    docs = response_data.get('data', {}).get('docs', [])
                    if not docs:  # 如果没有更多文档了
                        break
                    all_docs.extend(docs)
                    page += 1
                else:
                    print(f"获取文档列表失败: {response_data.get('message')}")
                    break
            else:
                print(f"请求失败: {result.get('error')}")
                break

        return all_docs

    def upload_file(self, kb_id: str, file_path: str):
        """
        上传文件
        :param kb_id: 知识库ID
        :param file_path: 文件路径
        :return: dict
        """
        try:
            if not os.path.exists(file_path):
                return {"success": False, "message": "文件不存在"}

            real_filename = os.path.basename(file_path)
            file_hash = self._calculate_file_hash(file_path)

            # 检查文件是否已存在（基于哈希值）
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute("""
                SELECT name, doc_id, process 
                FROM documents 
                WHERE kb_id = ? AND file_hash = ?
            """, (kb_id, file_hash))
            existing_doc = c.fetchone()
            conn.close()

            if existing_doc:
                name, doc_id, process = existing_doc
                process = float(process or 0)
                return {
                    "success": False,
                    "message": f"文件已存在（文件名：{name}，文档ID：{doc_id}，处理进度：{process * 100}%），跳过上传"
                }

            files = {
                'file': (real_filename, open(file_path, 'rb'), 'application/pdf')
            }

            result = self.__do_request__(
                'POST',
                self.upload_url,
                data={"kb_id": kb_id},
                files=files
            )

            if result['success']:
                response_data = result['data']
                if response_data.get('code') == 9 and response_data.get('data') is True:
                    self.sync(kb_id)
                    return {"success": True, "message": "文件上传成功"}
                else:
                    return {"success": False, "message": f"上传失败: {response_data.get('message')}"}
            else:
                return {"success": False, "message": f"上传失败: {result.get('error')}"}

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
        self.sync(kb_id)
        stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "failed_files": []
        }
        logging.info("正在扫描目录.....")
        pending_files = []
        # 遍历目录下的所有文件
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                if file.lower().endswith('.pdf'):
                    pending_files.append(os.path.join(root, file))
        total = len(pending_files)
        logging.info(f"扫描已完成：{total}")
        for i, file_path in enumerate(pending_files, start=1):
            p = i / total * 100
            stats["total"] += 1
            logging.info(f"{p:.2f}%({i}/{total}) 正在上传: {file_path}")
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

    def check_duplicates(self, kb_id: str) -> str:
        """
        检查知识库中的重复文档（基于文件哈希值）
        :param kb_id: 知识库ID
        :return: 重复文档报告
        """
        # 首先确保本地数据库是最新的
        self.sync(kb_id)

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # 查找具有相同哈希值的文档，并按重复数量降序排序
        c.execute('''
            WITH duplicate_counts AS (
                SELECT file_hash, COUNT(*) as count
                FROM documents 
                WHERE kb_id = ?
                GROUP BY file_hash 
                HAVING count > 1
                ORDER BY count DESC
            )
            SELECT 
                d.file_hash,
                GROUP_CONCAT(d.name) as names,
                GROUP_CONCAT(d.doc_id) as doc_ids,
                GROUP_CONCAT(d.create_date) as dates,
                GROUP_CONCAT(d.status) as statuses,
                GROUP_CONCAT(d.process) as processes,
                GROUP_CONCAT(d.size) as sizes,
                dc.count
            FROM documents d
            JOIN duplicate_counts dc ON d.file_hash = dc.file_hash
            GROUP BY d.file_hash
        ''', (kb_id,))

        duplicates = c.fetchall()

        # 获取知识库中的总文档数
        c.execute('SELECT COUNT(*) FROM documents WHERE kb_id = ?', (kb_id,))
        total_docs = c.fetchone()[0]

        # 生成报告
        report = []
        report.append(f"知识库文档查重报告")
        report.append(f"总文档数: {total_docs}")
        report.append(f"发现重复文档组数: {len(duplicates)}")

        if duplicates:
            report.append("\n重复文档详情:")
            for file_hash, names, doc_ids, dates, statuses, processes, sizes, count in duplicates:
                report.append(f"\n文件哈希值: {file_hash}")
                report.append(f"重复数量: {count}")

                # 将组合字符串分割成列表并创建文档信息元组列表
                doc_infos = []
                name_list = names.split(',')
                doc_id_list = doc_ids.split(',')
                date_list = dates.split(',')
                status_list = statuses.split(',')
                process_list = processes.split(',')
                size_list = sizes.split(',')

                for i in range(count):
                    doc_infos.append({
                        'doc_id': doc_id_list[i],
                        'name': name_list[i],
                        'date': date_list[i],
                        'status': status_list[i],
                        'process': float(process_list[i] or 0),  # 处理空值情况
                        'size': int(size_list[i])
                    })

                # 按处理进度降序排序
                doc_infos.sort(key=lambda x: x['process'], reverse=True)

                report.append("重复实例:")
                for doc in doc_infos:
                    status_map = {
                        "0": "待处理",
                        "1": "处理完成",
                        "-1": "处理失败"
                    }
                    status = status_map.get(doc['status'], "未知状态")

                    report.append(f"  - 文档ID: {doc['doc_id']}")
                    report.append(f"    文件名: {doc['name']}")
                    report.append(f"    创建时间: {doc['date']}")
                    report.append(f"    处理状态: {status}")
                    report.append(f"    处理进度: {doc['process'] * 100}%")
                    report.append(f"    文件大小: {doc['size']:,} 字节")
        else:
            report.append("\n未发现重复文档！")

        conn.close()
        return "\n".join(report)

    def get_duplicate_groups(self, kb_id: str) -> list:
        """
        获取重复文档组（用于后续可能的删除操作）
        :param kb_id: 知识库ID
        :return: 重复文档组列表
        """
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            SELECT file_hash, GROUP_CONCAT(doc_id) as doc_ids
            FROM documents 
            WHERE kb_id = ?
            GROUP BY file_hash 
            HAVING COUNT(*) > 1
        ''', (kb_id,))

        duplicate_groups = []
        for file_hash, doc_ids in c.fetchall():
            duplicate_groups.append({
                'hash': file_hash,
                'doc_ids': doc_ids.split(',')
            })

        conn.close()
        return duplicate_groups

    def delete_document(self, doc_id: str) -> bool:
        """
        删除指定的文档
        :param doc_id: 文档ID
        :return: bool
        """
        try:
            result = self.__do_request__(
                'POST',
                f"{self.base_url}/v1/document/rm",
                json={"doc_id": [doc_id]}
            )

            if result['success']:
                response_data = result['data']
                if response_data.get('code') == 0 and response_data.get('data') is True:
                    # 从本地数据库中也删除该文档
                    conn = sqlite3.connect(self.db_path)
                    c = conn.cursor()
                    c.execute('DELETE FROM documents WHERE doc_id = ?', (doc_id,))
                    conn.commit()
                    conn.close()
                    return True
            return False
        except Exception as e:
            print(f"删除文档时发生错误: {str(e)}")
            return False

    def clean_duplicates(self, kb_id: str) -> str:
        """
        清理重复文档，保留解析进度最高的版本
        :param kb_id: 知识库ID
        :return: 清理报告
        """
        # 首先确保本地数据库是最新的
        self.sync(kb_id)

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # 查找重复文档组
        c.execute('''
            WITH duplicate_counts AS (
                SELECT file_hash, COUNT(*) as count
                FROM documents 
                WHERE kb_id = ?
                GROUP BY file_hash 
                HAVING count > 1
            )
            SELECT 
                d.file_hash,
                GROUP_CONCAT(d.doc_id) as doc_ids,
                GROUP_CONCAT(d.name) as names,
                GROUP_CONCAT(d.process) as processes
            FROM documents d
            JOIN duplicate_counts dc ON d.file_hash = dc.file_hash
            GROUP BY d.file_hash
        ''', (kb_id,))

        duplicates = c.fetchall()

        # 清理统计
        stats = {
            "total_groups": len(duplicates),
            "total_deleted": 0,
            "failed_deletes": [],
            "details": []
        }

        for file_hash, doc_ids, names, processes in duplicates:
            doc_id_list = doc_ids.split(',')
            name_list = names.split(',')
            process_list = [float(p or 0) for p in processes.split(',')]

            # 将文档信息组合成列表并按处理进度排序
            docs = list(zip(doc_id_list, name_list, process_list))
            docs.sort(key=lambda x: x[2], reverse=True)  # 按进度降序排序

            # 保留进度最高的文档，删除其他的
            kept_doc = docs[0]
            docs_to_delete = docs[1:]

            group_detail = {
                "file_hash": file_hash,
                "kept_doc": {
                    "id": kept_doc[0],
                    "name": kept_doc[1],
                    "progress": kept_doc[2]
                },
                "deleted_docs": []
            }

            for doc_id, name, progress in docs_to_delete:
                if self.delete_document(doc_id):
                    stats["total_deleted"] += 1
                    group_detail["deleted_docs"].append({
                        "id": doc_id,
                        "name": name,
                        "progress": progress,
                        "status": "成功"
                    })
                else:
                    stats["failed_deletes"].append(doc_id)
                    group_detail["deleted_docs"].append({
                        "id": doc_id,
                        "name": name,
                        "progress": progress,
                        "status": "失败"
                    })

            stats["details"].append(group_detail)

        conn.close()

        # 生成报告
        report = []
        report.append("重复文档清理报告")
        report.append(f"重复文档组数: {stats['total_groups']}")
        report.append(f"已删除文档数: {stats['total_deleted']}")
        if stats["failed_deletes"]:
            report.append(f"删除失败数: {len(stats['failed_deletes'])}")

        if stats["details"]:
            report.append("\n清理详情:")
            for detail in stats["details"]:
                report.append(f"\n文件哈希值: {detail['file_hash']}")
                report.append("保留的文档:")
                report.append(f"  - ID: {detail['kept_doc']['id']}")
                report.append(f"    文件名: {detail['kept_doc']['name']}")
                report.append(f"    处理进度: {detail['kept_doc']['progress'] * 100}%")

                if detail["deleted_docs"]:
                    report.append("删除的文档:")
                    for doc in detail["deleted_docs"]:
                        report.append(f"  - ID: {doc['id']}")
                        report.append(f"    文件名: {doc['name']}")
                        report.append(f"    处理进度: {doc['progress'] * 100}%")
                        report.append(f"    状态: {doc['status']}")

        return "\n".join(report)
