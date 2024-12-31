import requests
import json
import os
from datetime import datetime


class IndustryReportUploader:
    def __init__(self):
        self.base_url = "http://172.30.58.252"
        self.upload_url = f"{self.base_url}/v1/document/upload"
        self.search_url = f"{self.base_url}/v1/document/list"
        self.headers = {
            'Accept': 'application/json',
            'Accept-Language': 'zh-CN',
            'Origin': self.base_url,
            'Connection': 'keep-alive',
            'Authorization': 'IjcxMjA4ZThjYzY1ZjExZWZhMjg4MDI0MmFjMTIwMDAzIg.Z3IVjQ.JdDCLmXjv-77b1vr3AIy06D_xRc',
        }

    def check_file_exists(self, kb_id: str, filename: str) -> bool:
        """
        检查文件是否已经存在于知识库中
        :param kb_id: 知识库ID
        :param filename: 文件名
        :return: 是否存在
        """
        try:
            params = {
                'kb_id': kb_id,
                'keywords': filename,
                'page_size': 10,
                'page': 1
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
                    # 检查是否有完全匹配的文件名
                    for doc in docs:
                        if doc.get('name') == filename:
                            return True
            return False
        except Exception as e:
            print(f"检查文件是否存在时发生错误: {str(e)}")
            return False

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


def main():
    uploader = IndustryReportUploader()
    
    # 设置要上传的目录路径
    directory_path = r"E:\data\产业研报-解压过的\2022年\2022年1月份第2周报告\其他研究报告（158份）"
    kb_id = "30bfd724c13911efa0ed0242ac120006"

    # 上传目录中的所有PDF文件
    result = uploader.upload_directory(kb_id, directory_path)
    print(result["message"])


if __name__ == "__main__":
    main()
