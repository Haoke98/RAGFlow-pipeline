import requests
import json
import os
from datetime import datetime


class IndustryReportUploader:
    def __init__(self):
        self.base_url = "http://172.30.58.252"
        self.upload_url = f"{self.base_url}/v1/document/upload"
        self.headers = {
            'Accept': 'application/json',
            'Accept-Language': 'zh-CN',
            'Origin': self.base_url,
            'Connection': 'keep-alive',
            'Authorization': 'IjcxMjA4ZThjYzY1ZjExZWZhMjg4MDI0MmFjMTIwMDAzIg.Z3IVjQ.JdDCLmXjv-77b1vr3AIy06D_xRc',
        }

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

            # 从文件路径中获取实际的文件名
            real_filename = os.path.basename(file_path)

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


def main():
    uploader = IndustryReportUploader()

    # 设置要上传的文件路径
    file_path = r"E:\data\产业研报-解压过的\2022年\2022年1月份第2周报告\其他研究报告（158份）\其他研究报告（158份）\“百城千屏”活动实施指南-8页.pdf"

    # 上传文件
    result = uploader.upload_file("30bfd724c13911efa0ed0242ac120006", file_path)
    print(result["message"])


if __name__ == "__main__":
    main()
