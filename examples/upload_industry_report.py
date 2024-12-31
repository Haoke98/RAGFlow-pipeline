import requests
import json
import os
from datetime import datetime

from RAGFlowSDK.core import RAGFlowCli


def main():
    uploader = RAGFlowCli(auth_token="IjcxMjA4ZThjYzY1ZjExZWZhMjg4MDI0MmFjMTIwMDAzIg.Z3IVjQ.JdDCLmXjv-77b1vr3AIy06D_xRc")
    # 设置要上传的目录路径
    directory_path = r"E:\data\产业研报-解压过的\2022年\2022年1月份第2周报告\其他研究报告（158份）"
    kb_id = "30bfd724c13911efa0ed0242ac120006"

    # 上传目录中的所有PDF文件
    result = uploader.upload_directory(kb_id, directory_path)
    print(result["message"])


if __name__ == "__main__":
    main()
