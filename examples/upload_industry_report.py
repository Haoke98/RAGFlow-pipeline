from RAGFlowSDK.core import RAGFlowCli


def main():
    uploader = RAGFlowCli()
    # 设置要上传的目录路径
    directory_path = r"E:\data\产业研报-解压过的"
    kb_id = "f1ee42ceef7811ef9c2c0242ac170006"

    # 上传目录中的所有PDF文件
    result = uploader.upload_directory(kb_id, directory_path)
    print(result["message"])


if __name__ == "__main__":
    main()
