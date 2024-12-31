from RAGFlowSDK.core import RAGFlowCli


def main():
    auth_token = "IjcxMjA4ZThjYzY1ZjExZWZhMjg4MDI0MmFjMTIwMDAzIg.Z3IVjQ.JdDCLmXjv-77b1vr3AIy06D_xRc"
    cli = RAGFlowCli(auth_token)
    kb_id = "30bfd724c13911efa0ed0242ac120006"
    
    print("开始清理重复文档...")
    report = cli.clean_duplicates(kb_id)
    
    # 将报告同时保存到文件
    with open('cleanup_report.txt', 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(report)
    print("\n报告已保存到 cleanup_report.txt")


if __name__ == "__main__":
    main() 