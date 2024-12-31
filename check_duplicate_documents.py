from RAGFlowSDK.core import RAGFlowCli


def main():
    auth_token = "IjcxMjA4ZThjYzY1ZjExZWZhMjg4MDI0MmFjMTIwMDAzIg.Z3IVjQ.JdDCLmXjv-77b1vr3AIy06D_xRc"
    cli = RAGFlowCli(auth_token)
    kb_id = "30bfd724c13911efa0ed0242ac120006"
    
    print("正在检查重复文档...")
    report = cli.check_duplicates(kb_id)
    
    print(report)


if __name__ == "__main__":
    main()