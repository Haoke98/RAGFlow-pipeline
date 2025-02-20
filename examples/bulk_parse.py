"""
自动检测知识库中的还未开始解析和解析失败的文档自动开启解析
"""
import logging

from RAGFlowSDK.core import RAGFlowCli


def main():
    cli = RAGFlowCli()
    # 设置要上传的目录路径
    kb_id = "f1ee42ceef7811ef9c2c0242ac170006"

    # 上传目录中的所有PDF文件
    docs = cli.get_all_documents(kb_id)
    total = len(docs)
    bulk_buffer = []
    for i, doc in enumerate(docs, start=1):
        p = i / total * 100
        doc_id = doc['id']
        prefix = f"{p:.2f}%({i}/{total}) {doc_id} {doc['name']} "
        # 0: 没在执行， 1：正在执行， 2： 失败， 3：成功
        run_status = doc['run']
        if run_status == '0':
            logging.critical(prefix + "(还未解析)")
            # TODO：触发解析
            bulk_buffer.append(doc_id)
        elif run_status == '1':
            logging.warning(prefix + f"(正在解析：{doc['progress'] * 100:.2f} %)")
        elif run_status == '2':
            logging.error(prefix + "(失败了，需要重新解析)")
            # TODO：触发解析
            bulk_buffer.append(doc_id)
        elif run_status == '3':
            logging.info(prefix + "(解析已完成)")
        if len(bulk_buffer) > 10:
            cli.run(bulk_buffer, 1)
            logging.debug("发起了批量解析请求")
            bulk_buffer = []
    cli.run(bulk_buffer, 1)
    # print(result["message"])


if __name__ == "__main__":
    main()
