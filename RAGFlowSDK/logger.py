# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2022/11/22
@Software: PyCharm
@disc:
======================================="""
import datetime
import logging
import os
import sys
import colorlog


# 定义一个函数来获取日志目录
def get_log_directory():
    if sys.platform.startswith('win'):
        # Windows系统
        log_dir = os.path.join(os.environ['ALLUSERSPROFILE'], 'Logs')
    elif sys.platform.startswith('darwin'):
        # macOS系统
        log_dir = os.path.join(os.path.expanduser('~'), 'Library', 'Logs')
    else:
        # Linux及其他类Unix系统
        log_dir = '/var/log'
    print("LogDir:",log_dir)
    # 确保日志目录存在
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    return log_dir


def init(filename, file_level=logging.DEBUG, console_level=logging.INFO):
    # 控制台输出不同级别日志颜色设置
    color_config = {
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'purple',
    }

    # 输出到控制台
    console_handler = logging.StreamHandler()
    # console_handler = logging.StreamHandler(sys.stdout)
    # 日志格化字符串
    console_handler.setFormatter(colorlog.ColoredFormatter(
        fmt='{log_color:s}[{asctime:s}][{levelname:^7s}][{threadName:s}-{filename:s}:{lineno:d}]: {message:s}',
        log_colors=color_config, style='{'))
    # 指定最低日志级别：（critical > error > warning > info > debug）
    console_handler.setLevel(console_level)
    log_dir = get_log_directory()
    n = datetime.datetime.now()
    logFilePath = os.path.join(log_dir, f"{filename}-{str(n.year).zfill(4)}{str(n.month).zfill(2)}{n.day}{str(n.hour).zfill(2)}{str(n.minute).zfill(2)}{str(n.second).zfill(2)}.log")
    print("LogFilePath:", logFilePath)
    file_handler = logging.FileHandler(filename=logFilePath, mode='a', encoding='utf-8')
    file_handler.setFormatter(
        logging.Formatter(fmt='[{asctime:s}][{levelname:^7s}][{threadName:s}-{filename:s}:{lineno:d}]: {message:s}',
                          style='{', datefmt='%m/%d/%Y %H:%M:%S'))
    # 指定最低日志级别：（critical > error > warning > info > debug）
    file_handler.setLevel(file_level)

    logging.basicConfig(level=min(file_level, console_level), handlers=[file_handler, console_handler])
    logging.debug("日志输出文件：" + logFilePath)
