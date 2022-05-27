import logging
import logging.handlers
import os


class Logs(object):
    def __init__(self, log_file):
        self.logger = logging.getLogger("")
        # 创建文件目录
        logs_dir = "logs"
        if not os.path.exists(logs_dir) and not os.path.isdir(logs_dir):
            os.mkdir(logs_dir)
        # 修改log保存位置
        log_file_path = os.path.join(logs_dir, '%s.log' % log_file)
        rotating_file_handler = logging.handlers.RotatingFileHandler(filename=log_file_path,
                                                                     maxBytes=1024 * 1024 * 50,
                                                                     backupCount=5)
        # 设置输出格式
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
        rotating_file_handler.setFormatter(formatter)
        # 控制台句柄
        console = logging.StreamHandler()
        console.setLevel(logging.NOTSET)
        console.setFormatter(formatter)
        # 添加内容到日志句柄中
        self.logger.addHandler(rotating_file_handler)
        self.logger.addHandler(console)
        self.logger.setLevel(logging.NOTSET)

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)
