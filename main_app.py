# main_app.py
import sys
import os
import logging
import logging.handlers
import faulthandler
from PyQt5.QtWidgets import QApplication, QMessageBox
from PyQt5.QtCore import Qt, QDateTime

from gui_manager import DownloadManager
# MODIFIED LINE: Added YT_DLP_EXECUTABLE_PATH to the import
from constants import LOG_FILE_NAME, APPLICATION_DATA_DIRECTORY, YT_DLP_EXECUTABLE_PATH

log_file_path_global = ""

def setup_logging():
    """配置日志系统"""
    global log_file_path_global

    log_file_path_global = os.path.join(APPLICATION_DATA_DIRECTORY, LOG_FILE_NAME)
    if not os.path.exists(APPLICATION_DATA_DIRECTORY):
        try:
            os.makedirs(APPLICATION_DATA_DIRECTORY, exist_ok=True)
        except Exception as e:
            print(f"CRITICAL: Failed to create log directory {APPLICATION_DATA_DIRECTORY} from main_app.py: {e}")

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    try:
        fh = logging.handlers.RotatingFileHandler(
            log_file_path_global, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'
        )
        fh.setLevel(logging.DEBUG)
        formatter_file = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(threadName)s:%(thread)d] - %(name)s.%(funcName)s:%(lineno)d - %(message)s'
        )
        fh.setFormatter(formatter_file)
        logger.addHandler(fh)
    except Exception as e:
        print(f"CRITICAL: Error setting up file logger to {log_file_path_global}: {e}")

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter_console = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    ch.setFormatter(formatter_console)
    logger.addHandler(ch)

    logging.info(f"日志系统已启动。日志文件路径: {log_file_path_global}")
    # This line should now work as YT_DLP_EXECUTABLE_PATH is imported globally in this file
    logging.info(f"YT_DLP_EXECUTABLE_PATH from constants: {YT_DLP_EXECUTABLE_PATH}")
    logging.info(f"APPLICATION_DATA_DIRECTORY from constants: {APPLICATION_DATA_DIRECTORY}")


def main():
    faulthandler.enable()
    setup_logging()

    QApplication.setApplicationName("ytdow")
    if hasattr(Qt, 'AA_EnableHighDpiScaling'):
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)

    app = QApplication(sys.argv)

    # Optional: Check if yt-dlp is actually found, though constants.py handles the path.
    # import shutil
    # if not YT_DLP_EXECUTABLE_PATH or (not os.path.isabs(YT_DLP_EXECUTABLE_PATH) and not shutil.which(YT_DLP_EXECUTABLE_PATH)):
    #     logging.warning(f"yt-dlp executable might not be correctly configured or found: {YT_DLP_EXECUTABLE_PATH}")
    #     QMessageBox.warning(None, "配置警告",
    #                         f"yt-dlp 执行程序路径可能未正确配置或找到: {YT_DLP_EXECUTABLE_PATH}\n"
    #                         "请确保 yt-dlp 已安装并位于系统PATH中，或与程序在同一目录。")

    logging.info("================ Application starting ================")
    exit_code = 0
    try:
        window = DownloadManager()
        window.show()
        exit_code = app.exec_()
        logging.info(f"Application exited with Qt event loop code {exit_code}.")
    except Exception as e:
        logging.critical("Unhandled exception in main application scope leading to exit.", exc_info=True)
        try:
            msg_box = QMessageBox()
            msg_box.setIcon(QMessageBox.Critical)
            msg_box.setWindowTitle("严重错误")
            msg_box.setText(f"应用程序遇到无法处理的严重错误，即将退出。\n错误信息: {type(e).__name__}: {e}\n\n详情请查看日志文件。")
            import traceback
            detailed_text = str(e) + "\n\n--- Traceback ---\n"
            detailed_text += "".join(traceback.format_exception(type(e), e, e.__traceback__))
            msg_box.setDetailedText(detailed_text)
            msg_box.exec_()
        except Exception as e_msgbox:
            logging.error(f"Could not show critical error QMessageBox: {e_msgbox}")
        exit_code = 1
    finally:
        logging.info(f"================ Application ended (exit code: {exit_code}) ==================")
        sys.exit(exit_code)


if __name__ == "__main__":
    # import shutil # Import if using shutil.which in main()
    main()
