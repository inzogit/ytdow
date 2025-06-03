# gui_manager.py
import os
import sys # sys.platform
import json
import subprocess
import platform
import logging
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QPushButton, QLabel, QTextEdit, QFileDialog, QLineEdit, QComboBox, QSpinBox,
    QMessageBox, QCheckBox, QTabWidget, QApplication
)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QDateTime # QDateTime for backup file naming

# 从其他模块导入
from workers import YtDlpListFetcher, DownloadTaskWorker
from constants import TASKS_HISTORY_FILE_NAME, APPLICATION_DATA_DIRECTORY, YT_DLP_EXECUTABLE_PATH # 使用常量

class DownloadManager(QWidget):
    def __init__(self):
        super().__init__()
        self.log_prefix = f"[{self.__class__.__name__}] "
        logging.info(f"{self.log_prefix}Initializing DownloadManager...")
        self.setWindowTitle("yt-dlp 下载助手")
        self.resize(1450, 900)

        self.tasks = {} # task_id: task_data_dict
        self.task_id_counter = 0 # 会在加载历史后调整
        self.failed_tasks = set() # 保存失败任务的ID，用于重试按钮状态

        self.active_workers = 0
        self.max_concurrent = 1 # 默认并发数
        self.task_queue = [] # 等待下载的任务ID列表
        self._urls_to_fetch_queue = [] # 等待解析的URL列表
        self.current_fetch_url = "" # 当前正在解析的URL
        self.list_fetcher_processed_count = 0 # 用于解析完成后的提示

        self._setup_ui() # 调用UI设置方法

        self.timer = QTimer(self)
        self.timer.setInterval(1000) # 1秒检查一次队列
        self.timer.timeout.connect(self.check_and_start_tasks)
        self.timer.start()

        self.load_tasks_from_file() # 启动时加载任务历史

        logging.info(f"{self.log_prefix}DownloadManager initialized.")

    def _setup_ui(self):
        """初始化UI元素"""
        self.overall_main_layout = QVBoxLayout(self)
        self.tab_widget = QTabWidget()
        self.overall_main_layout.addWidget(self.tab_widget)

        self.video_download_page = QWidget()
        self.tab_widget.addTab(self.video_download_page, "视频下载")
        self.video_download_content_layout = QVBoxLayout(self.video_download_page)

        # --- 输入和控制区域 ---
        input_layout = QHBoxLayout()
        self.video_download_content_layout.addLayout(input_layout)

        # 左侧：URL输入和主要操作按钮
        vbox_left = QVBoxLayout()
        input_layout.addLayout(vbox_left, 3) # 调整左侧宽度比例
        vbox_left.addWidget(QLabel("输入视频或播放列表链接（支持多行分别解析）："))
        self.text_urls = QTextEdit()
        self.text_urls.setPlaceholderText("每行一个视频链接或一个播放列表链接。\n解析时会分别处理每一行。")
        vbox_left.addWidget(self.text_urls)

        # === NEW: 链接解析参数输入框 ===
        vbox_left.addWidget(QLabel("链接解析yt-dlp参数 (例如: --user-agent \"...\"):"))
        self.line_fetch_extra_args = QLineEdit()
        self.line_fetch_extra_args.setPlaceholderText("--user-agent \"UA\" --referer \"URL\" 等")
        vbox_left.addWidget(self.line_fetch_extra_args)
        # === END NEW ===

        hbtns_main_ops = QHBoxLayout()
        vbox_left.addLayout(hbtns_main_ops)
        self.btn_fetch = QPushButton("解析链接/列表")
        self.btn_fetch.clicked.connect(self.fetch_links_from_input)
        hbtns_main_ops.addWidget(self.btn_fetch)
        self.btn_start_all = QPushButton("全部开始")
        self.btn_start_all.clicked.connect(self.start_all_tasks)
        hbtns_main_ops.addWidget(self.btn_start_all)
        self.btn_pause_all = QPushButton("全部暂停")
        self.btn_pause_all.clicked.connect(self.pause_all_active_tasks)
        hbtns_main_ops.addWidget(self.btn_pause_all)
        self.btn_resume_selected = QPushButton("继续选中")
        self.btn_resume_selected.clicked.connect(self.resume_selected_task)
        hbtns_main_ops.addWidget(self.btn_resume_selected)
        self.btn_delete_selected = QPushButton("删除选中")
        self.btn_delete_selected.clicked.connect(self.delete_selected_tasks)
        hbtns_main_ops.addWidget(self.btn_delete_selected)

        # 右侧：下载参数设置
        vbox_right_settings = QVBoxLayout()
        input_layout.addLayout(vbox_right_settings, 2) # 调整右侧宽度比例
        
        hfolder = QHBoxLayout()
        vbox_right_settings.addLayout(hfolder)
        hfolder.addWidget(QLabel("保存路径:"))
        self.line_folder = QLineEdit(os.path.join(os.path.expanduser("~"), "Downloads"))
        hfolder.addWidget(self.line_folder)
        btn_browse_folder = QPushButton("选择")
        btn_browse_folder.clicked.connect(self.choose_folder)
        hfolder.addWidget(btn_browse_folder)

        hcookie_browser = QHBoxLayout()
        vbox_right_settings.addLayout(hcookie_browser)
        hcookie_browser.addWidget(QLabel("Cookies (浏览器):"))
        self.combo_cookies = QComboBox()
        self.combo_cookies.addItems(['无', 'firefox', 'chrome', 'edge', 'brave', 'vivaldi', 'opera', 'safari'])
        hcookie_browser.addWidget(self.combo_cookies)

        hcookie_file = QHBoxLayout()
        vbox_right_settings.addLayout(hcookie_file)
        hcookie_file.addWidget(QLabel("Cookies (文件):"))
        self.line_cookies_file = QLineEdit()
        self.line_cookies_file.setPlaceholderText("可选 cookies.txt 文件路径")
        hcookie_file.addWidget(self.line_cookies_file)
        btn_browse_cookies_file = QPushButton("选择文件")
        btn_browse_cookies_file.clicked.connect(self.choose_cookies_file)
        hcookie_file.addWidget(btn_browse_cookies_file)

        hconv_mode = QHBoxLayout()
        vbox_right_settings.addLayout(hconv_mode)
        hconv_mode.addWidget(QLabel("转换模式:"))
        self.combo_conv_mode = QComboBox()
        self.combo_conv_mode.addItems(['无转换', '音频提取转换', '视频格式转换'])
        hconv_mode.addWidget(self.combo_conv_mode)
        
        hconv_fmt = QHBoxLayout()
        vbox_right_settings.addLayout(hconv_fmt)
        hconv_fmt.addWidget(QLabel("目标格式 (转换):"))
        self.combo_conv_fmt = QComboBox()
        self.combo_conv_fmt.addItems(['mp3', 'm4a', 'opus', 'aac', 'flac', 'wav',
                                      'mp4', 'mkv', 'webm', 'mov', 'avi', 'ogg'])
        self.combo_conv_fmt.setEditable(True)
        self.combo_conv_fmt.setPlaceholderText("如 mp3, mkv (留空则默认)")
        hconv_fmt.addWidget(self.combo_conv_fmt)

        hvideo_quality_preset = QHBoxLayout()
        vbox_right_settings.addLayout(hvideo_quality_preset)
        hvideo_quality_preset.addWidget(QLabel("视频质量:"))
        self.combo_video_quality_preset = QComboBox()
        self.combo_video_quality_preset.addItems([
            "最佳 (默认)",
            "4K (2160p)", "2K (1440p)", "1080p", "720p", "480p", "360p",
            "仅音频 (最佳)", "仅音频 (aac)", "仅音频 (mp3)"
        ])
        self.combo_video_quality_preset.setToolTip(
            "选择预设的视频质量。\n"
            "'最佳' 使用yt-dlp默认选择。\n"
            "p结尾的选项会尝试选择该分辨率下的最佳视频和音频。\n"
            "'仅音频' 选项会尝试只下载音频。"
        )
        self.combo_video_quality_preset.setCurrentText("最佳 (默认)")
        hvideo_quality_preset.addWidget(self.combo_video_quality_preset)

        haudio_quality = QHBoxLayout()
        vbox_right_settings.addLayout(haudio_quality)
        haudio_quality.addWidget(QLabel("音频质量 (提取时):"))
        self.combo_audio_quality = QComboBox()
        self.combo_audio_quality.addItems([
            "最佳 (0)", "较好 (2)", "标准 (5)", "较低 (7)", "最差 (9)",
            "320K", "256K", "192K", "128K", "96K", "64K"
        ])
        self.combo_audio_quality.setCurrentText("最佳 (0)")
        self.combo_audio_quality.setPlaceholderText("用于 -x 模式")
        haudio_quality.addWidget(self.combo_audio_quality)

        hlimit = QHBoxLayout()
        vbox_right_settings.addLayout(hlimit)
        hlimit.addWidget(QLabel("限速:"))
        self.line_limit_rate = QLineEdit()
        self.line_limit_rate.setPlaceholderText("500K, 1.5M (空不限)")
        hlimit.addWidget(self.line_limit_rate)

        hpost = QHBoxLayout()
        vbox_right_settings.addLayout(hpost)
        hpost.addWidget(QLabel("后处理脚本:"))
        self.line_post_script = QLineEdit()
        self.line_post_script.setPlaceholderText("可选 .py 脚本的完整路径")
        hpost.addWidget(self.line_post_script)
        btn_post_browse = QPushButton("选择")
        btn_post_browse.clicked.connect(self.choose_post_script)
        hpost.addWidget(btn_post_browse)
        
        hextra = QHBoxLayout()
        vbox_right_settings.addLayout(hextra)
        # === MODIFIED: Label for download extra args ===
        hextra.addWidget(QLabel("yt-dlp 参数 (下载时):"))
        self.line_extra_args = QLineEdit()
        self.line_extra_args.setPlaceholderText("--no-mtime --embed-thumbnail 等")
        hextra.addWidget(self.line_extra_args)

        hconcur = QHBoxLayout()
        vbox_right_settings.addLayout(hconcur)
        self.label_concur = QLabel("同时下载:")
        hconcur.addWidget(self.label_concur)
        self.spin_concur = QSpinBox()
        self.spin_concur.setMinimum(1); self.spin_concur.setMaximum(100)
        self.spin_concur.setValue(self.max_concurrent)
        self.spin_concur.valueChanged.connect(self.on_max_concurrent_changed)
        hconcur.addWidget(self.spin_concur)
        self.checkbox_unlimited = QCheckBox("不限")
        self.checkbox_unlimited.stateChanged.connect(self.on_unlimited_toggled)
        hconcur.addWidget(self.checkbox_unlimited)
        vbox_right_settings.addStretch()

        self.table = QTableWidget(0, 10)
        self.table.setHorizontalHeaderLabels(["ID", "标题", "链接", "状态", "进度", "速度", "保存路径", "打开目录", "操作", "控制"])
        self.video_download_content_layout.addWidget(self.table)
        self._set_table_column_widths()
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)

    def _set_table_column_widths(self):
        self.table.setColumnWidth(0, 40)
        self.table.setColumnWidth(1, 250)
        self.table.setColumnWidth(2, 180)
        self.table.setColumnWidth(3, 120)
        self.table.setColumnWidth(4, 170)
        self.table.setColumnWidth(5, 85)
        self.table.setColumnWidth(6, 200)
        self.table.setColumnWidth(7, 70)
        self.table.setColumnWidth(8, 70)
        self.table.setColumnWidth(9, 70)

    def get_tasks_file_path(self):
        return os.path.join(APPLICATION_DATA_DIRECTORY, TASKS_HISTORY_FILE_NAME)

    def save_tasks_to_file(self):
        logging.debug(f"{self.log_prefix}Saving tasks to file.")
        data_to_save = {
            "task_id_counter": self.task_id_counter,
            "tasks": []
        }
        for task_id, task_data in self.tasks.items():
            task_copy = task_data.copy()
            task_copy.pop("worker", None)
            
            if not isinstance(task_copy.get("params"), dict):
                 task_copy["params"] = {}
            
            task_copy["params"].setdefault("video_format", "")
            task_copy["params"].setdefault("audio_quality", "0")
            task_copy["params"].setdefault("selected_quality_preset", "最佳 (默认)")

            data_to_save["tasks"].append(task_copy)

        file_path = self.get_tasks_file_path()
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=4, ensure_ascii=False)
            logging.info(f"{self.log_prefix}Tasks saved to {file_path}")
        except IOError as e:
            logging.error(f"{self.log_prefix}Error saving tasks to {file_path}: {e}", exc_info=True)
        except TypeError as e:
            logging.error(f"{self.log_prefix}Error serializing tasks for saving: {e}", exc_info=True)


    def load_tasks_from_file(self):
        logging.debug(f"{self.log_prefix}Loading tasks from file.")
        file_path = self.get_tasks_file_path()
        if not os.path.exists(file_path):
            logging.info(f"{self.log_prefix}Tasks file {file_path} not found, starting fresh.")
            return

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"{self.log_prefix}Error loading tasks from {file_path}: {e}", exc_info=True)
            QMessageBox.warning(self, "加载错误", f"无法从 {file_path} 加载任务列表:\n{e}")
            try:
                corrupted_file_path = file_path + f".corrupted_{QDateTime.currentDateTime().toString('yyyyMMdd_HHmmss')}"
                os.rename(file_path, corrupted_file_path)
                logging.info(f"{self.log_prefix}Corrupted tasks file renamed to {corrupted_file_path}")
                QMessageBox.information(self, "提示", f"损坏的任务历史文件已备份为:\n{corrupted_file_path}")
            except OSError as re: logging.error(f"{self.log_prefix}Could not rename corrupted tasks file: {re}")
            return

        self.task_id_counter = loaded_data.get("task_id_counter", 0)
        loaded_tasks_list = loaded_data.get("tasks", [])
        max_loaded_id_val = 0

        for task_data_dict in loaded_tasks_list:
            task_id_from_file = task_data_dict.get("id")
            if not task_id_from_file:
                logging.warning(f"{self.log_prefix}Loaded task data missing 'id', skipping: {task_data_dict.get('title', 'N/A')}")
                continue

            status = task_data_dict.get("status", "等待")
            if status in ["下载中...", "启动中", "准备下载", "排队中"]:
                task_data_dict["status"] = "已暂停(中断)"
                task_data_dict["paused"] = True
            elif status == "错误" or status.startswith("失败") or "失败" in status:
                task_data_dict["failed"] = True
            else: task_data_dict["paused"] = (status == "暂停" or status == "已暂停(中断)")
            
            if not isinstance(task_data_dict.get("params"), dict):
                task_data_dict["params"] = {}
            
            task_data_dict["params"].setdefault("video_format", "")
            task_data_dict["params"].setdefault("audio_quality", "0")
            task_data_dict["params"].setdefault("selected_quality_preset", "最佳 (默认)")

            self.add_task_to_table(
                url=task_data_dict.get("url",""), title=task_data_dict.get("title","N/A"),
                task_id_override=task_id_from_file, initial_data=task_data_dict
            )
            try:
                current_id_val = int(task_id_from_file)
                if current_id_val > max_loaded_id_val: max_loaded_id_val = current_id_val
            except ValueError: pass

        self.task_id_counter = max(self.task_id_counter, max_loaded_id_val)
        logging.info(f"{self.log_prefix}{len(self.tasks)} tasks loaded. Next task ID will be based on {self.task_id_counter + 1}")
        self.update_all_task_row_indices()
        for task_id_loaded in self.tasks: self.update_task_ui(task_id_loaded)
        self.btn_start_all.setEnabled(self.table.rowCount() > 0)

    def add_task_to_table(self, url, title, task_id_override=None, initial_data=None):
        if initial_data and task_id_override and task_id_override in self.tasks:
            logging.info(f"{self.log_prefix}Task {task_id_override} already loaded/exists. Updating data.")
            existing_task_data = self.tasks[task_id_override]
            existing_task_data.update(initial_data)
            existing_task_data["worker"] = None
            existing_task_data["paused"] = initial_data.get("status") in ["已暂停(中断)", "暂停"]
            
            if not isinstance(existing_task_data.get("params"), dict):
                existing_task_data["params"] = {}
            loaded_params = initial_data.get("params", {})
            existing_task_data["params"].setdefault("video_format", loaded_params.get("video_format", ""))
            existing_task_data["params"].setdefault("audio_quality", loaded_params.get("audio_quality", "0"))
            existing_task_data["params"].setdefault("selected_quality_preset", loaded_params.get("selected_quality_preset", "最佳 (默认)"))

            self.update_task_ui(task_id_override)
            return task_id_override
        elif not initial_data:
            for r in range(self.table.rowCount()):
                item = self.table.item(r, 2)
                if item and item.text() == url:
                    logging.info(f"{self.log_prefix}Task with URL '{url}' already exists. Skipping.")
                    return None

        current_task_id = ""
        if initial_data and task_id_override:
            current_task_id = task_id_override
        else:
            self.task_id_counter += 1
            current_task_id = str(self.task_id_counter)

        row_position = self.table.rowCount()
        self.table.insertRow(row_position)

        status_val = "等待"
        progress_val = ""
        speed_val = ""
        filepath_val = ""
        paused_val = False
        failed_val = False
        params_val = {}
        task_title = title

        if initial_data:
            task_title = initial_data.get("title", title)
            status_val = initial_data.get("status", "等待")
            progress_val = initial_data.get("progress", "")
            speed_val = initial_data.get("speed", "")
            filepath_val = initial_data.get("filepath", "")
            paused_val = initial_data.get("paused", status_val in ["已暂停(中断)", "暂停"])
            failed_val = initial_data.get("failed", status_val == "错误" or "失败" in status_val)
            
            loaded_params = initial_data.get("params")
            if isinstance(loaded_params, dict):
                params_val = loaded_params.copy()
            params_val.setdefault("video_format", "")
            params_val.setdefault("audio_quality", "0")
            params_val.setdefault("selected_quality_preset", "最佳 (默认)")
        else:
            current_ui_params = self.get_current_download_parameters()
            if current_ui_params:
                params_val = current_ui_params.copy()
            else:
                logging.error(f"{self.log_prefix}Failed to get UI params for new task {title}. Using defaults.")
                params_val = {
                    "video_format": "", "audio_quality": "0", "selected_quality_preset": "最佳 (默认)",
                    "output_dir": os.path.join(os.path.expanduser("~"), "Downloads")
                }
        
        task_entry_base = {
            "id": current_task_id, "url": url, "title": task_title, "status": status_val,
            "progress": progress_val, "speed": speed_val, "filepath": filepath_val,
            "worker": None, "row": row_position, "paused": paused_val, "failed": failed_val,
            "in_queue": False, "params": params_val,
            "_marked_for_deletion_while_active": False
        }
        
        final_task_entry = initial_data.copy() if initial_data else {}
        final_task_entry.update(task_entry_base)
        
        if not isinstance(final_task_entry.get("params"), dict):
            final_task_entry["params"] = {}
        final_task_entry["params"].setdefault("video_format", params_val.get("video_format", ""))
        final_task_entry["params"].setdefault("audio_quality", params_val.get("audio_quality", "0"))
        final_task_entry["params"].setdefault("selected_quality_preset", params_val.get("selected_quality_preset", "最佳 (默认)"))

        self.tasks[current_task_id] = final_task_entry
        logging.debug(f"{self.log_prefix}Task {current_task_id} ('{task_title}') add/load. Row:{row_position}, Stat:{status_val}, Params: {final_task_entry['params']}")

        self.table.setItem(row_position, 0, QTableWidgetItem(current_task_id))
        self.table.setItem(row_position, 1, QTableWidgetItem(task_title))
        self.table.setItem(row_position, 2, QTableWidgetItem(url))

        btn_open_dir = QPushButton("打开")
        btn_open_dir.clicked.connect(lambda _, tid=current_task_id: self.open_containing_folder(tid))
        self.table.setCellWidget(row_position, 7, btn_open_dir)

        btn_retry = QPushButton("重试")
        btn_retry.clicked.connect(lambda _, tid=current_task_id: self.retry_task(tid))
        self.table.setCellWidget(row_position, 8, btn_retry)

        btn_ctrl = QPushButton("控制")
        btn_ctrl.clicked.connect(lambda _, tid=current_task_id: self.toggle_pause_resume_task(tid))
        self.table.setCellWidget(row_position, 9, btn_ctrl)

        self.update_task_ui(current_task_id)

        if not initial_data:
            self.save_tasks_to_file()

        self.btn_start_all.setEnabled(True)
        return current_task_id

    def update_task_ui(self, task_id):
        task_data = self.tasks.get(task_id)
        if not task_data:
            logging.debug(f"{self.log_prefix}update_task_ui: Task {task_id} not found.")
            return

        current_row = -1
        for r_idx in range(self.table.rowCount()):
            id_item = self.table.item(r_idx, 0)
            if id_item and id_item.text() == task_id:
                current_row = r_idx
                if task_data.get("row") != r_idx:
                    task_data["row"] = r_idx
                break
        
        if current_row == -1:
            logging.warning(f"{self.log_prefix}Task {task_id} UI update failed: row not found in table.")
            return
        row = current_row

        status_display = task_data.get("status", "未知")
        if task_data.get("_marked_for_deletion_while_active") and status_display not in ["错误", "失败", "完成"]:
            status_display = f"停止中...({status_display})"

        for col_idx in range(self.table.columnCount()):
            if col_idx not in [7, 8, 9]: # Skip columns with widgets
                if not self.table.item(row, col_idx):
                    self.table.setItem(row, col_idx, QTableWidgetItem(""))

        self.table.item(row, 1).setText(task_data.get("title", ""))
        self.table.item(row, 2).setText(task_data.get("url", ""))
        self.table.item(row, 3).setText(status_display)
        self.table.item(row, 4).setText(task_data.get("progress", ""))
        self.table.item(row, 5).setText(task_data.get("speed", ""))
        filepath_val = task_data.get("filepath", "")
        self.table.item(row, 6).setText(os.path.basename(filepath_val) if filepath_val else "")

        btn_open_dir = self.table.cellWidget(row, 7)
        if btn_open_dir:
            can_open_dir_from_file = bool(filepath_val and os.path.exists(filepath_val))
            can_open_dir_from_params = False
            params_dict = task_data.get("params")
            if isinstance(params_dict, dict):
                output_dir_val = params_dict.get("output_dir")
                if output_dir_val and os.path.isdir(output_dir_val):
                    can_open_dir_from_params = True
            btn_open_dir.setEnabled(can_open_dir_from_file or can_open_dir_from_params)

        btn_retry = self.table.cellWidget(row, 8)
        if btn_retry:
            btn_retry.setEnabled(task_data.get("failed", False) and not task_data.get("_marked_for_deletion_while_active"))

        btn_ctrl = self.table.cellWidget(row, 9)
        if btn_ctrl:
            current_status = task_data.get("status", "")
            if task_data.get("_marked_for_deletion_while_active"):
                btn_ctrl.setText("删除中"); btn_ctrl.setEnabled(False)
            else:
                worker_is_running = task_data.get("worker") and task_data.get("worker").isRunning()
                if current_status in ["下载中...", "准备下载", "启动中", "排队中"] or worker_is_running:
                    btn_ctrl.setText("暂停"); btn_ctrl.setEnabled(True)
                elif task_data.get("paused", False) or current_status in ["暂停", "已暂停(中断)"]:
                    btn_ctrl.setText("继续"); btn_ctrl.setEnabled(True)
                elif current_status in ["完成", "失败", "错误", "完成但路径未知", "完成但找不到文件", "完成但路径捕获失败"] or "错误(" in current_status:
                    btn_ctrl.setText("---"); btn_ctrl.setEnabled(False)
                elif current_status == "等待":
                    btn_ctrl.setText("开始"); btn_ctrl.setEnabled(True)
                else:
                    btn_ctrl.setText("控制"); btn_ctrl.setEnabled(False) # Default for unknown states

    def get_current_download_parameters(self):
        output_dir = self.line_folder.text().strip()
        if not output_dir :
            output_dir = os.path.join(os.path.expanduser("~"), "Downloads")
            self.line_folder.setText(output_dir)
            logging.info(f"{self.log_prefix}保存路径为空，使用默认路径: {output_dir}")

        if not os.path.isdir(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
                logging.info(f"{self.log_prefix}下载目录不存在，已创建: {output_dir}")
            except Exception as e:
                logging.error(f"{self.log_prefix}指定的保存路径无效或无法创建: {output_dir}\n{e}", exc_info=True)
                QMessageBox.warning(self, "路径错误", f"指定的保存路径无效或无法创建: {output_dir}\n请检查路径或权限。")
                return None
        
        audio_quality_text_ui = self.combo_audio_quality.currentText()
        audio_quality_val = audio_quality_text_ui
        if "(" in audio_quality_text_ui and ")" in audio_quality_text_ui:
            try:
                audio_quality_val = audio_quality_text_ui.split('(')[1].split(')')[0].strip()
            except IndexError:
                logging.warning(f"无法从 {audio_quality_text_ui} 解析音频质量值，使用原始文本")
        else:
            audio_quality_val = audio_quality_text_ui.strip()

        selected_quality_preset = self.combo_video_quality_preset.currentText()
        video_format_code = ""

        if selected_quality_preset == "最佳 (默认)":
            video_format_code = "" # yt-dlp default
        elif selected_quality_preset == "4K (2160p)":
            video_format_code = "bestvideo[height<=?2160]+bestaudio/best[height<=?2160]"
        elif selected_quality_preset == "2K (1440p)":
            video_format_code = "bestvideo[height<=?1440]+bestaudio/best[height<=?1440]"
        elif selected_quality_preset == "1080p":
            video_format_code = "bestvideo[height<=?1080]+bestaudio/best[height<=?1080]"
        elif selected_quality_preset == "720p":
            video_format_code = "bestvideo[height<=?720]+bestaudio/best[height<=?720]"
        elif selected_quality_preset == "480p":
            video_format_code = "bestvideo[height<=?480]+bestaudio/best[height<=?480]"
        elif selected_quality_preset == "360p":
            video_format_code = "bestvideo[height<=?360]+bestaudio/best[height<=?360]"
        elif selected_quality_preset == "仅音频 (最佳)":
            video_format_code = "bestaudio/best"
        elif selected_quality_preset == "仅音频 (aac)":
            video_format_code = "bestaudio[ext=m4a]/bestaudio[acodec=aac]" # More specific
        elif selected_quality_preset == "仅音频 (mp3)":
            # For mp3, yt-dlp typically extracts best audio then converts.
            # So format selection is 'bestaudio/best', and conversion mode + target format handle the mp3 part.
            video_format_code = "bestaudio/best"
            logging.info("为 '仅音频 (mp3)' 预设选择了 'bestaudio/best'，请配合使用 '音频提取转换' 和 'mp3' 目标格式。")
        
        return {
            "output_dir": output_dir,
            "cookies_browser": self.combo_cookies.currentText(),
            "cookies_file_path": self.line_cookies_file.text().strip(),
            "conv_mode": self.combo_conv_mode.currentText(),
            "conv_fmt": self.combo_conv_fmt.currentText().strip(),
            "video_format": video_format_code, # This is the -f format code
            "selected_quality_preset": selected_quality_preset, # Store the user-friendly preset name
            "audio_quality": audio_quality_val, # For -x --audio-quality
            "limit_rate": self.line_limit_rate.text().strip(),
            "post_script": self.line_post_script.text().strip(),
            "extra_args": self.line_extra_args.text().strip() # These are for download worker
        }

    def enqueue_task(self, task_id):
        task_data = self.tasks.get(task_id)
        if not task_data:
            logging.warning(f"{self.log_prefix}enqueue_task: Task {task_id} not found.")
            return
        if task_data.get("_marked_for_deletion_while_active"):
            logging.info(f"{self.log_prefix}Task {task_id} marked for deletion, not enqueued.")
            return
        if task_data.get("in_queue", False) or (task_data.get("worker") and task_data.get("worker").isRunning()):
            logging.debug(f"{self.log_prefix}Task {task_id} already running or in queue.")
            return

        params_are_valid = False
        current_task_params = task_data.get("params")
        if isinstance(current_task_params, dict) and current_task_params:
            if current_task_params.get("output_dir") and os.path.isdir(current_task_params.get("output_dir","")):
                params_are_valid = True
            else:
                logging.warning(f"{self.log_prefix}Task {task_id} existing output_dir '{current_task_params.get('output_dir')}' is invalid or missing.")
        
        if not params_are_valid:
            logging.info(f"{self.log_prefix}Task {task_id} missing valid params or output_dir, using current UI settings.")
            current_ui_params = self.get_current_download_parameters()
            if current_ui_params is None: # e.g., UI output_dir was invalid
                task_data["status"] = "错误(参数)"; task_data["failed"] = True
                self.update_task_ui(task_id)
                self.save_tasks_to_file()
                return
            task_data["params"] = current_ui_params.copy() # Use a copy
            logging.debug(f"{self.log_prefix}Task {task_id} assigned current UI parameters: {task_data['params']}")
        else:
            # Params were considered valid (e.g., had output_dir),
            # but might be old and missing newer format selection parameters.
            # We need to get the current UI's format settings to use as defaults if keys are missing.
            current_ui_settings_for_fallback = self.get_current_download_parameters()
            if current_ui_settings_for_fallback is None: # Should not happen if output_dir was valid before
                 logging.error(f"{self.log_prefix}Critical error getting UI params during enqueue for task {task_id}. Using hardcoded defaults for missing keys.")
                 current_ui_settings_for_fallback = {
                     "video_format": "",
                     "selected_quality_preset": "最佳 (默认)",
                     "audio_quality": "0" # yt-dlp default for -x
                 }

            task_data["params"].setdefault("video_format", current_ui_settings_for_fallback.get("video_format"))
            task_data["params"].setdefault("selected_quality_preset", current_ui_settings_for_fallback.get("selected_quality_preset"))
            task_data["params"].setdefault("audio_quality", current_ui_settings_for_fallback.get("audio_quality"))
            
            logging.debug(f"{self.log_prefix}Task {task_id} using its pre-existing params, ensured new keys: {task_data['params']}")
        
        task_data["failed"] = False # Reset failed status
        task_data["paused"] = False
        task_data["status"] = "排队中"
        task_data["in_queue"] = True
        if task_id not in self.task_queue:
            self.task_queue.append(task_id)
        
        self.update_task_ui(task_id)
        logging.info(f"{self.log_prefix}Task {task_id} ('{task_data.get('title', 'N/A')}') enqueued. Queue length: {len(self.task_queue)}")
        self.save_tasks_to_file() # Save state after enqueuing

    def start_all_tasks(self):
        logging.info(f"{self.log_prefix}start_all_tasks called.")
        if not self.tasks: QMessageBox.information(self, "提示", "任务列表为空。"); return

        queued_count = 0
        for task_id, task_data in self.tasks.items():
            if task_data.get("_marked_for_deletion_while_active"): continue
            # Start tasks that are waiting, paused, or failed
            if task_data.get("status") in ["等待", "暂停", "已暂停(中断)"] or task_data.get("failed", False):
                # Ensure it's not already running (worker check is implicit in enqueue_task)
                if not (task_data.get("worker") and task_data.get("worker").isRunning()):
                    self.enqueue_task(task_id)
                    queued_count +=1
        if queued_count > 0: logging.info(f"{self.log_prefix}{queued_count} tasks enqueued by start_all.")
        else: logging.info(f"{self.log_prefix}No new tasks to start via start_all.")
        self.check_and_start_tasks() # Trigger processing the queue

    def check_and_start_tasks(self):
        # Prune queue of tasks that might have been deleted or are invalid
        valid_queue = [tid for tid in self.task_queue if self.tasks.get(tid) and not self.tasks[tid].get("_marked_for_deletion_while_active")]
        if len(valid_queue) != len(self.task_queue):
            logging.debug(f"Task queue pruned from {len(self.task_queue)} to {len(valid_queue)}")
            self.task_queue = valid_queue

        while self.active_workers < self.max_concurrent and self.task_queue:
            if not self.task_queue: break # Should be redundant due to while condition but safe
            task_id_to_start = self.task_queue.pop(0) # Get from front of queue
            task_data = self.tasks.get(task_id_to_start)

            if not task_data: # Should have been pruned but double check
                logging.warning(f"{self.log_prefix}Task {task_id_to_start} vanished before start attempt."); continue
            if task_data.get("_marked_for_deletion_while_active"):
                logging.info(f"{self.log_prefix}Task {task_id_to_start} is marked for deletion, not starting."); continue

            # Check if task is already running or completed (shouldn't be in queue if so, but defensive)
            if (task_data.get("worker") and task_data.get("worker").isRunning()) or \
               task_data.get("status") in ["完成", "下载中...", "启动中", "准备下载"]: # "准备下载" means worker is about to start
                task_data["in_queue"] = False # Ensure it's marked as not in queue
                logging.debug(f"{self.log_prefix}Task {task_id_to_start} status '{task_data.get('status')}', skipping start from queue.")
                continue
            
            # Final check on params before starting worker
            task_params = task_data.get("params", {})
            if not isinstance(task_params, dict) or not task_params.get("output_dir") or \
               not os.path.isdir(task_params.get("output_dir")):
                logging.error(f"{self.log_prefix}Task {task_id_to_start} ('{task_data.get('title')}') has invalid params/output_dir at start, cannot start. Params: {task_params}")
                task_data["status"] = "错误(参数/路径)"; task_data["failed"] = True; task_data["in_queue"] = False
                self.on_task_error_custom(task_id_to_start, "启动前检查：无效下载参数或保存路径。") # Use custom error handler
                continue
            
            self.start_task_thread(task_id_to_start)

    def start_task_thread(self, task_id):
        task_data = self.tasks.get(task_id)
        
        # Critical check, should always have params by now due to enqueue_task logic
        if not task_data or not isinstance(task_data.get("params"), dict):
            logging.error(f"{self.log_prefix}CRITICAL: start_task_thread called for task {task_id} with missing/invalid params.")
            if task_data: # If task_data exists but params are bad
                task_data["in_queue"] = False; task_data["status"] = "错误(内部)"; self.update_task_ui(task_id); self.save_tasks_to_file()
            return

        if task_data.get("_marked_for_deletion_while_active"): # Double check
            logging.info(f"{self.log_prefix}Task {task_id} marked for deletion, cancelling start_task_thread."); return

        self.active_workers += 1
        task_data.update({"in_queue": False, "paused": False, "failed": False, "status": "启动中"})
        self.update_task_ui(task_id)

        params_for_worker = task_data["params"] # Already ensured to be a dict
        worker = DownloadTaskWorker(
            task_id,
            task_data["url"],
            task_data["title"],
            output_dir=params_for_worker.get("output_dir"), # Known to be valid dir
            cookies_browser=params_for_worker.get("cookies_browser"),
            cookies_file_path=params_for_worker.get("cookies_file_path"),
            conv_mode=params_for_worker.get("conv_mode"),
            conv_fmt=params_for_worker.get("conv_fmt"),
            limit_rate=params_for_worker.get("limit_rate"),
            post_script=params_for_worker.get("post_script"),
            extra_args=params_for_worker.get("extra_args"), # For download
            video_format=params_for_worker.get("video_format"), # Actual -f format
            audio_quality=params_for_worker.get("audio_quality") # For -x
        )
        task_data["worker"] = worker

        worker.progress_signal.connect(self.on_task_progress)
        worker.status_signal.connect(self.on_task_status)
        worker.speed_signal.connect(self.on_task_speed)
        worker.finished_signal.connect(self.on_task_finished_custom) # Renamed for clarity
        worker.error_signal.connect(self.on_task_error_custom)     # Renamed for clarity

        logging.info(f"{self.log_prefix}Starting task {task_id} ('{task_data.get('title', 'N/A')}'). Active workers: {self.active_workers}. Params: {params_for_worker}")
        worker.start()
    
    def on_task_finished_custom(self, task_id, result_or_filepath):
        logging.debug(f"{self.log_prefix}on_task_finished for task {task_id}, result: {result_or_filepath}")
        task_data = self.tasks.get(task_id)
        if not task_data: logging.warning(f"{self.log_prefix}Task {task_id} finished but not found in self.tasks."); return

        worker_that_finished = task_data.pop("worker", None) # Remove worker reference
        if worker_that_finished: # Only decrement if a worker was actually associated
            self.active_workers = max(0, self.active_workers - 1) # Decrement active workers
        
        logging.info(f"{self.log_prefix}Task {task_id} ('{task_data.get('title', 'N/A')}') ended. Result: {result_or_filepath}. Active workers: {self.active_workers}")

        if result_or_filepath == "失败": task_data.update({"status":"失败", "failed":True, "paused":False}); self.failed_tasks.add(task_id)
        elif result_or_filepath == "暂停": task_data.update({"status":"暂停", "paused":True, "failed":False }) # Worker was stopped
        elif result_or_filepath == "完成但路径未知": task_data.update({"status":"完成但路径未知", "filepath":"", "failed":False, "paused":False})
        elif result_or_filepath == "完成但找不到文件": task_data.update({"status":"失败(文件丢失)", "failed":True, "filepath":"", "paused":False}) # Treat as failure
        elif result_or_filepath == "完成但路径捕获失败": task_data.update({"status":"完成但路径捕获失败", "filepath":"", "failed":False, "paused":False})
        else: # Assumed to be a valid filepath
            task_data.update({"status":"完成", "filepath":result_or_filepath, "progress":"100%", "speed":"", "failed":False, "paused":False})

        # Handle deletion if marked during active state
        if task_data.get("_marked_for_deletion_while_active"):
            logging.info(f"{self.log_prefix}Task {task_id} (marked for deletion) finished/paused, performing final removal.")
            if task_id in self.tasks: # Check if not already deleted by another path
                current_row_for_deletion = self.tasks[task_id].get("row", -1) # Get row BEFORE deleting from self.tasks
                del self.tasks[task_id]
                self.failed_tasks.discard(task_id) # Remove from failed set if it was there

                # Find and remove row from QTableWidget
                # Iterate backwards to handle row index changes correctly if multiple rows are removed
                # Or, find by ID if row index isn't perfectly reliable
                if current_row_for_deletion != -1 and current_row_for_deletion < self.table.rowCount():
                    id_item_check = self.table.item(current_row_for_deletion, 0)
                    if id_item_check and id_item_check.text() == task_id:
                         self.table.removeRow(current_row_for_deletion)
                    else: # Row index might have shifted, find by ID
                        for r_idx in range(self.table.rowCount() -1, -1, -1):
                            id_item_at_row = self.table.item(r_idx, 0)
                            if id_item_at_row and id_item_at_row.text() == task_id:
                                self.table.removeRow(r_idx)
                                break
                self.update_all_task_row_indices() # Update row indices for remaining tasks
        else:
            self.update_task_ui(task_id) # Update UI for normally finished/paused task
        
        self.save_tasks_to_file() # Save changes
        self.check_and_start_tasks() # Check if new tasks can be started

    def on_task_error_custom(self, task_id, error_msg):
        logging.debug(f"{self.log_prefix}on_task_error for task {task_id}, error: {error_msg}")
        task_data = self.tasks.get(task_id)
        if not task_data: logging.warning(f"{self.log_prefix}Task {task_id} errored but not found in self.tasks."); return

        worker_that_errored = task_data.pop("worker", None) # Remove worker reference
        if worker_that_errored:
            self.active_workers = max(0, self.active_workers - 1)

        logging.error(f"{self.log_prefix}Error - Task {task_id} ('{task_data.get('title', 'N/A')}'): {error_msg}. Active workers: {self.active_workers}")
        task_data.update({"status":"错误", "failed":True, "paused":False}); self.failed_tasks.add(task_id)

        # Handle deletion if marked during active state
        if task_data.get("_marked_for_deletion_while_active"):
            logging.info(f"{self.log_prefix}Task {task_id} (marked for deletion) errored, performing final removal.")
            if task_id in self.tasks:
                current_row_for_deletion = self.tasks[task_id].get("row", -1)
                del self.tasks[task_id]
                self.failed_tasks.discard(task_id)
                if current_row_for_deletion != -1 and current_row_for_deletion < self.table.rowCount():
                    id_item_check = self.table.item(current_row_for_deletion, 0)
                    if id_item_check and id_item_check.text() == task_id:
                         self.table.removeRow(current_row_for_deletion)
                    else:
                        for r_idx in range(self.table.rowCount() -1, -1, -1):
                            id_item_at_row = self.table.item(r_idx, 0)
                            if id_item_at_row and id_item_at_row.text() == task_id:
                                self.table.removeRow(r_idx)
                                break
                self.update_all_task_row_indices()
        else:
            self.update_task_ui(task_id)
        
        self.save_tasks_to_file()
        self.check_and_start_tasks()

    def delete_selected_tasks(self):
        logging.info(f"{self.log_prefix}delete_selected_tasks called.")
        selected_model_indices = self.table.selectionModel().selectedRows()
        if not selected_model_indices:
            QMessageBox.information(self, "提示", "请先选择要删除的任务。")
            return

        tasks_to_delete_ids_rows = {} # Store {task_id: original_row_index}
        for model_index in selected_model_indices:
            row = model_index.row()
            task_id_item = self.table.item(row, 0)
            if task_id_item:
                tasks_to_delete_ids_rows[task_id_item.text()] = row
        
        if not tasks_to_delete_ids_rows: return

        reply = QMessageBox.question(self, "确认删除",
                                     f"确定要从列表中删除选中的 {len(tasks_to_delete_ids_rows)} 个任务吗？\n"
                                     "注意：此操作不会删除已下载的文件。",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.No:
            logging.debug(f"{self.log_prefix}User cancelled deletion.")
            return

        tasks_were_modified = False # Flag to save if any task state changes
        rows_to_remove_from_ui_immediately = [] # Store original row indices for immediate UI removal

        for task_id, original_row_idx in tasks_to_delete_ids_rows.items():
            task_data = self.tasks.get(task_id)
            if task_data:
                if task_data.get("_marked_for_deletion_while_active"):
                    # Already marked, it will be handled by its finish/error handler
                    tasks_were_modified = True # State will change eventually
                    continue # Skip direct deletion here

                worker = task_data.get("worker")
                if worker and worker.isRunning():
                    logging.info(f"{self.log_prefix}Deleting task {task_id}: stopping active worker & marking for deletion.")
                    task_data["_marked_for_deletion_while_active"] = True
                    tasks_were_modified = True
                    worker.stop() # Signal worker to stop
                    self.update_task_ui(task_id) # Update UI to show "Stopping..." or similar
                else:
                    # Task is not active, delete immediately
                    logging.info(f"{self.log_prefix}Deleting task {task_id} (inactive).")
                    if task_data.get("in_queue", False) and task_id in self.task_queue:
                        try: self.task_queue.remove(task_id)
                        except ValueError: pass # Already removed or not found, ignore
                    
                    if task_id in self.tasks: del self.tasks[task_id] # Remove from internal dict
                    self.failed_tasks.discard(task_id) # Remove from failed set
                    rows_to_remove_from_ui_immediately.append(original_row_idx)
                    tasks_were_modified = True
            elif original_row_idx < self.table.rowCount(): # Task not in self.tasks but row exists in UI (should be rare)
                 rows_to_remove_from_ui_immediately.append(original_row_idx)
                 tasks_were_modified = True


        # Remove rows for inactive tasks from UI
        # Sort in reverse to avoid index shifting issues during removal
        if rows_to_remove_from_ui_immediately:
            # Get unique row indices and sort them in reverse
            unique_rows_to_remove = sorted(list(set(rows_to_remove_from_ui_immediately)), reverse=True)
            for r_idx in unique_rows_to_remove:
                if r_idx < self.table.rowCount(): # Check if row still exists
                    self.table.removeRow(r_idx)
            logging.info(f"{self.log_prefix}{len(unique_rows_to_remove)} inactive tasks removed from UI.")
            self.update_all_task_row_indices() # Update row indices after UI removal

        if tasks_were_modified:
            self.save_tasks_to_file() # Save changes (e.g., removed tasks, _marked_for_deletion)

        self.check_and_start_tasks() # May free up slots if active tasks were stopped


    def on_task_status(self, task_id, status_text):
        task_data = self.tasks.get(task_id)
        if not task_data or task_data.get("_marked_for_deletion_while_active"): return # Ignore updates if marked for deletion

        old_status = task_data.get("status", "")
        # Define statuses that are considered final or stable (should not be easily overwritten by transient messages)
        final_or_stable_statuses = [
            "完成", "失败", "错误", "暂停", "已暂停(中断)",
            "完成但路径未知", "完成但找不到文件", "完成但路径捕获失败"
        ] + [s for s in [old_status] if "错误(" in s] # Include specific error messages

        # Define prefixes of important status messages that *can* overwrite a stable status (e.g., post-processing)
        important_status_prefixes = [
            "后处理", "完成:", # yt-dlp sometimes emits "完成: <filename>"
            "yt-dlp code:", "yt-dlp 进程以错误码", "ERROR:", # Critical errors from yt-dlp
            "Destination:", "Merging formats into", "Extracting audio to", "Recoding to" # Path related messages
        ]

        can_overwrite_current_status = True
        if old_status in final_or_stable_statuses:
            # Only overwrite stable status if the new message is an important update
            can_overwrite_current_status = any(status_text.startswith(prefix) for prefix in important_status_prefixes)

        status_actually_changed_in_logic = False
        if can_overwrite_current_status:
            if task_data["status"] != status_text: # Avoid redundant updates
                logging.debug(f"{self.log_prefix}Task {task_id} status from '{task_data['status']}' to: '{status_text}'")
                task_data["status"] = status_text
                status_actually_changed_in_logic = True
        elif task_data["status"] != status_text: # Log if we decided not to update a stable status
             logging.debug(f"{self.log_prefix}Task {task_id}: Did not update stable status '{task_data['status']}' to '{status_text}'")
        
        self.update_task_ui(task_id) # Always update UI to reflect the latest, even if not saved to task_data["status"]

        # Save to file only if a logically significant status changed to a final one
        if status_actually_changed_in_logic and task_data["status"] in final_or_stable_statuses:
            self.save_tasks_to_file()


    def on_task_progress(self, task_id, progress_text):
        task_data = self.tasks.get(task_id)
        if not task_data or task_data.get("_marked_for_deletion_while_active"): return
        
        # Clean up progress text from yt-dlp's [download] prefix if present
        new_progress = progress_text.replace("[download]", "").strip()
        if task_data.get("progress") != new_progress: # Avoid redundant UI updates
            task_data["progress"] = new_progress
            self.update_task_ui(task_id)


    def on_task_speed(self, task_id, speed_text):
        task_data = self.tasks.get(task_id)
        if not task_data or task_data.get("_marked_for_deletion_while_active"): return

        if task_data.get("speed") != speed_text: # Avoid redundant UI updates
            task_data["speed"] = speed_text
            self.update_task_ui(task_id)


    def pause_all_active_tasks(self, clear_queue=True):
        logging.info(f"{self.log_prefix}pause_all_active_tasks called. Clear queue: {clear_queue}")
        active_tasks_signaled_to_stop = 0
        # Iterate over a copy of keys if workers might modify self.tasks upon stopping
        for task_id_iter in list(self.tasks.keys()): # Use list() for safe iteration if tasks can be deleted
            task_data = self.tasks.get(task_id_iter)
            if not task_data or task_data.get("_marked_for_deletion_while_active"): continue

            worker = task_data.get("worker")
            if worker and worker.isRunning():
                logging.debug(f"{self.log_prefix}Pausing worker for task {task_id_iter}")
                worker.stop() # Signal worker to stop
                active_tasks_signaled_to_stop += 1
                # Worker's finished_signal (with "暂停" status) will handle UI update and saving

        if active_tasks_signaled_to_stop > 0:
            logging.info(f"{self.log_prefix}{active_tasks_signaled_to_stop} active tasks signaled to stop.")
        else:
            logging.info(f"{self.log_prefix}No active tasks were running to pause.")

        if clear_queue:
            logging.info(f"{self.log_prefix}Clearing download queue...")
            tasks_updated_from_queue = 0
            for task_id_in_queue in list(self.task_queue): # Iterate over a copy of the queue
                task_data = self.tasks.get(task_id_in_queue)
                if task_data:
                    if task_data.get("_marked_for_deletion_while_active"):
                        try: self.task_queue.remove(task_id_in_queue) # Remove if marked for deletion
                        except ValueError: pass
                        continue

                    task_data["in_queue"] = False
                    if task_data.get("status") == "排队中": # Only update status if it was "排队中"
                        task_data["status"] = "等待" # Or "已暂停(队列)" for clarity
                        task_data["paused"] = True # Mark as paused implicitly
                    self.update_task_ui(task_id_in_queue)
                    tasks_updated_from_queue +=1
            
            self.task_queue.clear() # Clear the original queue
            if tasks_updated_from_queue > 0: # If any task states were changed
                 self.save_tasks_to_file()
            logging.info(f"{self.log_prefix}Download queue cleared. {tasks_updated_from_queue} tasks (if any) updated from queue.")


    def resume_selected_task(self):
        logging.info(f"{self.log_prefix}resume_selected_task called.")
        selected_rows = list(set(index.row() for index in self.table.selectionModel().selectedRows()))
        if not selected_rows:
            QMessageBox.information(self, "提示", "请选择要继续的任务。")
            return
        
        resumed_count = 0
        for row in selected_rows:
            task_id_item = self.table.item(row, 0)
            if not task_id_item: continue
            task_id = task_id_item.text()
            task_data = self.tasks.get(task_id)

            if task_data and not task_data.get("_marked_for_deletion_while_active"):
                # Eligible for resume if paused, waiting, or failed
                if task_data.get("paused") or \
                   task_data.get("status") in ["暂停", "已暂停(中断)", "等待"] or \
                   task_data.get("failed"): # Allow retrying failed tasks with "Resume Selected"
                    
                    # Don't re-enqueue if already running (shouldn't happen if status is correct)
                    if task_data.get("worker") and task_data.get("worker").isRunning():
                        logging.debug(f"{self.log_prefix}Task {task_id} is already running, skipping resume.")
                        continue
                    
                    self.enqueue_task(task_id) # enqueue_task will reset 'failed' and 'paused'
                    resumed_count += 1
        
        if resumed_count > 0:
            logging.info(f"{self.log_prefix}{resumed_count} tasks re-enqueued for resume/start.")
            self.check_and_start_tasks() # Trigger queue processing
        else:
            logging.info(f"{self.log_prefix}No selected tasks were eligible for resume/start.")


    def retry_task(self, task_id):
        logging.info(f"{self.log_prefix}retry_task called for task {task_id}.")
        task_data = self.tasks.get(task_id)
        if not task_data: return
        if task_data.get("_marked_for_deletion_while_active"):
            QMessageBox.warning(self, "提示", f"任务 {task_id} 已标记为删除，无法重试。"); return
        
        if task_data.get("worker") and task_data.get("worker").isRunning():
            QMessageBox.warning(self, "错误", "任务正在运行，请先等待或暂停。")
            return
            
        # Reset relevant fields for retry
        task_data.update({
            "failed": False,
            "paused": False, # Will be set to '排队中' by enqueue_task
            "status": "等待", # Will be changed by enqueue_task
            "progress": "",
            "speed": ""
            # Keep filepath and other params as they were
        })
        
        self.update_task_ui(task_id) # Reflect reset state immediately
        self.enqueue_task(task_id) # This will set status to "排队中" and add to queue
        logging.info(f"{self.log_prefix}Task {task_id} ('{task_data.get('title')}') enqueued for retry.")
        self.check_and_start_tasks()


    def toggle_pause_resume_task(self, task_id):
        logging.debug(f"{self.log_prefix}toggle_pause_resume_task for {task_id}")
        task_data = self.tasks.get(task_id)
        if not task_data: return
        if task_data.get("_marked_for_deletion_while_active"):
            logging.info(f"{self.log_prefix}Task {task_id} is marked for deletion, cannot toggle state.")
            return
        
        worker = task_data.get("worker")
        current_status = task_data.get("status", "")

        # If task is active (running or in queue to run) -> Pause it
        if (worker and worker.isRunning()) or current_status in ["下载中...", "准备下载", "启动中", "排队中"]:
            logging.info(f"{self.log_prefix}Requesting pause for task {task_id} (status: {current_status})")
            if worker and worker.isRunning():
                worker.stop() # Worker's finished signal will update status to "暂停"
            elif task_id in self.task_queue: # If it's in queue but not yet started by a worker
                try:
                    self.task_queue.remove(task_id)
                    task_data.update({"in_queue":False, "paused":True, "status":"暂停"}) # Manually set to Paused
                    self.update_task_ui(task_id)
                    self.save_tasks_to_file()
                    logging.info(f"{self.log_prefix}Task {task_id} removed from queue and paused.")
                except ValueError:
                    logging.warning(f"{self.log_prefix}Task {task_id} was in queue but remove failed.")
            else: # Should be handled by worker.stop() if it's running without being in queue (unlikely)
                 # This case might occur if status is "下载中..." but worker is None or not running
                 task_data.update({"paused":True, "status":"暂停"})
                 self.update_task_ui(task_id); self.save_tasks_to_file()

        # If task is paused, waiting, or failed -> Resume/Start it
        elif task_data.get("paused") or current_status in ["暂停", "已暂停(中断)", "等待"] or task_data.get("failed"):
            logging.info(f"{self.log_prefix}Requesting resume/start for task {task_id} (status: {current_status})")
            self.enqueue_task(task_id) # enqueue_task handles resetting failed/paused states
            self.check_and_start_tasks()
        else:
            # For statuses like "完成", "错误" (that are not failed=True and retryable), this button does nothing
            logging.info(f"{self.log_prefix}Task {task_id} status '{current_status}' is not toggleable by this button.")
    
    def update_all_task_row_indices(self):
        # Call this after rows are added/removed from QTableWidget to sync internal 'row' field
        changed_indices = 0
        for r in range(self.table.rowCount()):
            task_id_item = self.table.item(r, 0)
            if task_id_item:
                task_id = task_id_item.text()
                if task_id in self.tasks:
                    if self.tasks[task_id].get("row") != r:
                        self.tasks[task_id]["row"] = r
                        changed_indices +=1
        if changed_indices > 0:
            logging.debug(f"{self.log_prefix}{changed_indices} task row indices updated in self.tasks.")


    def choose_cookies_file(self):
        logging.debug(f"{self.log_prefix}choose_cookies_file called.")
        # Start directory for dialog: current path in line edit if valid, else user's home
        start_dir = os.path.dirname(self.line_cookies_file.text()) if \
                    self.line_cookies_file.text() and \
                    os.path.isdir(os.path.dirname(self.line_cookies_file.text())) else \
                    os.path.expanduser("~")
        
        file_path, _ = QFileDialog.getOpenFileName(self, "选择 Cookies 文件", start_dir, "Text files (*.txt);;All files (*.*)")
        if file_path:
            self.line_cookies_file.setText(file_path)
            logging.info(f"{self.log_prefix}Cookies文件选择为: {file_path}")
    
    def _open_directory(self, path):
        # Helper function to open a directory in the system's file explorer
        if not path or not os.path.isdir(path):
            actual_path_to_check = path if path else "<未提供路径>"
            logging.warning(f"{self.log_prefix}无法打开目录：路径 '{actual_path_to_check}' 无效或不是一个目录。")
            QMessageBox.warning(self, "错误", f"无法打开目录：路径 '{actual_path_to_check}' 无效或不是一个目录。")
            return
        try:
            norm_path = os.path.normpath(path) # Normalize path for the current OS
            if platform.system() == "Windows":
                os.startfile(norm_path) # For Windows
            elif platform.system() == "Darwin": # macOS
                subprocess.Popen(["open", norm_path])
            else: # Linux and other Unix-like
                subprocess.Popen(["xdg-open", norm_path])
            logging.info(f"{self.log_prefix}尝试打开目录: {norm_path}")
        except Exception as e:
            logging.error(f"{self.log_prefix}打开目录 {path} 失败: {e}", exc_info=True)
            QMessageBox.warning(self, "打开目录错误", f"无法打开目录 '{path}':\n{e}")


    def open_containing_folder(self, task_id):
        logging.debug(f"{self.log_prefix}open_containing_folder called for task_id: {task_id}")
        task_data = self.tasks.get(task_id)
        if not task_data:
            QMessageBox.information(self, "提示", f"任务 {task_id} 数据未找到。")
            return

        filepath = task_data.get("filepath")
        # Try to open directory of the downloaded file first
        if filepath and os.path.exists(filepath):
            directory = os.path.dirname(filepath)
            self._open_directory(directory)
            return
        elif filepath and not os.path.exists(filepath) and task_data.get("status") == "完成":
             # File was marked as completed but now missing
             QMessageBox.warning(self, "文件丢失", f"任务 {task_id} 已完成，但文件 '{filepath}' 未找到。尝试打开其预期目录。")
             directory = os.path.dirname(filepath)
             if os.path.isdir(directory): self._open_directory(directory)
             else: # If even the dir of missing file is gone, try task's output_dir
                 params = task_data.get("params")
                 if isinstance(params, dict) and params.get("output_dir") and os.path.isdir(params.get("output_dir")):
                     self._open_directory(params["output_dir"])
                 else: QMessageBox.information(self, "提示", f"任务 {task_id} 的文件路径和输出目录均未知或无效。")
             return

        # If no filepath or file doesn't exist (and not a "completed but missing" case), try task's output_dir
        params = task_data.get("params")
        if isinstance(params, dict) and params.get("output_dir") and os.path.isdir(params.get("output_dir")):
            output_dir = params["output_dir"]
            self._open_directory(output_dir)
            if not filepath: # Inform user if we opened output_dir because filepath was unknown
                QMessageBox.information(self, "提示", f"已打开任务 {task_id} 的默认输出目录。")
            return
        
        # If all else fails
        QMessageBox.information(self, "提示", f"任务 {task_id} 的下载文件路径或输出目录未知/无效。")


    def on_max_concurrent_changed(self, value):
        if not self.checkbox_unlimited.isChecked(): # Only apply if "unlimited" is not checked
            self.max_concurrent = value
            logging.info(f"{self.log_prefix}同时下载数更改为: {value}")
            self.check_and_start_tasks() # Potentially start more tasks


    def on_unlimited_toggled(self, state):
        is_checked = (state == Qt.Checked)
        self.spin_concur.setEnabled(not is_checked) # Disable spinbox if unlimited
        if is_checked:
            self.max_concurrent = 999999 # A very large number for "unlimited"
            logging.info(f"{self.log_prefix}同时下载数设置为: 不限")
        else:
            self.max_concurrent = self.spin_concur.value() # Restore from spinbox
            logging.info(f"{self.log_prefix}同时下载数恢复为: {self.max_concurrent}")
        self.check_and_start_tasks() # Re-evaluate task starting


    def choose_folder(self):
        current_path = self.line_folder.text()
        # Start directory for dialog: current path if valid, else user's home
        start_dir = current_path if current_path and os.path.isdir(current_path) else os.path.expanduser("~")
        
        folder = QFileDialog.getExistingDirectory(self, "选择保存目录", start_dir)
        if folder: # If a folder was selected (not cancelled)
            self.line_folder.setText(folder)
            logging.info(f"{self.log_prefix}保存路径更改为: {folder}")


    def choose_post_script(self):
        current_script_path = self.line_post_script.text()
        # Start directory: directory of current script if valid file, else user's home
        start_dir = os.path.dirname(current_script_path) if \
                    current_script_path and os.path.isfile(current_script_path) else \
                    os.path.expanduser("~")

        file, _ = QFileDialog.getOpenFileName(self, "选择Python3后处理脚本", start_dir, "Python Scripts (*.py);;All Files (*)")
        if file:
            self.line_post_script.setText(file)
            logging.info(f"{self.log_prefix}后处理脚本选择为: {file}")

    # === MODIFIED: fetch_links_from_input and _fetch_next_url ===
    def fetch_links_from_input(self):
        logging.info(f"{self.log_prefix}fetch_links_from_input called.")
        text_content = self.text_urls.toPlainText().strip()
        if not text_content:
            QMessageBox.warning(self, "提示", "请输入至少一个链接。")
            return

        urls_to_process = [u.strip() for u in text_content.splitlines() if u.strip()]
        if not urls_to_process:
            QMessageBox.warning(self, "提示", "未找到有效链接。")
            return

        self.btn_fetch.setEnabled(False) # Disable button during fetching
        self.btn_start_all.setEnabled(False) # Also disable start all

        self._urls_to_fetch_queue = urls_to_process
        self.list_fetcher_processed_count = 0 # Reset counter
        logging.info(f"{self.log_prefix}开始解析 {len(self._urls_to_fetch_queue)} 个链接...")
        self._fetch_next_url() # Start fetching the first URL in the queue

    def _fetch_next_url(self):
        if not self._urls_to_fetch_queue: # All URLs processed
            self.btn_fetch.setEnabled(True) # Re-enable button
            self.btn_start_all.setEnabled(self.table.rowCount() > 0) # Re-enable if tasks exist
            logging.info(f"{self.log_prefix}所有链接解析尝试完成。")
            if self.list_fetcher_processed_count > 0 : # Only show message if something was processed
                QMessageBox.information(self, "解析完成", f"所有链接解析完成。当前列表共 {self.table.rowCount()} 个任务。")
            self.list_fetcher_processed_count = 0 # Reset counter
            return

        self.list_fetcher_processed_count +=1 # Increment for this URL
        self.current_fetch_url = self._urls_to_fetch_queue.pop(0) # Get next URL
        logging.info(f"{self.log_prefix}正在解析: {self.current_fetch_url} (还剩 {len(self._urls_to_fetch_queue)} 个)")

        # Get parameters for the fetcher
        current_cookies_browser_setting = self.combo_cookies.currentText()
        current_cookies_file_path = self.line_cookies_file.text().strip()
        current_fetch_extra_args = self.line_fetch_extra_args.text().strip() # Get from new UI field

        self.list_fetcher = YtDlpListFetcher(
            self.current_fetch_url,
            cookies_browser=current_cookies_browser_setting,
            cookies_file_path=current_cookies_file_path,
            extra_args_for_fetching=current_fetch_extra_args # Pass to fetcher
        )
        self.list_fetcher.fetched_signal.connect(self.on_entries_fetched_for_url)
        self.list_fetcher.error_signal.connect(self.on_fetch_error_for_url)
        self.list_fetcher.finished.connect(self._on_fetcher_finished) # To proceed to next URL
        self.list_fetcher.start()
        logging.debug(f"{self.log_prefix}YtDlpListFetcher for {self.current_fetch_url} started.")
    # === END MODIFIED ===

    def _on_fetcher_finished(self):
        # This slot is called when a YtDlpListFetcher thread finishes (successfully or with error)
        logging.info(f"{self.log_prefix}解析线程 for '{self.current_fetch_url}' 已结束 (finished signal).")
        
        # Clean up connections for the completed fetcher
        # Check if list_fetcher still exists and is the one that finished
        if hasattr(self, 'list_fetcher') and self.list_fetcher and not self.list_fetcher.isRunning():
            try:
                if self.list_fetcher.signalsBlocked(): # Should not be blocked, but good check
                    self.list_fetcher.blockSignals(False)
                # Disconnect signals to prevent issues if fetcher object is reused or lingers
                self.list_fetcher.fetched_signal.disconnect(self.on_entries_fetched_for_url)
                self.list_fetcher.error_signal.disconnect(self.on_fetch_error_for_url)
                self.list_fetcher.finished.disconnect(self._on_fetcher_finished)
            except TypeError: # Signals might have already been disconnected or were never connected
                logging.debug(f"{self.log_prefix}Signals for fetcher '{self.current_fetch_url}' likely already disconnected.")
            except Exception as e_disc:
                logging.warning(f"{self.log_prefix}Error disconnecting fetcher signals for {self.current_fetch_url}: {e_disc}")
            # self.list_fetcher = None # Optional: explicitly dereference, Python's GC should handle it
        
        self._fetch_next_url() # Process the next URL in the queue

    def on_entries_fetched_for_url(self, entries): # entries is a list of dicts
        logging.info(f"{self.log_prefix}成功从 '{self.current_fetch_url}' 解析到 {len(entries)} 条目。")
        if not entries and self.current_fetch_url: # No entries found but fetch was "successful"
             QMessageBox.warning(self, "解析结果", f"链接 '{self.current_fetch_url}' 解析成功，但未返回任何视频条目。")
        
        added_count = 0
        for entry in entries:
            url = entry.get("url")
            if not url:
                logging.warning(f"{self.log_prefix}Fetched entry missing URL for {self.current_fetch_url}, entry: {entry}")
                continue
            title = entry.get("title", url) # Use URL as fallback title
            
            if self.add_task_to_table(url, title): # add_task_to_table returns task_id or None
                added_count += 1
        
        if added_count > 0:
            logging.info(f"{self.log_prefix}Added {added_count} new tasks to the table.")
            # No need to save_tasks_to_file here, add_task_to_table does it for new tasks


    def on_fetch_error_for_url(self, msg): # msg is a string
        logging.error(f"{self.log_prefix}解析错误 ({self.current_fetch_url}): {msg}")
        QMessageBox.warning(self, f"链接解析错误", f"无法解析链接 '{self.current_fetch_url}':\n{msg}")
        # Note: _on_fetcher_finished will still be called to proceed to the next URL if any

    def closeEvent(self, event):
        logging.info(f"{self.log_prefix}应用程序关闭请求...")
        self.pause_all_active_tasks(clear_queue=True) # Stop active downloads and clear queue
        
        logging.debug(f"{self.log_prefix}Saving tasks before waiting for threads...")
        self.save_tasks_to_file() # Save current state

        active_threads_to_wait_for = []
        # Check DownloadTaskWorker threads
        for task_data_val in list(self.tasks.values()): # Iterate over a copy
            worker = task_data_val.get("worker")
            if worker and worker.isRunning():
                logging.debug(f"{self.log_prefix}CloseEvent: DownloadWorker for task {getattr(worker, 'task_id', 'Unknown')} is running.")
                active_threads_to_wait_for.append(worker)
        
        # Check YtDlpListFetcher thread (if any is active)
        if hasattr(self, 'list_fetcher') and self.list_fetcher and self.list_fetcher.isRunning():
            logging.debug(f"{self.log_prefix}CloseEvent: ListFetcher for {self.current_fetch_url} is running.")
            active_threads_to_wait_for.append(self.list_fetcher)

        if active_threads_to_wait_for:
            app_instance = QApplication.instance() # type: ignore
            if app_instance:
                # Allow Qt to process some events, which might help threads to finish cleanly
                # especially if they emit signals that need processing in the main thread.
                logging.info(f"{self.log_prefix}正在等待 {len(active_threads_to_wait_for)} 个活动线程结束 (短时事件处理)...")
                # Process events for a short duration, e.g., 100ms per thread, up to a max
                # This is a heuristic. Direct thread join (wait) is more robust.
                # For a GUI app, a tight loop without processEvents can freeze.
                # However, QThread.wait() is blocking and should be used.
                # Let's rely on QThread.wait() primarily.
                # app_instance.processEvents(QEventLoop.AllEvents, 100 * len(active_threads_to_wait_for))
            else:
                logging.info(f"{self.log_prefix}QApplication instance not found, skipping processEvents on close for {len(active_threads_to_wait_for)} threads.")


            for thread_to_wait in active_threads_to_wait_for:
                thread_name = getattr(thread_to_wait, 'task_id', type(thread_to_wait).__name__)
                logging.info(f"{self.log_prefix}等待线程: {thread_name}...")
                # QThread.wait() is blocking and will wait for the thread's run() method to exit.
                # It also processes events necessary for the thread to finish if it's event-driven.
                if not thread_to_wait.wait(5000): # Wait up to 5 seconds
                    logging.warning(f"{self.log_prefix}线程 {thread_name} 等待超时，可能未完全结束。")
                else:
                    logging.info(f"{self.log_prefix}线程 {thread_name} 已结束。")
        
        logging.debug(f"{self.log_prefix}Final save before exiting...")
        self.save_tasks_to_file() # Final save after threads are hopefully done
        
        logging.info(f"{self.log_prefix}所有可等待的活动线程处理完毕，应用程序正在退出...")
        event.accept() # Accept the close event
