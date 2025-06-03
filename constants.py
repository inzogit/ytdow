# constants.py
import os
import sys
import stat
import logging # 虽然这里主要用print，但保留以备将来使用
import shutil # 用于 shutil.which

# --- 先定义 get_yt_dlp_path 函数 ---
def get_yt_dlp_path(executable_name='yt-dlp'): # 允许传入不同的可执行文件名
    """
    Determines the path to the specified executable (yt-dlp, ffmpeg, ffprobe).
    Tries to find it in bundled locations first, then falls back to system PATH.
    """
    # Windows上通常有.exe后缀
    exe_with_suffix = executable_name
    if sys.platform == "win32" and not executable_name.endswith('.exe'):
        exe_with_suffix = f"{executable_name}.exe"

    # 1. 检查打包环境 (PyInstaller)
    if getattr(sys, 'frozen', False):
        if hasattr(sys, '_MEIPASS'):
            # --onefile 模式，sys._MEIPASS 是解压后的临时目录
            base_path = sys._MEIPASS
        else:
            # --onedir 模式，可执行文件通常在 sys.executable 所在的目录
            # 对于 .app 包, sys.executable 是 .app/Contents/MacOS/YourAppName
            base_path = os.path.dirname(sys.executable)

        # 尝试的相对路径 (相对于 base_path 或 .app 包的特定位置)
        # 这些路径是常见的放置捆绑二进制文件的地方
        possible_relative_paths = [
            exe_with_suffix,                                     # 与主可执行文件同目录
            os.path.join("Resources", exe_with_suffix),          # .app/Contents/Resources/
            os.path.join("..", "Resources", exe_with_suffix),    # 如果 base_path 是 .app/Contents/MacOS/
            os.path.join("external_tools", exe_with_suffix),     # 自定义子目录 (如果在spec中如此设置)
            os.path.join("_internal", exe_with_suffix)           # PyInstaller --add-binary 通常的目标
        ]
        
        for rel_path in possible_relative_paths:
            bundled_path = os.path.abspath(os.path.join(base_path, rel_path))
            if os.path.exists(bundled_path):
                print(f"Info constants.py: Found bundled '{executable_name}' at: {bundled_path}")
                if sys.platform != "win32" and not os.access(bundled_path, os.X_OK):
                    try:
                        current_permissions = os.stat(bundled_path).st_mode
                        os.chmod(bundled_path, current_permissions | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                        print(f"Info constants.py: Set execute permission for: {bundled_path}")
                    except Exception as e:
                        print(f"Warning constants.py: Failed to set execute permission for {bundled_path}: {e}")
                return bundled_path
        
        print(f"Warning constants.py: Bundled '{executable_name}' not found in common PyInstaller locations relative to {base_path}. Will check PATH.")

    # 2. 如果不是打包环境，或者打包环境中未找到，则从系统 PATH 中查找
    # 使用 shutil.which 来可靠地查找 PATH 中的可执行文件
    path_from_env = shutil.which(exe_with_suffix)
    if path_from_env:
        print(f"Info constants.py: Found '{executable_name}' in system PATH: {path_from_env}")
        return path_from_env

    # 3. 最后回退，直接返回名称，寄希望于它在调用时能被找到（例如，如果 PATH 在运行时被修改）
    print(f"Warning constants.py: '{executable_name}' not found via shutil.which. Returning as-is: '{exe_with_suffix}'. Execution might fail if not in PATH at runtime.")
    return exe_with_suffix


# --- 常量定义 ---
YT_DLP_EXECUTABLE_PATH = get_yt_dlp_path('yt-dlp')
# 您也可以用同样的方式定义 ffmpeg 和 ffprobe 的路径，如果需要在代码中直接引用它们
# 不过通常情况下，yt-dlp 会在其自己的查找路径（包括与yt-dlp同目录，或系统PATH）中找到ffmpeg/ffprobe
# FFMPEG_PATH = get_yt_dlp_path('ffmpeg')
# FFPROBE_PATH = get_yt_dlp_path('ffprobe')


LOG_FILE_NAME = "ytdow_debug.log"
# 使用更明确的目录名，并确保在不同平台下的一致性
APP_NAME_FOR_DIRS = "ytdow" # 应用的统一名称，用于目录

# 用户文档目录下的特定文件夹 (首选)
APP_DATA_BASE_DIR_DOCUMENTS = os.path.join(os.path.expanduser("~"), "Documents", APP_NAME_FOR_DIRS + "Data")

# 用户主目录下的隐藏文件夹 (备用)
APP_DATA_FALLBACK_DIR_HOME = os.path.join(os.path.expanduser("~"), "." + APP_NAME_FOR_DIRS + "Data")

# macOS 下 Application Support 目录 (更符合 macOS 规范)
APP_DATA_MACOS_APP_SUPPORT_DIR = None
if sys.platform == "darwin": # macOS
    APP_DATA_MACOS_APP_SUPPORT_DIR = os.path.join(os.path.expanduser("~"), "Library", "Application Support", APP_NAME_FOR_DIRS)


TASKS_HISTORY_FILE_NAME = "tasks_history.json"

# --- 应用数据目录创建逻辑 ---
def get_app_data_dir():
    """Ensures the application data directory exists and returns its path."""
    # 目录尝试顺序: macOS Application Support -> Documents/AppNameData -> ~/.AppNameData
    paths_to_try = []
    if APP_DATA_MACOS_APP_SUPPORT_DIR: # 只有 macOS 才会有这个路径
        paths_to_try.append(APP_DATA_MACOS_APP_SUPPORT_DIR)
    paths_to_try.append(APP_DATA_BASE_DIR_DOCUMENTS)
    paths_to_try.append(APP_DATA_FALLBACK_DIR_HOME)

    for path_attempt in paths_to_try:
        if not os.path.exists(path_attempt):
            try:
                os.makedirs(path_attempt, exist_ok=True)
                print(f"Info constants.py: Application data directory created at: {path_attempt}")
                return path_attempt
            except Exception as e:
                print(f"Warning constants.py: Could not create app data directory {path_attempt}: {e}. Trying next option.")
        else:
            print(f"Info constants.py: Using existing application data directory: {path_attempt}")
            return path_attempt
    
    # 如果所有尝试都失败，作为最后手段使用当前工作目录 (非常不推荐)
    critical_fallback_dir = os.path.join(os.getcwd(), APP_NAME_FOR_DIRS + "Data_fallback")
    print(f"CRITICAL constants.py: All preferred app data directories failed. Attempting to use: {critical_fallback_dir}")
    try:
        os.makedirs(critical_fallback_dir, exist_ok=True)
        return critical_fallback_dir
    except Exception as e_critical:
        print(f"CRITICAL constants.py: Failed to create even the last resort directory {critical_fallback_dir}: {e_critical}. App data might not be saved.")
        # 返回一个象征性的路径，但写入可能会失败
        return critical_fallback_dir


APPLICATION_DATA_DIRECTORY = get_app_data_dir()

# 打印最终确定的路径 (在logging配置前用print)
print(f"CONSTANTS FINAL: YT_DLP_EXECUTABLE_PATH resolved to: {YT_DLP_EXECUTABLE_PATH}")
print(f"CONSTANTS FINAL: Application data directory set to: {APPLICATION_DATA_DIRECTORY}")
