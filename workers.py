# workers.py
import os
import sys
import json
import subprocess
import logging
import re
import signal
from PyQt5.QtCore import QThread, pyqtSignal, QMutex, QMutexLocker # Removed QTimer as it wasn't used in stop()

try:
    from constants import YT_DLP_EXECUTABLE_PATH
except ImportError:
    # This fallback is useful if testing workers.py standalone or if constants isn't in PYTHONPATH
    print("Warning: Could not import YT_DLP_EXECUTABLE_PATH from constants. Falling back to 'yt-dlp'.")
    YT_DLP_EXECUTABLE_PATH = 'yt-dlp'


class YtDlpListFetcher(QThread):
    fetched_signal = pyqtSignal(list) # list of dicts: [{"url": ..., "title": ...}, ...]
    error_signal = pyqtSignal(str)    # error message string

    # === MODIFIED: __init__ to accept extra_args_for_fetching ===
    def __init__(self, url, cookies_browser=None, cookies_file_path=None, extra_args_for_fetching=None):
        super().__init__()
        self.url = url.strip()
        self.cookies_browser = cookies_browser
        self.cookies_file_path = cookies_file_path
        self.extra_args_for_fetching = extra_args_for_fetching # Store the new parameter
        self.setObjectName(f"Fetcher_{self.url[:30]}") # Set object name for easier debugging
        logging.debug(
            f"YtDlpListFetcher created for URL: {self.url}, "
            f"BrowserCookies: {self.cookies_browser}, CookiesFile: {self.cookies_file_path}, "
            f"ExtraFetchArgs: {self.extra_args_for_fetching}" # Log the new parameter
        )
    # === END MODIFIED ===

    def run(self):
        logging.debug(f"YtDlpListFetcher run started for {self.url}")
        try:
            # Base command arguments for yt-dlp
            base_cmd_args = [YT_DLP_EXECUTABLE_PATH]
            
            # Add cookie arguments if provided
            if self.cookies_file_path and os.path.exists(self.cookies_file_path):
                base_cmd_args.extend(['--cookies', self.cookies_file_path])
                logging.info(f"Fetcher for {self.url}: Using cookies file: {self.cookies_file_path}")
            elif self.cookies_browser and self.cookies_browser.lower() != '无': # Assuming '无' means no browser cookies
                base_cmd_args.extend(['--cookies-from-browser', self.cookies_browser.lower()])
                logging.info(f"Fetcher for {self.url}: Using cookies from browser: {self.cookies_browser}")

            # === NEW: Add extra arguments for fetching ===
            if self.extra_args_for_fetching and self.extra_args_for_fetching.strip():
                import shlex # Import shlex for safe argument splitting
                try:
                    parsed_extra_args = shlex.split(self.extra_args_for_fetching)
                    base_cmd_args.extend(parsed_extra_args)
                    logging.info(f"Fetcher for {self.url}: Using extra fetching args: {parsed_extra_args}")
                except Exception as e:
                    # Log error if parsing extra args fails, but continue to let yt-dlp attempt it
                    # Or, emit an error and return if strict parsing is required.
                    logging.error(f"Fetcher for {self.url}: Error parsing extra fetching args '{self.extra_args_for_fetching}': {e}")
                    # Example: self.error_signal.emit(f"解析自定义获取参数错误: {e}"); return
            # === END NEW ===

            # Command for fetching playlist (flat, JSON output)
            cmd_playlist_specific_args = ['--flat-playlist', '-J', self.url, '--no-colors'] # Specific to playlist fetching
            cmd_playlist = base_cmd_args + cmd_playlist_specific_args
            
            logging.debug(f"Fetcher for {self.url}: Running cmd_playlist: {' '.join(cmd_playlist)}")
            
            # Execute the command
            # Use a timeout to prevent indefinite blocking
            proc = subprocess.run(cmd_playlist, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=60)

            # Check if playlist fetch was successful and produced output
            if proc.returncode == 0 and proc.stdout and proc.stdout.strip():
                data = json.loads(proc.stdout)
                entries = data.get("entries") # "entries" key is typical for --flat-playlist
                if entries is not None: # Check if 'entries' key exists (it could be an empty list)
                    tasks = []
                    for entry in entries:
                        # Prioritize webpage_url, then url, then original URL as fallback
                        final_url = entry.get("webpage_url") or entry.get("url")
                        # Fix for YouTube-like URLs if only ID is present (heuristic)
                        if not final_url and entry.get("id") and \
                           (entry.get("ie_key", "").lower() == "youtube" or "youtube.com" in self.url.lower()):
                            final_url = f"https://www.youtube.com/watch?v={entry.get('id')}"
                        if not final_url: final_url = self.url # Fallback to original input URL if all else fails

                        title = entry.get("title") or final_url # Use URL as title if title is missing
                        tasks.append({"url": final_url, "title": title})
                    self.fetched_signal.emit(tasks)
                    logging.info(f"Fetcher for {self.url}: successfully fetched {len(tasks)} playlist entries.")
                    return # Successfully fetched playlist, no need to try single video

            # If playlist fetch failed or produced no entries, try fetching as a single video
            # Rebuild command for single video info (JSON output)
            # base_cmd_args already contains yt-dlp path, cookies, and extra_fetch_args
            cmd_video_specific_args = ['-J', self.url, '--no-colors'] # Specific to single video info
            cmd_video = base_cmd_args + cmd_video_specific_args
            
            logging.debug(f"Fetcher for {self.url}: Running cmd_video (fallback): {' '.join(cmd_video)}")
            proc2 = subprocess.run(cmd_video, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=60)
            
            if proc2.returncode == 0 and proc2.stdout and proc2.stdout.strip():
                video_data = json.loads(proc2.stdout)
                title = video_data.get('title', self.url) # Fallback to URL if title missing

                # Handle cases where -J on a playlist URL might still return a single JSON with "entries"
                if video_data.get("_type") == "playlist" and video_data.get("entries"):
                    tasks = []
                    for entry_in_single_fetch in video_data.get("entries"):
                        entry_url = entry_in_single_fetch.get("webpage_url") or entry_in_single_fetch.get("url")
                        if not entry_url and entry_in_single_fetch.get("id") and \
                           (entry_in_single_fetch.get("ie_key", "").lower() == "youtube" or "youtube.com" in self.url.lower()):
                            entry_url = f"https://www.youtube.com/watch?v={entry_in_single_fetch.get('id')}"
                        entry_title = entry_in_single_fetch.get("title") or entry_url or "Untitled"
                        tasks.append({"url": entry_url if entry_url else self.url, "title": entry_title})
                    self.fetched_signal.emit(tasks)
                    logging.info(f"Fetcher for {self.url}: successfully fetched {len(tasks)} entries from single URL (was playlist type).")
                else: # Assume it's a single video entry
                    single_video_url = video_data.get("webpage_url") or self.url
                    self.fetched_signal.emit([{"url": single_video_url, "title": title}])
                    logging.info(f"Fetcher for {self.url}: successfully fetched single video info for URL: {single_video_url}.")
                return # Successfully fetched single video info
            else:
                # Both attempts failed, construct a comprehensive error message
                error_parts = []
                proc_defined = 'proc' in locals() and proc is not None
                proc2_defined = 'proc2' in locals() and proc2 is not None

                if proc_defined:
                    err_output = (proc.stderr.strip() if proc.stderr else proc.stdout.strip() if proc.stdout else 'N/A')
                    error_parts.append(f"播放列表尝试: RC={proc.returncode}, Err={err_output[:500]}") # Limit error length
                else: # proc might not be defined if an exception occurred before subprocess.run
                    error_parts.append(f"播放列表尝试: 执行失败或超时 (proc 未定义或为 None)")

                if proc2_defined:
                    err_output2 = (proc2.stderr.strip() if proc2.stderr else proc2.stdout.strip() if proc2.stdout else 'N/A')
                    error_parts.append(f"单视频尝试: RC={proc2.returncode}, Err={err_output2[:500]}")
                else:
                    error_parts.append(f"单视频尝试: 未执行或执行失败 (proc2 未定义或为 None)")
                
                error_message = f"解析链接失败。\n" + "\n".join(error_parts)
                logging.error(f"YtDlpListFetcher failed for {self.url}: {error_message}")
                self.error_signal.emit(error_message)

        except json.JSONDecodeError as e:
            # Determine which process's output might be relevant for JSON error
            stdout_info = "N/A"; stderr_info = "N/A"
            if 'proc2' in locals() and proc2 and (proc2.stdout or proc2.stderr): # Check proc2 first as it's the fallback
                stdout_info = proc2.stdout[:200] if proc2.stdout else "N/A"
                stderr_info = proc2.stderr.strip()[:200] if proc2.stderr else "N/A"
            elif 'proc' in locals() and proc and (proc.stdout or proc.stderr): # Then check proc
                stdout_info = proc.stdout[:200] if proc.stdout else "N/A"
                stderr_info = proc.stderr.strip()[:200] if proc.stderr else "N/A"
            
            err_str = f"解析JSON输出失败: {e}\n相关进程STDOUT: {stdout_info}\n相关进程STDERR: {stderr_info}"
            logging.error(f"YtDlpListFetcher JSONDecodeError for {self.url}: {err_str}", exc_info=True)
            self.error_signal.emit(err_str)
        except subprocess.TimeoutExpired:
            logging.error(f"YtDlpListFetcher TimeoutExpired for {self.url}", exc_info=True)
            self.error_signal.emit(f"yt-dlp 执行超时 (60s)")
        except FileNotFoundError: # yt-dlp executable not found
            logging.error(f"YtDlpListFetcher FileNotFoundError: '{YT_DLP_EXECUTABLE_PATH}' not found.", exc_info=True)
            self.error_signal.emit(f"执行yt-dlp失败: 未找到yt-dlp程序 ('{YT_DLP_EXECUTABLE_PATH}').")
        except Exception as e: # Catch any other unexpected exceptions
            logging.error(f"YtDlpListFetcher Unknown Exception for {self.url}: {type(e).__name__} - {e}", exc_info=True)
            self.error_signal.emit(f"获取视频信息时发生未知异常: {type(e).__name__} - {e}")
        logging.debug(f"YtDlpListFetcher run finished for {self.url}")


class DownloadTaskWorker(QThread):
    progress_signal = pyqtSignal(str, str) # task_id, progress_string
    status_signal = pyqtSignal(str, str)   # task_id, status_message
    speed_signal = pyqtSignal(str, str)    # task_id, speed_string
    finished_signal = pyqtSignal(str, str) # task_id, result_or_filepath ("失败", "暂停", or filepath)
    error_signal = pyqtSignal(str, str)    # task_id, error_message

    _mutex = QMutex() # Mutex for self._stop_requested and self.process

    def __init__(self, task_id, url, title, output_dir, cookies_browser, conv_mode, conv_fmt,
                 limit_rate, post_script, extra_args, cookies_file_path=None,
                 video_format=None, audio_quality=None):
        super().__init__()
        self.task_id = task_id
        self.url = url
        self.title = title
        self.output_dir = output_dir
        self.cookies_browser = cookies_browser
        self.cookies_file_path = cookies_file_path
        self.conv_mode = conv_mode
        self.conv_fmt = conv_fmt
        self.limit_rate = limit_rate
        self.post_script = post_script
        self.extra_args = extra_args # These are for download, not fetching
        self.video_format = video_format # -f format string
        self.audio_quality = audio_quality # For -x --audio-quality
        
        self._stop_requested = False
        self.process = None # Holds the subprocess.Popen object
        self.pgid = None    # Process group ID for Unix-like systems
        self.setObjectName(f"Worker_{self.task_id}") # For easier debugging
        logging.debug(f"DownloadTaskWorker {self.task_id} created for URL: {self.url}, Title: {self.title}, "
                      f"VideoFormat: {self.video_format}, AudioQuality: {self.audio_quality}")

    def stop(self):
        logging.debug(f"DownloadTaskWorker {self.task_id}: stop() called.")
        with QMutexLocker(self._mutex):
            self._stop_requested = True
            if self.process and self.process.poll() is None: # If process exists and is running
                logging.info(f"Worker {self.task_id}: Attempting to stop process/group {self.process.pid}.")
                try:
                    if sys.platform != "win32" and self.pgid is not None:
                        # Send SIGTERM to the entire process group on Unix-like systems
                        logging.debug(f"Worker {self.task_id}: Sending SIGTERM to process group {self.pgid}.")
                        os.killpg(self.pgid, signal.SIGTERM)
                        # Give it a moment to terminate gracefully
                        try: self.process.wait(timeout=1) # Brief wait
                        except subprocess.TimeoutExpired:
                            if self.process.poll() is None: # Still running?
                                logging.warning(f"Worker {self.task_id}: Process group {self.pgid} did not terminate with SIGTERM, sending SIGKILL.")
                                os.killpg(self.pgid, signal.SIGKILL) # Force kill
                        logging.info(f"Worker {self.task_id}: SIGTERM/SIGKILL sent to process group {self.pgid}.")
                    elif sys.platform == "win32":
                        # On Windows, try to terminate the process tree using taskkill,
                        # or fall back to Popen.terminate() / Popen.kill()
                        # Popen.terminate() sends CTRL_C_EVENT to console apps, or TerminateProcess
                        # For more robust tree killing on Windows, taskkill is often better.
                        # However, simple terminate/kill is often sufficient for yt-dlp.
                        logging.debug(f"Worker {self.task_id}: Sending terminate/kill to process {self.process.pid} on Windows.")
                        self.process.terminate() # Try graceful termination
                        try: self.process.wait(timeout=1)
                        except subprocess.TimeoutExpired:
                             if self.process.poll() is None: # Still running?
                                logging.warning(f"Worker {self.task_id}: Process {self.process.pid} did not terminate gracefully on Windows, killing.")
                                self.process.kill() # Force kill
                        logging.info(f"Worker {self.task_id}: Terminate/kill signal sent to Windows process {self.process.pid}.")
                    else: # Fallback for other OS or if pgid wasn't obtained
                        logging.debug(f"Worker {self.task_id}: Using terminate/kill for process {self.process.pid}.")
                        self.process.terminate()
                        try: self.process.wait(timeout=1)
                        except subprocess.TimeoutExpired:
                            if self.process.poll() is None:
                                logging.warning(f"Worker {self.task_id}: Process did not terminate gracefully, killing.")
                                self.process.kill()
                        logging.info(f"Worker {self.task_id}: Terminate/kill signal sent to process.")
                except ProcessLookupError: # Process already ended
                    logging.debug(f"Worker {self.task_id}: Process/group already ended before explicit stop signal.")
                except Exception as e:
                    logging.error(f"Worker {self.task_id}: Error stopping process/group: {e}", exc_info=True)
            else:
                logging.debug(f"Worker {self.task_id}: Process not running or not initialized at stop() call.")


    def is_stopped(self):
        with QMutexLocker(self._mutex):
            return self._stop_requested

    def run(self):
        logging.debug(f"DownloadTaskWorker {self.task_id} run started for {self.url}")
        if not self.output_dir or not os.path.isdir(self.output_dir):
            err_msg = f"输出目录无效或未提供: '{self.output_dir}'"
            logging.error(f"Task {self.task_id}: {err_msg}")
            self.error_signal.emit(self.task_id, err_msg)
            self.finished_signal.emit(self.task_id, "失败")
            return

        self.status_signal.emit(self.task_id, "准备下载")
        
        # Define output template for yt-dlp
        # %(title)s and %(ext)s are yt-dlp's template placeholders
        out_template = os.path.join(self.output_dir, '%(title)s.%(ext)s')
        
        cmd = [
            YT_DLP_EXECUTABLE_PATH, self.url,
            '-o', out_template,       # Output template
            '--newline',              # Progress updates on new lines
            '--ignore-errors',        # Continue on most download errors (e.g., for playlists)
            '--no-colors',            # Disable colors in output for easier parsing
            # '--write-info-json',    # Optional: to get a .json file with metadata
            # '--write-thumbnail',    # Optional: to download thumbnail
        ]

        # Add cookie arguments
        if self.cookies_file_path and os.path.exists(self.cookies_file_path):
            cmd.extend(['--cookies', self.cookies_file_path])
        elif self.cookies_browser and self.cookies_browser.lower() != '无':
            cmd.extend(['--cookies-from-browser', self.cookies_browser.lower()])

        # Add video/audio format selection
        if self.video_format and self.video_format.strip():
            cmd.extend(['-f', self.video_format.strip()])

        # Add conversion options
        if self.conv_mode == '音频提取转换':
            cmd.append('-x') # Extract audio
            if self.conv_fmt: cmd.extend(['--audio-format', self.conv_fmt])
            if self.audio_quality and self.audio_quality.strip():
                cmd.extend(['--audio-quality', self.audio_quality.strip()])
        elif self.conv_mode == '视频格式转换':
            if self.conv_fmt: cmd.extend(['--recode-video', self.conv_fmt])
        
        # Add rate limit
        if self.limit_rate and self.limit_rate.strip():
            # Basic validation for rate limit format (e.g., 500K, 1.5M)
            # yt-dlp handles more complex validation internally
            allowed_units = ('K', 'M', 'G') # yt-dlp supports k, m, g, kiB, miB, giB
            limit_upper = self.limit_rate.upper().strip()
            valid_limit_format = False
            if limit_upper:
                if limit_upper[-1] in allowed_units and limit_upper[:-1].replace('.', '', 1).isdigit():
                    valid_limit_format = True
                elif limit_upper.isdigit(): # Plain bytes
                    valid_limit_format = True
            
            if valid_limit_format:
                cmd.extend(['-r', self.limit_rate.strip()])
            else:
                logging.warning(f"Task {self.task_id}: Invalid rate limit format: {self.limit_rate}. Ignoring limit.")

        # Add extra arguments from UI (for download)
        if self.extra_args and self.extra_args.strip():
            import shlex
            try:
                cmd.extend(shlex.split(self.extra_args))
            except Exception as e:
                err_msg = f"解析自定义下载参数错误: {e}"
                logging.error(f"Task {self.task_id}: {err_msg} from args: '{self.extra_args}'")
                self.error_signal.emit(self.task_id, err_msg)
                self.finished_signal.emit(self.task_id, "失败")
                return
        
        self.status_signal.emit(self.task_id, "下载中...")
        filepath = None # To store the successfully downloaded file's path

        try:
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8' # Ensure utf-8 for subprocess I/O

            # If bundled, ensure yt-dlp can find its bundled tools (ffmpeg)
            if getattr(sys, 'frozen', False): # Checks if running in a PyInstaller bundle
                # Add the directory of YT_DLP_EXECUTABLE_PATH to the PATH for the subprocess
                # This helps yt-dlp find ffmpeg/ffprobe if they are bundled in the same dir
                bundled_tools_dir = os.path.dirname(YT_DLP_EXECUTABLE_PATH)
                original_path = env.get('PATH', '')
                env['PATH'] = bundled_tools_dir + os.pathsep + original_path
            
            logging.info(f"Worker {self.task_id}: Executing command: \"{' '.join(cmd)}\"")

            # Popen arguments
            popen_kwargs = {
                'stdout': subprocess.PIPE,    # Capture stdout
                'stderr': subprocess.STDOUT,   # Redirect stderr to stdout
                'text': True,                  # Decode output as text
                'encoding': 'utf-8',           # Use utf-8 encoding
                'errors': 'replace',           # Replace undecodable characters
                'bufsize': 1,                  # Line-buffered
                'env': env                     # Pass modified environment
            }

            # Process creation flags for different OS
            if sys.platform == "win32":
                # CREATE_NO_WINDOW: Don't create a console window
                # CREATE_NEW_PROCESS_GROUP: Allows sending CTRL_BREAK_EVENT to the group,
                # though Popen.terminate/kill is usually preferred for direct control.
                popen_kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP
            else: # Unix-like (Linux, macOS)
                # os.setsid(): Run in a new session, becoming a process group leader.
                # This is crucial for ensuring that signals (like SIGTERM, SIGKILL)
                # affect yt-dlp and any child processes it spawns (e.g., ffmpeg).
                popen_kwargs['preexec_fn'] = os.setsid

            # Start the subprocess
            with QMutexLocker(self._mutex): # Protect access to self.process and _stop_requested
                if self._stop_requested: # Check if stop was requested just before starting
                    logging.info(f"Worker {self.task_id}: Stop requested before Popen, aborting run.")
                    self.status_signal.emit(self.task_id, "已取消") # Or "已暂停"
                    self.finished_signal.emit(self.task_id, "暂停")
                    return
                self.process = subprocess.Popen(cmd, **popen_kwargs)
                if sys.platform != "win32":
                    try:
                        self.pgid = os.getpgid(self.process.pid) # Get process group ID
                        logging.debug(f"Worker {self.task_id}: yt-dlp process {self.process.pid} started in process group {self.pgid}.")
                    except Exception as e_pgid: # Should not happen if setsid worked
                        logging.warning(f"Worker {self.task_id}: Could not get pgid for process {self.process.pid}: {e_pgid}")
                        self.pgid = self.process.pid # Fallback: use PID if PGID fails
                else:
                     logging.debug(f"Worker {self.task_id}: yt-dlp process {self.process.pid} started (Windows).")
            
            # If self.process is None here, Popen failed (should raise exception below)
            if not self.process: # Should be caught by exceptions, but defensive check
                 raise Exception("subprocess.Popen failed to create a process.")
            
            logging.info(f"Worker {self.task_id}: Subprocess {self.process.pid} (PGID: {self.pgid if self.pgid else 'N/A'}) started.")

        except FileNotFoundError: # yt-dlp executable not found
            err_msg = f"执行yt-dlp失败: 未找到程序 ('{YT_DLP_EXECUTABLE_PATH}')."
            logging.error(f"Task {self.task_id}: {err_msg}", exc_info=True)
            self.error_signal.emit(self.task_id, err_msg)
            self.finished_signal.emit(self.task_id, "失败")
            return
        except Exception as e_popen: # Other errors during Popen
            err_msg = f"启动yt-dlp执行时发生未知错误: {str(e_popen)}"
            logging.error(f"Task {self.task_id}: {err_msg}", exc_info=True)
            self.error_signal.emit(self.task_id, err_msg)
            self.finished_signal.emit(self.task_id, "失败")
            return

        # Read output line by line
        for line_output in iter(self.process.stdout.readline, ''):
            if self.is_stopped(): # Check if stop was requested during output processing
                self.status_signal.emit(self.task_id, "正在尝试停止...")
                logging.info(f"Worker {self.task_id}: Stop requested during readline loop.")
                break # Exit loop, process will be handled by stop() or wait() logic

            line = line_output.strip()
            if not line: continue # Skip empty lines
            
            # Log raw output at a very low level if needed for deep debugging
            logging.log(logging.DEBUG - 1, f"Task {self.task_id} RAW_YTDLP_OUTPUT: {line}")

            # Parse progress and speed from [download] lines
            if line.startswith('[download]'):
                self.progress_signal.emit(self.task_id, line) # Send full line for now
                # Regex for speed (e.g., "at 1.23MiB/s", "at 500.00KiB/s")
                m_speed = re.search(r'at\s+([0-9.]+\s*(?:[KMGT]iB|[KMGTB])\/s)', line, re.IGNORECASE)
                if m_speed:
                    self.speed_signal.emit(self.task_id, m_speed.group(1).strip())
                else: # If no speed, but progress is updating, clear old speed
                    m_percent = re.search(r'(\d+\.\d%)', line) # Check if it's a progress line
                    if m_percent : self.speed_signal.emit(self.task_id, "") # Clear speed display
            
            # Update status for other important messages
            elif line.startswith(('[ffmpeg]', '[ExtractAudio]', '[Merger]', '[Recode]', '[FixupM3u8]', '[FixupTimestamp]')) or \
                 line.lower().startswith('error:') or \
                 "already been downloaded" in line.lower() or \
                 "has already been recorded" in line.lower():
                self.status_signal.emit(self.task_id, line)

            # Try to capture filepath from output
            # These markers indicate a final or intermediate output file
            dest_markers = [
                'Destination: ',
                'Merging formats into "',             # yt-dlp message when merging
                '[ExtractAudio] Destination: ',       # Audio extraction destination
                '[ffmpeg] Destination: ',             # ffmpeg output (less common directly)
                'Recoding to "',                      # Video recoding destination
                'Already downloaded and merged to "'  # If file existed and was merged
            ]
            # Markers for file movement (the "to" part is the destination)
            move_markers = ['Moving item from ', 'Fixing MPEG2 transport stream to ']
            
            processed_line_for_path = line # Start with the original line
            # If it's a "move" operation, the destination is after " to "
            if any(m.lower() in line.lower() for m in move_markers) and " to " in line.lower():
                parts = line.split(" to ", 1)
                if len(parts) > 1: processed_line_for_path = parts[1]

            for marker in dest_markers:
                # Check if the (potentially processed) line starts with a known marker
                if processed_line_for_path.lstrip().startswith(marker):
                    potential_path_parts = processed_line_for_path.lstrip().split(marker, 1)
                    if len(potential_path_parts) > 1:
                        temp_path = potential_path_parts[1].strip().strip('"') # Remove quotes
                        temp_path = temp_path.split(' (frag')[0].strip() # Remove fragment info if any
                        # Basic sanity check for a filepath (contains a dot, basename > 3 chars)
                        if '.' in os.path.basename(temp_path) and len(os.path.basename(temp_path)) > 3:
                             filepath = temp_path # Captured a potential filepath
                             # Update status with a cleaner message about the path
                             clean_marker_type = marker.split(':')[0].strip('[]" ')
                             self.status_signal.emit(self.task_id, f"{clean_marker_type}: {os.path.basename(filepath)}")
                             logging.debug(f"Task {self.task_id}: Captured potential filepath: {filepath} from line: '{line}' with marker: '{marker}'")
                             break # Found a path, no need to check other markers for this line
            if filepath: pass # Ensure filepath from previous lines is not overwritten if a new non-path line matches a marker

        # Close stdout pipe after reading all output
        if self.process and self.process.stdout:
            try: self.process.stdout.close()
            except Exception: pass # Ignore errors on close

        # Wait for the process to terminate and get return code
        return_code = -1 # Default if process was never waited on
        if self.process:
            try:
                # Wait for a reasonable time. If it times out, it's an issue.
                return_code = self.process.wait(timeout=300) # 5 minutes timeout for process completion
                logging.debug(f"Worker {self.task_id}: yt-dlp process {self.process.pid} finished with code {return_code}.")
            except subprocess.TimeoutExpired:
                logging.error(f"Worker {self.task_id}: yt-dlp process wait timed out. Attempting to stop (which might kill).")
                self.status_signal.emit(self.task_id, "超时，尝试终止")
                self.stop() # Call stop() which includes kill logic
                self.error_signal.emit(self.task_id, "yt-dlp 执行超时，进程已被尝试终止。")
                self.finished_signal.emit(self.task_id, "失败") # Timeout is a failure
                return # Critical failure, exit run method
            except Exception as e_wait: # Other errors during wait (rare)
                 logging.error(f"Worker {self.task_id}: Error waiting for yt-dlp process: {e_wait}", exc_info=True)
                 self.status_signal.emit(self.task_id, f"等待进程结束时出错")
                 return_code = -99 # Indicate a special error during wait
        else: # Should not happen if Popen was successful
            logging.warning(f"Worker {self.task_id}: self.process was None at wait() call.")


        # Final status determination based on stop_requested, return_code, and filepath
        if self.is_stopped(): # If stop was requested and loop broke or process ended due to stop
            self.status_signal.emit(self.task_id, "已强制暂停")
            self.finished_signal.emit(self.task_id, "暂停")
            logging.debug(f"DownloadTaskWorker {self.task_id} run: emitting '暂停' after wait() due to stop request.")
            return

        # If not stopped, evaluate return_code and filepath
        if return_code != 0:
            err_msg = f"下载失败 (yt-dlp code: {return_code})"
            if return_code == -99 : err_msg = "下载失败 (等待yt-dlp进程时出错)"
            logging.error(f"Worker {self.task_id}: yt-dlp exited with error code {return_code}.")
            self.status_signal.emit(self.task_id, err_msg)
            self.error_signal.emit(self.task_id, f"yt-dlp 进程以错误码 {return_code} 退出。")
            self.finished_signal.emit(self.task_id, "失败")
            return

        # Return code is 0, check if filepath was captured
        if not filepath and return_code == 0:
            # This can happen if yt-dlp reports success but we couldn't parse the path
            # or if the task was, e.g., "--skip-download" and produced no file.
            logging.warning(f"Task {self.task_id}: yt-dlp exited successfully but no filepath captured.")
            self.status_signal.emit(self.task_id, "完成但未捕获路径")
            # Consider if this is an error or a special success case based on yt-dlp args
            self.error_signal.emit(self.task_id, "yt-dlp成功退出，但未能从输出中解析文件路径。")
            self.finished_signal.emit(self.task_id, "完成但路径捕获失败") # Special finished state
            return

        # Return code is 0 and filepath is captured, check if file exists
        if filepath and os.path.exists(filepath):
            logging.info(f"Task {self.task_id}: Download complete. File at: {filepath}")
            self.status_signal.emit(self.task_id, f"完成: {os.path.basename(filepath)}")
            
            # Handle post-processing script if defined and file exists
            if self.post_script and os.path.isfile(self.post_script):
                self.status_signal.emit(self.task_id, f"后处理: {os.path.basename(self.post_script)}")
                try:
                    abs_post_script = os.path.abspath(self.post_script)
                    abs_filepath = os.path.abspath(filepath)
                    python_exe = sys.executable if sys.executable else "python3" # Use current python interpreter
                    
                    # Run post-processing script, passing the downloaded filepath as an argument
                    script_run = subprocess.run(
                        [python_exe, abs_post_script, abs_filepath],
                        check=True,             # Raise CalledProcessError on non-zero exit
                        capture_output=True,    # Capture stdout/stderr
                        text=True, encoding='utf-8', errors='replace',
                        timeout=300             # 5 minutes timeout for script
                    )
                    # Log and display first 100 chars of script output as status
                    script_stdout_short = script_run.stdout.strip()[:100]
                    self.status_signal.emit(self.task_id, f"后处理完成: {script_stdout_short}")
                    logging.info(f"Task {self.task_id}: Post-processing script stdout: {script_run.stdout.strip()}")
                except subprocess.CalledProcessError as e_script:
                    err_out = (e_script.stderr or e_script.stdout or str(e_script)).strip()
                    logging.error(f"Task {self.task_id}: Post-processing script failed (Code {e_script.returncode}): {err_out}", exc_info=True)
                    self.status_signal.emit(self.task_id, f"后处理脚本失败 (码 {e_script.returncode}): {err_out[:100]}")
                except subprocess.TimeoutExpired:
                    logging.error(f"Task {self.task_id}: Post-processing script timed out.", exc_info=True)
                    self.status_signal.emit(self.task_id, "后处理脚本超时")
                except Exception as e_script_generic:
                    logging.error(f"Task {self.task_id}: Post-processing script exception: {type(e_script_generic).__name__} - {e_script_generic}", exc_info=True)
                    self.status_signal.emit(self.task_id, f"后处理脚本异常: {type(e_script_generic).__name__}")
            
            # Emit finished signal with the filepath after all processing
            self.finished_signal.emit(self.task_id, filepath)

        elif filepath and not os.path.exists(filepath):
            # yt-dlp reported success and gave a path, but file is not there
            logging.error(f"Task {self.task_id}: yt-dlp reported success but file not found at '{filepath}'")
            self.status_signal.emit(self.task_id, "完成但文件丢失")
            self.error_signal.emit(self.task_id, f"下载工具报告成功但找不到文件: {filepath}")
            self.finished_signal.emit(self.task_id, "完成但找不到文件") # Special finished state
        else: # Should not be reached if logic is correct (RC=0 but no filepath was handled above)
            logging.error(f"Task {self.task_id}: Unknown state after download. RC={return_code}, Filepath='{filepath}'")
            self.status_signal.emit(self.task_id, "状态未知 (RC=0)")
            self.error_signal.emit(self.task_id, "下载后状态未知。")
            self.finished_signal.emit(self.task_id, "失败")
        
        logging.debug(f"DownloadTaskWorker {self.task_id} run finished for {self.url}")
