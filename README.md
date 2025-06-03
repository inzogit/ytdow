python3 --version
python3 -m venv venv
source venv/bin/activate
pip install PyQt5
pip install yt-dlp
#pip install curl_cffi
chmod +x external_tools/yt-dlp
chmod +x external_tools/ffmpeg
chmod +x external_tools/ffprobe
python3 main_app.py
