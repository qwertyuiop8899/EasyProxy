# 🚀 EasyProxy

**Universal HLS/M3U8 Proxy & Stream Extractor**
A powerful, lightweight proxy server designed to handle HLS, M3U8, and DASH (MPD) streams. It includes specialized extractors for popular streaming services, DRM support, and an integrated DVR system.

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## ✨ Features

- **🌐 Universal Proxy**: Seamlessly handles HLS, M3U8, MPD (DASH), and static video files.
- **🔓 DRM Support**: ClearKey decryption via FFmpeg transcoding or legacy mode.
- **🔐 Specialized Extractors**: Native support for Vavoo, DaddyliveHD, Sportsonline, VixSrc, DoodStream, MaxStream, and more.
- **📼 Integrated DVR**: Record live streams while watching or schedule background recordings.
- **🛠️ Playlist Builder**: Web interface to combine, manage, and proxy entire M3U playlists.
- **☁️ Cloud Ready**: Optimized for HuggingFace, Render, Koyeb, and other free-tier platforms.
- **🛡️ Cloudflare Bypass**: Integrated with FlareSolverr and Byparr for bot protection bypass.

---

## 🚀 Quick Start

### 🐳 Docker (Recommended)
The **Full** version includes the proxy plus FlareSolverr and Byparr for maximum compatibility.

```bash
# Light Version (Proxy Only - Default)
docker run -d -p 7860:7860 --name EasyProxy ghcr.io/realbestia1/easyproxy:latest

# Full Version (Proxy + Solvers)
docker run -d -p 7860:7860 --name EasyProxy ghcr.io/realbestia1/easyproxy:full
```

### 🐍 Python (Local)
For a simple "Light" install, run the proxy directly. For "Full" mode (to use solvers like FlareSolverr/Byparr), you must run them separately.

**1. Install Solvers (Optional, for Full Mode)**
- **FlareSolverr**: [Download](https://github.com/FlareSolverr/FlareSolverr/releases) and run `flaresolverr.exe` (Port 8191).
- **Byparr**: [Download](https://github.com/ThePhaseless/Byparr/releases) and run `byparr.exe` (Port 8192).

**2. Start EasyProxy**
```bash
git clone https://github.com/realbestia1/EasyProxy.git
cd EasyProxy
pip install -r requirements.txt
python -m playwright install chromium
python app.py
```
*Access the dashboard at `http://localhost:7860`*

---

## 📦 Deployment Options

| Method | Description |
| :--- | :--- |
| **Light (Default)** | Standard `docker build .` uses the base `Dockerfile`. |
| **Full** | Use `Dockerfile.full` for a monolithic build with solvers included. |
| **Docker Compose** | Run the complete stack (Proxy + Solvers) with `docker-compose up -d`. |
| **HuggingFace** | Use `Dockerfile-hf` for seamless deployment on HF Spaces. |
| **Termux** | Support for Android via Python & FFmpeg. |

---

## ⚙️ Configuration

Configure the server via a `.env` file. See `.env.example` for all options.

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PORT` | Server port | `7860` |
| `API_PASSWORD` | Optional password for API endpoints | `ep` |
| `FLARESOLVERR_URL` | URL for FlareSolverr (Not needed in Full version) | `http://localhost:8191` |
| `FLARESOLVERR_TIMEOUT` | Timeout for FlareSolverr requests (seconds) | `30` |
| `BYPARR_URL` | URL for Byparr (Not needed in Full version) | `http://localhost:8192` |
| `DVR_ENABLED` | Enable recording features | `false` |

---

## 📖 API Usage
For detailed API documentation and testing, use the built-in **Interactive Docs** available at:
- `http://localhost:7860/docs` (Swagger UI)
- `http://localhost:7860/redoc` (ReDoc)

### 📺 Streaming Proxy
Prefix any stream URL with the proxy endpoint to handle headers and DRM.
```
http://localhost:7860/proxy/manifest.m3u8?url=<URL>
```
**Options:**
- `&clearkey=KID:KEY`: Provide keys for DASH streams.
- `&h_<Header Name>=<Value>`: Pass custom headers (e.g., `&h_User-Agent=VLC`).

### 🔍 Stream Extractor
Extract direct video links from supported websites.
```
http://localhost:7860/extractor/video?d=<URL>&redirect_stream=true
```
*Tip: Open `http://localhost:7860/extractor` in your browser for a list of all parameters and supported hosts.*

### 📼 DVR & Recordings
Manage your recordings via the `/recordings` web UI or API.
- `/record?url=<URL>&name=<NAME>`: Start recording and watch simultaneously.
- `/api/recordings/start`: Trigger a background recording.

---

## 🛠️ Integrated Tools
- **Playlist Builder** (`/builder`): A visual tool to create custom M3U playlists with proxied links.
- **Server Info** (`/info`): Check status, public IP, and version information.

---

## 🤝 Contributing
Contributions are welcome!
1. **Fork** the repository.
2. **Commit** your changes (features, extractors, or bug fixes).
3. **Open a Pull Request** to the main branch.

*Found a bug? Open an [Issue](https://github.com/realbestia1/EasyProxy/issues)!*

---

## 📄 License
Distributed under the MIT License. See `LICENSE` for more information.

<div align="center">
  <p><b>⭐ If this project helped you, please give it a star! ⭐</b></p>
</div>
