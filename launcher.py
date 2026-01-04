import os
import sys
import logging
from aiohttp import web

# ✅ Determine base path (Handle PyInstaller frozen state)
if getattr(sys, 'frozen', False):
    # Running as compiled EXE
    base_path = sys._MEIPASS
else:
    # Running as script
    base_path = os.path.dirname(os.path.abspath(__file__))

# ✅ Add base_path to sys.path to ensure imports work
if base_path not in sys.path:
    sys.path.insert(0, base_path)

# ✅ Change working directory to base_path to ensure relative file access works
os.chdir(base_path)

try:
    # Import from the original app.py
    # We assume app.py is in the same directory (or bundled root)
    from app import create_app
    from config import PORT
except ImportError as e:
    print(f"❌ Error importing application: {e}")
    print("Make sure you are running this from the EasyProxy root directory or the built EXE.")
    sys.exit(1)

def main():
    # ✅ Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )
    
    print("=" * 50)
    print(f"🚀 EasyProxy Launcher")
    print(f"📂 Base Path: {base_path}")
    print(f"📡 Port: {PORT}")
    print("=" * 50)

    # ✅ Create the aiohttp application
    app = create_app()

    # ✅ Run the application
    # Using aiohttp's run_app directly
    web.run_app(app, host='0.0.0.0', port=PORT)

if __name__ == '__main__':
    main()
