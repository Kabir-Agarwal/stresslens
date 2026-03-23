"""
StressLens launcher.
Run from project root: python run.py
"""

import sys
import os

# Add the stresslens directory to Python path so imports work
stresslens_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stresslens")
sys.path.insert(0, stresslens_dir)

# Change working directory to stresslens so uvicorn reload watches the right folder
os.chdir(stresslens_dir)

if __name__ == "__main__":
    import uvicorn

    print("=" * 50)
    print("  StressLens - Forensic Stress Intelligence")
    print("=" * 50)
    print(f"  Server starting at http://localhost:8000")
    print(f"  Working dir: {stresslens_dir}")
    print("=" * 50)

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=[stresslens_dir],
    )
