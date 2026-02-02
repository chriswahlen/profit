import subprocess
import sys
from pathlib import Path


def test_cli_stub_runs(tmp_path):
    script = Path("scripts/ask_agent.py")
    if not script.exists():
        script = Path.cwd() / "scripts" / "ask_agent.py"
    cmd = [sys.executable, str(script), "Price for AAPL", "--start", "2024-01-01", "--end", "2024-01-02"]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    assert proc.returncode == 0
    assert "source=prices" in proc.stdout
    assert "[unresolved inputs]" not in proc.stderr
