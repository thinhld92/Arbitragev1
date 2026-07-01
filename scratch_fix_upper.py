import re
import os

src_dir = "d:/Workspace/HFT_Trading/hedge_mt5/botmt5b1/src"

files_to_fix = [
    "launcher.py",
    "worker.py",
    "master_single.py",
    "master_copy_diff.py",
    "master_copy_base.py",
    "master_copy_multi.py",
    "mastery.py"
]

def process_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # launcher.py
    content = content.replace("cap.get('base_symbol', '')).strip().upper()", "cap.get('base_symbol', '')).strip()")
    content = content.replace("cap.get('diff_symbol', '')).strip().upper()", "cap.get('diff_symbol', '')).strip()")
    content = content.replace("execution.get('symbol', '')).strip().upper()", "execution.get('symbol', '')).strip()")
    content = content.replace("ex.get('symbol', '')).strip().upper()", "ex.get('symbol', '')).strip()")
    
    # worker.py
    content = content.replace("args.symbol.upper()", "args.symbol")
    content = content.replace("cap['base_symbol'].upper()", "cap['base_symbol']")
    content = content.replace("cap['diff_symbol'].upper()", "cap['diff_symbol']")
    
    # master_*.py
    content = content.replace("safe_upper(execution.get(\"symbol\"))", "str(execution.get(\"symbol\")).strip()")
    content = content.replace("safe_upper(exec_cfg.get(\"symbol\"))", "str(exec_cfg.get(\"symbol\")).strip()")
    content = content.replace("safe_upper(cap.get(\"base_symbol\"))", "str(cap.get(\"base_symbol\")).strip()")
    content = content.replace("safe_upper(cap.get(\"diff_symbol\"))", "str(cap.get(\"diff_symbol\")).strip()")
    
    # TICK keys and others
    content = content.replace("{safe_upper(cap_hien_tai['base_symbol'])}", "{cap_hien_tai['base_symbol']}")
    content = content.replace("{safe_upper(cap_hien_tai['diff_symbol'])}", "{cap_hien_tai['diff_symbol']}")
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

for fn in files_to_fix:
    path = os.path.join(src_dir, fn)
    if os.path.exists(path):
        process_file(path)
        print(f"Fixed {fn}")

print("Done")
