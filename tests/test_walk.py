import time
import os
import re
from pathlib import Path
from datetime import datetime

def test():
    root = Path(r'C:\Users\felip\Documents\WeChat Files\wxid_xd3703k0ih2p22\FileStorage')
    
    # Calculate current and previous month in YYYY-MM format
    now = datetime.now()
    curr_month = now.strftime("%Y-%m")
    if now.month == 1:
        prev_month = f"{now.year - 1}-12"
    else:
        prev_month = f"{now.year}-{now.month - 1:02d}"
        
    allowed_months = {curr_month, prev_month}
    print(f"Allowed months: {allowed_months}")
    
    start = time.perf_counter()
    count = 0
    dirs_visited = 0
    
    # Custom walker with pruning
    for dirpath, dirnames, filenames in os.walk(root):
        dirs_visited += 1
        path_parts = Path(dirpath).parts
        
        # Determine depth or folder type to prune
        # Parts relative to FileStorage root:
        # If dirpath is root, we only visit MsgAttach and Temp
        rel_parts = Path(dirpath).relative_to(root).parts
        
        if len(rel_parts) == 0:
            # We are at FileStorage root. Only keep MsgAttach and Temp
            dirnames[:] = [d for d in dirnames if d.lower() in ('msgattach', 'temp')]
        elif len(rel_parts) == 1 and rel_parts[0].lower() == 'msgattach':
            # We are inside MsgAttach. The subdirectories are chat_ids. Keep all of them.
            pass
        elif len(rel_parts) == 2 and rel_parts[0].lower() == 'msgattach':
            # We are inside MsgAttach/<chat_id>. Keep only Image and Thumb
            dirnames[:] = [d for d in dirnames if d.lower() in ('image', 'thumb')]
        elif len(rel_parts) == 3 and rel_parts[0].lower() == 'msgattach' and rel_parts[2].lower() in ('image', 'thumb'):
            # We are inside MsgAttach/<chat_id>/Image or Thumb. The subdirectories are months (YYYY-MM).
            # Keep only current and previous month
            dirnames[:] = [d for d in dirnames if d in allowed_months]
            
        # Count candidate files in the current folder
        for f in filenames:
            count += 1
            
    duration = time.perf_counter() - start
    print(f"Pruned walk: visited {dirs_visited} directories, found {count} files")
    print(f"Time taken: {duration:.4f} seconds")

if __name__ == '__main__':
    test()
