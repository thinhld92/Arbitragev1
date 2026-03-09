import os

def generate_code_context(target_dir, output_filename):
    # Các thư mục và định dạng file muốn bỏ qua
    ignore_dirs = {'.git', 'node_modules', '__pycache__', 'venv', '.idea', 'logs', 'history', 'prereqs'}
    ignore_exts = {'.png', '.jpg', '.exe', '.dll', '.pdf', '.zip', '.pyc'}
    
    with open(output_filename, 'w', encoding='utf-8') as outfile:
        # 1. Tạo sơ đồ cây thư mục
        outfile.write("=========================================\n")
        outfile.write("            CÂY THƯ MỤC DỰ ÁN\n")
        outfile.write("=========================================\n\n")
        
        for root, dirs, files in os.walk(target_dir):
            # Bỏ qua các thư mục không cần thiết
            dirs[:] = [d for d in dirs if d not in ignore_dirs]
            
            level = root.replace(target_dir, '').count(os.sep)
            indent = ' ' * 4 * level
            
            if root != target_dir:
                outfile.write(f"{indent}{os.path.basename(root)}/\n")
                
            subindent = ' ' * 4 * (level + 1) if root != target_dir else ''
            for f in files:
                if not any(f.endswith(ext) for ext in ignore_exts) and f not in (output_filename, 'gop_code.py'):
                    outfile.write(f"{subindent}{f}\n")
        
        outfile.write("\n\n=========================================\n")
        outfile.write("            NỘI DUNG MÃ NGUỒN\n")
        outfile.write("=========================================\n\n")
        
        # 2. Đọc và gộp nội dung từng file
        for root, dirs, files in os.walk(target_dir):
            dirs[:] = [d for d in dirs if d not in ignore_dirs]
            
            for file in files:
                # Bỏ qua file rác, file output và chính file script này
                if any(file.endswith(ext) for ext in ignore_exts) or file in (output_filename, 'gop_code.py'):
                    continue
                
                filepath = os.path.join(root, file)
                rel_path = os.path.relpath(filepath, target_dir)
                
                # Ghi chú vị trí file
                outfile.write(f"/* {'-'*50} */\n")
                outfile.write(f"/* FILE: {rel_path} */\n")
                outfile.write(f"/* {'-'*50} */\n\n")
                
                # Ghi nội dung file
                try:
                    with open(filepath, 'r', encoding='utf-8') as infile:
                        outfile.write(infile.read())
                except Exception as e:
                    outfile.write(f"// Không thể đọc file (Có thể là file nhị phân): {e}\n")
                
                outfile.write("\n\n")

if __name__ == "__main__":
    output_file = 'toan_bo_code.txt'
    generate_code_context('.', output_file)
    print(f"Hoàn tất! Toàn bộ code đã được gộp vào file '{output_file}'")