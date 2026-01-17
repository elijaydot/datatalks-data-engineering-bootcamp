import os
from pathlib import Path

def read_and_display_files(folder_path="test"):
    """Read all files in a folder and display their contents with labels."""
    
    folder = Path(__file__).parent.parent / folder_path
    
    if not folder.exists():
        print(f"Error: Folder '{folder_path}' does not exist.")
        return
    
    if not folder.is_dir():
        print(f"Error: '{folder_path}' is not a directory.")
        return
    
    current_file = Path(__file__).name
    
    files = [f for f in folder.iterdir() if f.is_file() and f.name != current_file]
    print(f"Reading from the FOLDER: {folder.name} | PATH: {folder}")
    
    if not files:
        print(f"No files found in '{folder_path}'.")
        return
    
    for file_path in sorted(files):
        print(f"\n{'='*60}")
        print(f"FILE: {file_path.name}")
        print(f"{'='*60}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                print(content)
        except Exception as e:
            print(f"Error reading file: {e}")

if __name__ == "__main__":
    read_and_display_files()