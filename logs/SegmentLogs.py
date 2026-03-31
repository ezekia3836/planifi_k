import os
import shutil
MAX_LINES_PER_FILE = 100_000
CSV_DIR = "csv_segments"
os.makedirs(CSV_DIR, exist_ok=True)
def clean_csv_dir():
    if os.path.exists(CSV_DIR):
        shutil.rmtree(CSV_DIR)
        os.makedirs(CSV_DIR)
class LogManager:
    def __init__(self):
        self.file_index = 0
        self.line_count = 0
        self.file = self._open_new_file()
    
    def _open_new_file(self):
        path = os.path.join(CSV_DIR, f"processed_{self.file_index}.txt")
        return open(path, "a", encoding="utf-8")

    def write(self, segments):
        for s in segments:
            line = f"{s['id_segment']}|{s['database_id']}\n"
            self.file.write(line)
            self.line_count += 1
            if self.line_count >= MAX_LINES_PER_FILE:
                self.file.close()
                self.file_index += 1
                self.line_count = 0
                self.file = self._open_new_file()

    def close(self):
        if self.file:
            self.file.close()