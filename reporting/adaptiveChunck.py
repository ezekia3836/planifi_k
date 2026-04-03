import psutil
import time
import numpy as np
from collections import deque

class AdaptiveChunkManager:
    def __init__(self):
        self.MIN_CHUNK  = 10
        self.MAX_CHUNK  = 500
        self.current_chunk = 50    
        self.batch_times  = deque(maxlen=10)  
        self.batch_sizes  = deque(maxlen=10) 
        self.memory_history= deque(maxlen=10) 
        self._last_batch_start = None

    def _get_metrics(self):
        vm      = psutil.virtual_memory()
        cpu     = psutil.cpu_percent(interval=0.2)
        process = psutil.Process()

        mem_available = vm.available / vm.total   
        mem_process   = process.memory_info().rss / vm.total  
        mem_pressure  = mem_available - mem_process 

        return {
            "mem_pressure" : mem_pressure,
            "mem_available": mem_available,
            "cpu_percent"  : cpu,
            "mem_process_mb": process.memory_info().rss / 1024**2
        }

    def record_batch_end(self, chunk_size):
    
        if self._last_batch_start is None:
            return
        elapsed = time.time() - self._last_batch_start
        self.batch_times.append(elapsed)
        self.batch_sizes.append(chunk_size)
        self.memory_history.append(self._get_metrics()["mem_pressure"])

    def record_batch_start(self):
        self._last_batch_start = time.time()

    def adaptive_chunk_size(self):
        metrics = self._get_metrics()
        mem  = metrics["mem_pressure"]
        cpu  = metrics["cpu_percent"]
        if mem < 0.10:
            base_chunk = self.MIN_CHUNK      
        elif mem < 0.20:
            base_chunk = 15
        elif mem < 0.35:
            base_chunk = 30
        elif mem < 0.50:
            base_chunk = 75
        elif mem < 0.70:
            base_chunk = 150
        else:
            base_chunk = self.MAX_CHUNK

        if cpu > 80:
            cpu_factor = 0.60
        elif cpu > 60:
            cpu_factor = 0.80
        else:
            cpu_factor = 1.0

        adjusted = int(base_chunk * cpu_factor)

        if len(self.batch_times) >= 3:
            avg_time = np.mean(self.batch_times)
            last_time = self.batch_times[-1]
            last_chunk = self.batch_sizes[-1]
            if last_time > avg_time * 1.20:
                slowdown_factor = avg_time / last_time 
                adjusted = int(adjusted * slowdown_factor)

            elif last_time < avg_time * 0.80 and mem > 0.40:
                adjusted = int(adjusted * 1.10)
        max_change = int(self.current_chunk * 0.30)
        adjusted   = max(
            self.current_chunk - max_change,
            min(self.current_chunk + max_change, adjusted)
        )

        self.current_chunk = max(self.MIN_CHUNK, min(self.MAX_CHUNK, adjusted))
        return self.current_chunk