from service.cache_batch import CacheBatchService
from datetime import datetime
if __name__ == "__main__":
    start = datetime.now()
    CacheBatchService().run_full_batch()
    print(f"[{datetime.now()}] Exécution:  {datetime.now() - start}")