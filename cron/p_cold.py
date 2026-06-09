from config.ClickHouseConfig import ClickHouseConfig

class p_cold():
    def __init__(self):
        self.clk = ClickHouseConfig().getClient()
        self.table='clean_reporting'
    def get_cold(self):
        query=f"SELECT COUNT() FROM {self.table} WHERE updated_at >= today();"
        rows = self.clk.execute(query)
        return rows[0][0]