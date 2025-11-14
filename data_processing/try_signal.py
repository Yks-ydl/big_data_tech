import signal
import pickle
import sys
import pandas as pd



class EmergencySaver:
    def __init__(self):
        self.data = pd.DataFrame({"A": range(10), "B": range(10)})

        self.save_path = "emergency_save.csv"

    def save_and_exit(self,signum, frame):
        """捕获信号后保存并退出"""
        self.data.to_csv(self.save_path, index=False)
        print(f"\n捕获到终止信号，已保存变量至 self.{self.save_path}")
        sys.exit(0)
    def run(self):
        signal.signal(signal.SIGINT, self.save_and_exit)

# 注册信号处理器（捕获Ctrl+C）

# 可选：捕获其他信号（如SIGTERM）
# signal.signal(signal.SIGTERM, save_and_exit)

# 主程序（模拟长时间运行）
try:
    while True:
        e = EmergencySaver()
        e.run()
except Exception as e:
    print(f"其他异常: {e}")