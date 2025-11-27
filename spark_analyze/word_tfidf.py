import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# 1. 读取CSV文件（确保文件路径正确）
df = pd.read_csv("low_tomatometer_low_audience_review_u_tfidf.csv")  # 替换为你的CSV文件路径

# 2. 数据预处理：将word和avg_tfidf转为词云所需的字典格式
word_weight = dict(zip(df["word"], df["avg_tfidf"]))

# 3. 配置词云参数（可按需调整）
wordcloud = WordCloud(
    width=800,  # 图片宽度
    height=600,  # 图片高度
    background_color="white",  # 背景色（可改为你想要的#2d416e）
    max_words=100,  # 最多显示词汇数
    font_path="msyh.ttc",  # 中文需指定字体（英文可删除此行）
    contour_width=3,  # 轮廓宽度（可选）
    contour_color="#2d416e"  # 轮廓颜色（可选）
)

# 4. 生成词云（根据TF-IDF权重生成）
wordcloud.generate_from_frequencies(word_weight)

# 5. 显示并保存词云图
plt.figure(figsize=(10, 7))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")  # 隐藏坐标轴
plt.title("TF-IDF Word Cloud", fontsize=16, pad=20)
plt.tight_layout()

# 保存图片（可选，保存为高清PNG）
plt.savefig("tfidf_wordcloud.png", dpi=300, bbox_inches="tight")
plt.show()