import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import glob
import os

def remove_stopwords_from_csv(csv_file_path, stopwords_file_path, output_dir=None):
    """
    根据停用词文件删除CSV文件中的对应行
    
    Args:
        csv_file_path (str): CSV文件路径
        stopwords_file_path (str): 停用词文件路径（逗号分隔）
        output_dir (str): 输出目录，如果为None则覆盖原文件
    """
    try:
        # 读取停用词
        with open(stopwords_file_path, 'r', encoding='utf-8') as f:
            stopwords_content = f.read().strip()
            stopwords_list = [word.strip().lower() for word in stopwords_content.split(',') if word.strip()]
        
        print(f"加载了 {len(stopwords_list)} 个停用词")
        
        # 读取CSV文件
        df = pd.read_csv(csv_file_path)
        original_count = len(df)
        print(f"原始数据行数: {original_count}")
        
        # 过滤掉停用词对应的行
        filtered_df = df[~df['word'].str.lower().isin(stopwords_list)]
        filtered_count = len(filtered_df)
        removed_count = original_count - filtered_count
        
        print(f"过滤后行数: {filtered_count}")
        print(f"删除行数: {removed_count}")
        
        # 确定输出路径
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            filename = os.path.basename(csv_file_path)
            output_path = os.path.join(output_dir, filename)
        else:
            output_path = csv_file_path
        
        # 保存结果
        filtered_df.to_csv(output_path, index=False)
        print(f"结果已保存到: {output_path}")
        
        # 显示被删除的部分词汇
        removed_words = df[df['word'].str.lower().isin(stopwords_list)]['word'].tolist()
        if removed_words:
            print(f"删除的部分词汇示例: {', '.join(removed_words[:20])}")
            if len(removed_words) > 20:
                print(f"... 等 {len(removed_words)} 个词汇")
        
        return removed_count
        
    except Exception as e:
        print(f"处理文件时出错: {e}")
        return 0

def batch_remove_stopwords(csv_files, stopwords_file_path, output_dir=None):
    """
    批量处理多个CSV文件
    
    Args:
        csv_files (list): CSV文件路径列表
        stopwords_file_path (str): 停用词文件路径
        output_dir (str): 输出目录
    """
    total_removed = 0
    
    for csv_file in csv_files:
        print(f"\n正在处理: {csv_file}")
        removed = remove_stopwords_from_csv(csv_file, stopwords_file_path, output_dir)
        total_removed += removed
    
    print(f"\n批量处理完成！总共删除了 {total_removed} 行数据")
def generate_wordclouds(csv_files, output_dir="wordclouds"):
    """
    为多个CSV文件生成词云图
    
    Args:
        csv_files: CSV文件路径列表或通配符
        output_dir (str): 输出目录
    """
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 处理文件列表
    if isinstance(csv_files, str):
        files = glob.glob(csv_files)
    else:
        files = csv_files
    
    print(f"找到 {len(files)} 个CSV文件")
    
    for csv_file in files:
        try:
            print(f"处理文件: {csv_file}")
            
            # 读取CSV文件
            df = pd.read_csv(csv_file)
            
            if df.empty:
                print(f"  ⚠️ 文件为空，跳过")
                continue
            
            # 创建词频字典 {word: frequency}
            word_freq = dict(zip(df['word'], df['word_occurrences']))
            
            # 生成词云
            wordcloud = WordCloud(
                width=1200,
                height=800,
                background_color='white',
                colormap='viridis',  # 颜色方案
                max_words=100,       # 最多显示100个词
                relative_scaling=0.5,
                random_state=42
            ).generate_from_frequencies(word_freq)
            
            # 创建图表
            plt.figure(figsize=(12, 8))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            
            # 设置标题
            filename = os.path.basename(csv_file).replace('.csv', '')
            plt.title(f'{filename} - 词云图', fontsize=16, fontweight='bold', pad=20)
            
            # 保存图表
            output_file = os.path.join(output_dir, f"{filename}_wordcloud.png")
            plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()
            
            print(f"  ✅ 词云图已保存: {output_file}")
            
        except Exception as e:
            print(f"  ❌ 处理文件时出错: {e}")

# 使用示例
if __name__ == "__main__":
    stopwords_file = "stop_word.txt"
    csv_files = []
    for file in os.listdir("word_freq_data"):
        if file.endswith(".csv"):
            csv_files.append(os.path.join("word_freq_data", file))
    batch_remove_stopwords(csv_files, stopwords_file, "cleaned_word_freq_data")
    
    generate_wordclouds("cleaned_word_freq_data/*.csv")