import spacy

def remove_adjectives_spacy(file_path):
    """
    使用spaCy删除文件中的所有形容词
    """
    # 加载spaCy模型（需要先安装：pip install spacy && python -m spacy download en_core_web_sm）
    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError:
        print("请先安装spaCy英文模型：")
        print("pip install spacy")
        print("python -m spacy download en_core_web_sm")
        return
    
    try:
        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read().strip()
        
        # 分割词汇（假设文件以逗号分隔）
        words = [word.strip() for word in content.split(',') if word.strip()]
        
        if not words:
            print("文件为空或格式不正确")
            return
        
        # 使用spaCy进行词性标注
        non_adjective_words = []
        removed_adjectives = []
        
        removed_count = 0
        
        # 分批处理以避免内存问题
        batch_size = 1000
        for i in range(0, len(words), batch_size):
            batch = words[i:i + batch_size]
            # 将词汇组合成文本进行处理
            text = " ".join(batch)
            doc = nlp(text)
            
            # 构建词汇到词性的映射
            word_pos = {}
            for token in doc:
                word_pos[token.text.lower()] = token.pos_
            
            # 过滤形容词
            for word in batch:
                if word_pos.get(word.lower()) == 'ADJ':
                    removed_adjectives.append(word)
                    removed_count += 1
                else:
                    non_adjective_words.append(word)
            
            print(f"处理进度: {min(i + batch_size, len(words))}/{len(words)} 词汇")
        
        # 将结果写回文件
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(','.join(non_adjective_words))
        
        print(f"处理完成！")
        print(f"原始词汇数: {len(words)}")
        print(f"过滤后词汇数: {len(non_adjective_words)}")
        print(f"删除的形容词数量: {removed_count}")
        if removed_adjectives:
            print(f"删除的部分形容词示例: {', '.join(removed_adjectives[:20])}...")
        
    except Exception as e:
        print(f"处理文件时出错: {e}")

# 如果还是想用NLTK，这里是修复版本
def remove_adjectives_nltk_fixed(file_path):
    """
    修复版的NLTK方法删除形容词
    """
    import nltk
    from nltk.tag import PerceptronTagger
    from nltk.data import find
    
    try:
        # 确保模型已下载
        try:
            find('taggers/averaged_perceptron_tagger')
        except LookupError:
            print("下载NLTK词性标注模型...")
            nltk.download('averaged_perceptron_tagger')
        
        # 读取文件
        with open(file_path, 'r', encoding='utf-8') as f:
            words = [w.strip() for w in f.read().split(',') if w.strip()]
        
        # 使用PerceptronTagger
        tagger = PerceptronTagger()
        tagged = tagger.tag(words)
        
        # 过滤形容词
        filtered = [word for word, tag in tagged if tag not in ['JJ', 'JJR', 'JJS']]
        removed = [word for word, tag in tagged if tag in ['JJ', 'JJR', 'JJS']]
        
        # 写回文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(','.join(filtered))
        
        print(f"删除了 {len(removed)} 个形容词")
        if removed:
            print(f"删除的形容词示例: {', '.join(removed[:20])}...")
            
    except Exception as e:
        print(f"NLTK处理出错: {e}")
        print("尝试使用spaCy方法...")
        remove_adjectives_spacy(file_path)



# 使用示例
if __name__ == "__main__":
    # 推荐使用spaCy方法
    # remove_adjectives_spacy('stop_word.txt')
    
    # 或者使用修复的NLTK方法
    remove_adjectives_nltk_fixed('stop_word.txt')