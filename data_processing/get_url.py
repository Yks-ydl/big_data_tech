import pandas as pd

import string
class Geturl:
   
    def __init__(self, movies_file='data_processing/movies_metadata.csv'):
        self.movies = pd.read_csv(movies_file)
        print(f"Total movies: {len(self.movies)}")
        self.url = pd.DataFrame(columns=['title','imdb_id','url'])
        #self.other_movies = pd.DataFrame(columns=['title','imdb_id'])



    def get_urls(self):
        for  i in range(len(self.movies)):
            title = str(self.movies.iloc[i]['title']).strip()
            title_1 = title.replace("’", '').replace("'", '').replace("&", 'and')
            title_s = ""
            # 上一个字符是否为非法字符的状态,0:不是,1:是
            t =0
            for j in range(len(title_1)):
                if title_1[j].isalnum():
                    title_s += title_1[j].lower()
                    t = 0
                else:
                    if t == 0:
                        title_s += "_"
                        t = 1

        
            imdb_id = self.movies.iloc[i]['imdb_id']
            url = f"https://www.rottentomatoes.com/m/{title_s}/reviews"
            self.url.loc[i] = [title, imdb_id, url]

        return self.url

if __name__ == "__main__":
    urls  = Geturl().get_urls()

    for i in range(38):
        file_name = f'data_processing/url_data/imdb_reviews_urls_part_{i+1}.csv'
        urls_part = urls[i*1200:(i+1)*1200]
        urls_part.to_csv(file_name, index=False)