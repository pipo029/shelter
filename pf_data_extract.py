#人流データを展開するコード
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import zipfile
import os

class PfDataExtractor:
    def __init__(
        self, pf_zip_path, pf_file_path):
        self.pf_zip_path = pf_zip_path  # テンプレートを保持
        self.pf_file_path = pf_file_path

    def pf_extractor(self):
        #人流データの解凍

        for pf_year in range(2019, 2022):
            for pf_month in range(1, 13):
                # zipファイルの解凍
                pf_month_str = str(pf_month).zfill(2)
                zip_path = self.pf_zip_path.format(pf_year=pf_year, pf_month=pf_month_str)
                file_path = self.pf_file_path.format(pf_year=pf_year, pf_month=pf_month_str)
                print(zip_path)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(file_path)


    def run(self):
        self.pf_extractor()



if __name__ == "__main__":
    pf_zip_path = "G:/マイドライブ/akiyamalab/避難所/data/人流/monthly_mdp_mesh1km_25/25/{pf_year}/{pf_month}/monthly_mdp_mesh1km.csv.zip"
    pf_file_path = "G:/マイドライブ/akiyamalab/避難所/data/人流/monthly_mdp_mesh1km_25/25/{pf_year}/{pf_month}/monthly_mdp_mesh1km.csv"
    # classのインスタンス化
    pfDataExtractor = PfDataExtractor(
        pf_zip_path=pf_zip_path,
        pf_file_path=pf_file_path
    )
    # メソッドの実行
    pfDataExtractor.run()