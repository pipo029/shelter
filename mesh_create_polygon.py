# -*- coding: utf-8 -*-
#1km mesh のgeometryを作成

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon


class MeshPolygonCreator:
    def __init__(self, mesh_df_path, output_path, crs):
        self.mesh_df_path = mesh_df_path
        self.output_path = output_path
        self.crs = crs

    def load_data(self):
        # attributeの読み込み
        self.mesh_df = pd.read_csv(self.mesh_df_path, encoding="shift-jis")
    
    def create_polygons(self):
        # ベクトル化してポリゴンを生成
        polygons = [[(xmin, ymin),
                        (xmin, ymax),
                        (xmax, ymax),
                        (xmax, ymin),
                        (xmin, ymin)]
            for xmin, xmax, ymin, ymax in zip(
                self.mesh_df['lon_min'], self.mesh_df['lon_max'],
                self.mesh_df['lat_min'], self.mesh_df['lat_max'])
        ]

        # geometry列を追加
        self.mesh_df['geometry'] = gpd.array.from_shapely([Polygon(c) for c in polygons])
        
        self.mesh_gdf = gpd.GeoDataFrame(self.mesh_df, geometry='geometry', crs=self.crs)
            
    def output_file(self):
        # 結果を保存
        self.mesh_gdf.to_parquet(self.output_path, index=False)

    def run(self):
        self.load_data()
        self.create_polygons()
        self.output_file()


if __name__ == "__main__":
    mesh_df_path = "G:/マイドライブ/akiyamalab/避難所/data/1kmメッシュ/attribute/attribute_mesh1km_2020.csv/attribute_mesh1km_2020.csv"
    output_path = "G:/マイドライブ/akiyamalab/避難所/data/1kmメッシュ/attribute_mesh1km_2020.parquet"
    crs = 4326

    # classのインスタンス化
    meshPolygonCreator = MeshPolygonCreator(
        mesh_df_path=mesh_df_path,
        output_path=output_path,
        crs=crs
    )
    # メソッドの実行
    meshPolygonCreator.run()