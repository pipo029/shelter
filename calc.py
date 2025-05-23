# アルファ化米の計算を行うクラス

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import matplotlib.pyplot as plt


#彦根市で実装できるように書き換えaiu
class Calc:
    def __init__(
        self, pf_df_path, mesh_geometry_path, evacuation_sites_path, 
        city_ward_path, output_path, buffer_distance, 
        crs, pf_year, pf_month):
        self.pf_df_path = pf_df_path
        self.mesh_geometry_path = mesh_geometry_path
        self.evacuation_sites_path = evacuation_sites_path
        self.city_ward_path = city_ward_path
        self.buffer_distance = buffer_distance
        self.output_path = output_path
        self.crs = crs
        self.pf_year = pf_year
        self.pf_month = pf_month
    
    def load_data(self):
        # 人流データの読み込み
        self.pf_df_path = self.pf_df_path.format(pf_year=self.pf_year, pf_month=self.pf_month)
        self.pf_df = pd.read_csv(self.pf_df_path, encoding="shift-jis")
        self.pf_df = self.pf_df[(self.pf_df["dayflag"]==1) & (self.pf_df["timezone"]==0)]
        self.pf_df["mesh1kmid"] = self.pf_df["mesh1kmid"].astype(str)
        self.pf_df = self.pf_df[self.pf_df["mesh1kmid"].str.startswith("5339")]
        self.pf_df.reset_index(drop=True, inplace=True)
        
        # 1kmメッシュデータの読み込み
        self.mesh = gpd.read_file(self.mesh_geometry_path)
        self.mesh.to_crs(self.crs, inplace=True)
        
        # 人流データに1kmメッシュのgeometryを結合
        self.pf_mesh_gdf = self.pf_df.merge(self.mesh, left_on="mesh1kmid", right_on="KEY_CODE", how="left")
        self.pf_mesh_gdf = gpd.GeoDataFrame(self.pf_mesh_gdf, geometry="geometry")
        
        # 避難所データの読み込み
        self.evacuation_sites = gpd.read_parquet(self.evacuation_sites_path)
        self.evacuation_sites.to_crs(self.crs, inplace=True)
        self.evacuation_sites["buffer_{self.buffer_distance}"] = self.evacuation_sites.geometry.buffer(self.buffer_distance)
        self.evacuation_sites["geometry"] = self.evacuation_sites["buffer_{self.buffer_distance}"]
        self.evacuation_sites.drop(columns=["buffer_{self.buffer_distance}"], inplace=True)
        
        # 市区町村ポリゴンの読み込み
        self.city_ward = gpd.read_parquet(self.city_ward_path)
        self.city_ward.to_crs(self.crs, inplace=True)
        self.city_ward = self.city_ward[self.city_ward["city"] == "品川区"]
        self.city_ward.reset_index(drop=True, inplace=True)
    
    
    def buffer_and_intersect(self):
        # 人流データ（1kmメッシュポリゴン）と避難所バッファ済みデータとの空間結合
        self.buffered_mesh = gpd.sjoin(
            self.pf_mesh_gdf,
            self.evacuation_sites,
            how="inner",
            predicate="intersects",
        )
        
        # バッファのオーバーラップエリア計算
        self.buffered_mesh["overlapArea"] = self.buffered_mesh.apply(
            lambda row: row.geometry.intersection(
                self.evacuation_sites.loc[row["index_right"], "geometry"]).area, axis=1
        )
        
        # 施設ごとのバッファにかぶるメッシュの面積の合計を計算
        self.buffered_mesh["total_overlapArea_per_evacuation"] = (
            self.buffered_mesh.groupby("施設ID")["overlapArea"].transform("sum")
        )
        
        # 面積按分率の計算
        self.buffered_mesh["initial_ratio"] = (
            self.buffered_mesh["overlapArea"] / self.buffered_mesh["total_overlapArea_per_evacuation"]
        )
        
        # 各施設ごとの按分率合計を計算
        self.buffered_mesh['initial_ratio_sum'] = (
            self.buffered_mesh.groupby('施設ID')['initial_ratio'].transform('sum')
        )
        
        # 各備蓄倉庫、避難所から各メッシュへの配分量を計算。100%放出する。
        self.buffered_mesh['assigned_alpha_rice'] = (
            self.buffered_mesh['アルファ化米'] * self.buffered_mesh['initial_ratio']
        )
        
        # 各メッシュごとに配分される量を計算
        self.mesh_supply = (
            self.buffered_mesh.groupby(
                'mesh1kmid')['assigned_alpha_rice'].sum().reset_index()
        )
        
    def city_ward_overray(self):
        # 市区町村ポリゴンとself.resultの空間結合をして、品川区に含まれるメッシュを抽出
        self.shinagawa_surround = gpd.sjoin(
            self.pf_mesh_gdf,
            self.city_ward,
            how="inner",
            predicate="intersects",
        )
        self.shinagawa_surround.reset_index(drop=True, inplace=True)
    
    def calc_supply_and_demand(self):
        # 人流データの人口から1人当たり3食分の米を計算
        self.shinagawa_surround["required_alpha_rice"] = self.shinagawa_surround["population"] * 3
        self.result = self.shinagawa_surround[["mesh1kmid", "population", "required_alpha_rice", "geometry"]].merge(
            self.mesh_supply, on="mesh1kmid", how="left"
        )
        # self.result = self.result[self.result["assigned_alpha_rice"].notna()]
        self.result.reset_index(drop=True, inplace=True)
        
        self.result.fillna(0, inplace=True)
        
        # 不足量の計算
        self.result["shotage_alpha_rice"] = (
            self.result["required_alpha_rice"] - self.result["assigned_alpha_rice"]
        )
        
        # 不足率の計算
        self.result["fulfillment_ratio"] = (
           self.result["assigned_alpha_rice"] / self.result["required_alpha_rice"]
        )
        
        self.result["deviation_percentage"] = self.result["assigned_alpha_rice"] / self.result["required_alpha_rice"] - 1
        
    def output_file(self):
        # 結果を保存
        result_gdf = gpd.GeoDataFrame(self.result, geometry="geometry")
        result_gdf.to_parquet(self.output_path, index=False)
    
    def run(self):
        self.load_data()
        self.buffer_and_intersect()
        self.city_ward_overray()
        self.calc_supply_and_demand()
        # 結果を保存
        self.output_file()



if __name__ == "__main__":
    pf_df_path = "I:/マイドライブ/品川区/人流/monthly_mdp_mesh1km_13/13/{pf_year}/{pf_month}/monthly_mdp_mesh1km.csv"
    mesh_geometry_path = "I:/マイドライブ/品川区/人流/SDDSWS5339/MESH05339.shp"
    evacuation_sites_path = "D:/work/品川区データPJファイル共有/避難所データ/processed/備蓄データ.parquet"
    city_ward_path = "D:/work/品川区データPJファイル共有/避難所データ/raw/city_polygon.parquet"
    output_path = "D:/work/品川区データPJファイル共有/避難所データ/processed/alpha_rice_shotage.parquet"
    buffer_distance = 500
    crs = 6677
    pf_year =  '2019'
    pf_month = '07'
    # classのインスタンス化
    calc = Calc(
        pf_df_path=pf_df_path,
        mesh_geometry_path=mesh_geometry_path,
        evacuation_sites_path=evacuation_sites_path,
        city_ward_path=city_ward_path,
        output_path=output_path,
        buffer_distance=buffer_distance,
        crs=crs,
        pf_year=pf_year,
        pf_month=pf_month
    )
    # メソッドの実行
    calc.run()
