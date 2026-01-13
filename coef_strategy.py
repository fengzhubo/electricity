from time import strptime

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score,mean_absolute_error
from sklearn.preprocessing import StandardScaler
import warnings
from datetime import datetime, timedelta

from get_data import ElectricityDataReader

warnings.filterwarnings('ignore')


class MLTrader:
    def __init__(self, model_type='random_forest', **model_params):
        """
        初始化
        参数:
        model_type: 模型类型，目前支持 'random_forest'
        model_params: 模型参数
        """
        self.model_type = model_type
        self.model = None
        self.scaler = StandardScaler()
        self.is_trained = False

        if model_type == 'random_forest':
            self.model = xgb.XGBRegressor(**model_params
            )
        else:
            raise ValueError("暂不支持该模型类型")

    def create_features(self, date, train_days):
        """
        特征处理
        """
        reader = ElectricityDataReader()
        end_date =(datetime.strptime(date, '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d')
        start_date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=train_days)).strftime('%Y-%m-%d')
        fea_date = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=16)).strftime('%Y-%m-%d')
        try:

            diff_df = pd.DataFrame()
            for day in pd.date_range(fea_date, end_date).strftime('%Y-%m-%d'):
                tmp = reader.read_electricity_data(target_date=day, flag=2)['electricity_prices']
                tmp['diff'] = tmp['day_ahead_price'] - tmp['real_time_price']
                diff_df = pd.concat([diff_df, tmp], axis=0)

            # 读取日期范围数据
            bidding = reader.read_date_range_data(
                start_date=start_date,
                end_date=date
            )
        except ValueError as e:
            print(f"错误: {e}")

        # 创建特征
        df = pd.merge(bidding['bidding_space'], bidding['electricity_prices'], on=['date_time', 'time96'])

        # 价格特征
        df['ahead_d-1'] = df['day_ahead_price'].shift(96)
        df['price_diff'] = df['day_ahead_price']-df['real_time_price']
        df['month'] = [int(i.strftime('%Y-%m')[5:]) for i in df['date_time'].values]
        df = df.dropna()
        fea_dic = {'ave_3':[], 'ave_7' : [], 'ave_15' : []}
        time_fea = []

        # 移动平均线
        for i in range(0,len(df),96):
            time_fea.extend(range(96))
            for day in [3,7,15]:
                day_range = diff_df.iloc[(15-day)*96+i:15*96+i]
                ave_list = (day_range[['time96','diff']].groupby(['time96']).mean()).values.flatten()
                days_name = 'ave_'+str(day)
                fea_dic[days_name].extend(ave_list)

        df['ave_3'] = fea_dic['ave_3']
        df['ave_7'] = fea_dic['ave_7']
        df['ave_15'] = fea_dic['ave_15']
        df['time_fea'] = time_fea
        df['real_d-2'] = (diff_df['real_time_price'].values)[-len(df):]


        return df

    def prepare_data(self, data):
        """
        准备训练数据
        """
        # 特征列
        feature_columns = [
            'time_fea', 'load_forecast', 'wind_power_forecast',
            'pv_forecast', 'nuclear_power_forecast', 'local_power_forecast',
            'hydro_power_forecast', 'non_market_forecast', 'tie_line_forecast',
            'month', 'ave_3', 'ave_7', 'ave_15', 'ahead_d-1', 'real_d-2'
        ]

        X = data[feature_columns]
        y = data['price_diff']

        return X, y

    def train(self, X,y):
        """
        训练模型
        """
        print("开始训练模型...")

        # # 标准化特征
        # X_train_scaled = self.scaler.fit_transform(X_train)
        # X_test_scaled = self.scaler.transform(X_test)

        # 训练模型
        self.model.fit(X, y)



    def predict(self, date):
        """
        预测交易信号
        """
        data_with_features = self.create_features(date, 60)

        # 准备数据
        X, y = self.prepare_data(data_with_features)
        X_train = X[:-96].astype(float)
        y_train = y[:-96].astype(float)
        X_test = X[-96:].astype(float)
        y_test = y[-96:].astype(float)
        self.train(X_train,y_train)
        # 预测
        predictions = self.model.predict(X_test)

        # 添加预测结果到数据
        result = data_with_features[-96:]
        result['pred'] = predictions
        result['real'] = y_test
        return result[['date_time', 'time96', 'pred', 'real']]

    def benifit(self,real_diff,pred):
        profit_8 = 0
        profit = 0
        for i in range(len(real_diff)):
            coef = 0.2 if pred[i] >=0 else -0.2
            profit += real_diff[i]*coef
            profit_8 += real_diff[i]*0.2
        return profit, profit_8

    def backtest(self, start_date, end_date, train_days=10):
        """
        回测策略
        """
        result = pd.DataFrame()
        for date in pd.date_range(start_date, end_date).strftime('%Y-%m-%d'):
            print(date)
            tmp = self.predict(date)
            tmp_p,tmp_p8 = self.benifit(tmp['real'].values,tmp['pred'].values)
            print(tmp_p,tmp_p8)
            result = pd.concat([result, tmp], axis=0)

        # 评估模型
        pred = result['pred'].values
        real = result['real'].values
        profit,profit_8 = self.benifit(real,pred)
        mae = mean_absolute_error(result['pred'], result['real'])
        r2 = r2_score(result['pred'], result['real'])

        print(f"模型训练完成，收益为：{profit}，全天0.8收益为{profit_8}，mae: {mae},r2: {r2}")




# 使用示例
if __name__ == "__main__":
    # 生成示例数据
    print("生成示例数据...")


    # 初始化模型
    trader = MLTrader(
        model_type='random_forest',
        n_estimators=100,
        max_depth=4
    )

    # 训练模型
    accuracy = trader.backtest('2025-06-01', '2025-11-30')



