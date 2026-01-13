import pymysql
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple


class ElectricityDataReader:
    def __init__(self, host: str = 'localhost', user: str = 'root', password: str = '123456',
                 database: str = 'electridata', port: int = 3306):
        """
        初始化数据库连接
            host: 数据库主机地址
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名
            port: 数据库端口，默认3306
        """
        self.db_config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }

    def get_connection(self):
        """获取数据库连接"""
        return pymysql.connect(**self.db_config)

    def read_electricity_data(self, target_date: str,flag: int = 0, read_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        根据标的日期和读取日期读取电价和竞价空间数据

        Args:
            target_date: 标的日期 (格式: 'YYYY-MM-DD')
            read_date: 读取日期 (格式: 'YYYY-MM-DD')，如果为None则使用当前日期
            flag:1 竞价空间
            flag:2 电价

        Returns:
            包含两个DataFrame的字典:
            {
                'electricity_prices': 电价数据,
                'bidding_space': 竞价空间数据
            }

        Raises:
            ValueError: 当读取日期早于标的日期时
        """
        # 验证日期
        target_dt = datetime.strptime(target_date, '%Y-%m-%d').date()

        if read_date is None:
            read_dt = date.today()
        else:
            read_dt = datetime.strptime(read_date, '%Y-%m-%d').date()

        # 检查读取日不能早于标的日
        if read_dt < target_dt:
            raise ValueError(f"读取日期({read_date})不能早于标的日期({target_date})")

        connection = self.get_connection()
        try:
            with connection.cursor() as cursor:
                if flag==1:
                    electricity_df = pd.DataFrame()
                else:
                    # 读取电价数据
                    electricity_query = """
                                        SELECT date_time, time96, day_ahead_price, real_time_price
                                        FROM electricity_prices
                                        WHERE date_time = %s
                                        ORDER BY time96 \
                                        """
                    cursor.execute(electricity_query, (target_date,))
                    electricity_data = cursor.fetchall()
                    electricity_df = pd.DataFrame(electricity_data)

                if flag==2:
                    bidding_df = pd.DataFrame()
                else:
                    # 读取竞价空间数据
                    bidding_query = """
                                    SELECT date_time, \
                                           time96, \
                                           load_forecast, \
                                           wind_power_forecast, \
                                           pv_forecast,
                                           nuclear_power_forecast, \
                                           local_power_forecast, \
                                           hydro_power_forecast,
                                           non_market_forecast, \
                                           tie_line_forecast, \
                                           capacity_online_forecast
                                    FROM bidding_space_forecast
                                    WHERE date_time = %s
                                    ORDER BY time96 \
                                    """
                    cursor.execute(bidding_query, (target_date,))
                    bidding_data = cursor.fetchall()
                    bidding_df = pd.DataFrame(bidding_data)

            return {
                'electricity_prices': electricity_df,
                'bidding_space': bidding_df
            }

        finally:
            connection.close()

    def read_date_range_data(self, start_date: str, end_date: str,
                             read_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        读取日期范围内的数据

        Args:
            start_date: 开始日期 (格式: 'YYYY-MM-DD')
            end_date: 结束日期 (格式: 'YYYY-MM-DD')
            read_date: 读取日期 (格式: 'YYYY-MM-DD')

        Returns:
            包含两个DataFrame的字典
        """
        # 验证日期范围
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

        if read_date is None:
            read_dt = date.today()
        else:
            read_dt = datetime.strptime(read_date, '%Y-%m-%d').date()

        # 检查读取日不能早于任何标的日
        if read_dt < start_dt:
            raise ValueError(f"读取日期({read_date})不能早于开始日期({start_date})")

        connection = self.get_connection()
        try:
            with connection.cursor() as cursor:
                # 读取电价数据
                electricity_query = """
                                    SELECT date_time, time96, day_ahead_price, real_time_price
                                    FROM electricity_prices
                                    WHERE date_time BETWEEN %s AND %s
                                    ORDER BY date_time, time96 \
                                    """
                cursor.execute(electricity_query, (start_date, end_date))
                electricity_data = cursor.fetchall()

                # 读取竞价空间数据
                bidding_query = """
                                SELECT date_time, \
                                       time96, \
                                       load_forecast, \
                                       wind_power_forecast, \
                                       pv_forecast,
                                       nuclear_power_forecast, \
                                       local_power_forecast, \
                                       hydro_power_forecast,
                                       non_market_forecast, \
                                       tie_line_forecast, \
                                       capacity_online_forecast
                                FROM bidding_space_forecast
                                WHERE date_time BETWEEN %s AND %s
                                ORDER BY date_time, time96 \
                                """
                cursor.execute(bidding_query, (start_date, end_date))
                bidding_data = cursor.fetchall()

            # 转换为DataFrame
            electricity_df = pd.DataFrame(electricity_data)
            bidding_df = pd.DataFrame(bidding_data)
            return {
                'electricity_prices': electricity_df,
                'bidding_space': bidding_df
            }

        finally:
            connection.close()



# 使用示例
if __name__ == "__main__":
    # 初始化读取器
    reader = ElectricityDataReader(
        host='localhost',
        user='root',
        password='123456',
        database='electridata'
    )
    try:
        # 读取单日数据
        data = reader.read_electricity_data(flag=1,
            target_date='2025-03-15',
            read_date='2025-03-16'  # 可选，如果不提供则使用当前日期
        )

        print("电价数据:")
        # print(data['electricity_prices'].head())
        # print("\n竞价空间数据:")
        # print(data['bidding_space'].head())

        # 读取日期范围数据
        range_data = reader.read_date_range_data(
            start_date='2025-10-27',
            end_date='2025-10-31'
        )


    except ValueError as e:
        print(f"错误: {e}")
    except Exception as e:
        print(f"数据库错误: {e}")