import mysql.connector
from mysql.connector import Error
import pandas as pd
import os
from typing import List, Tuple, Optional
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ElectricityDataInserter:
    """电力数据导入"""

    def __init__(self, host: str = 'localhost', user: str = 'root', password: str = '123456',
                 database: str = 'electridata', port: int = 3306):
        """
        初始化数据库连接配置

        Args:
            host: 数据库主机
            database: 数据库名
            user: 用户名
            password: 密码
            port: 端口号，默认3306
        """
        self.db_config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port,
            'charset': 'utf8mb4'
        }

    def _get_connection(self):
        """获取数据库连接"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            if connection.is_connected():
                logger.info(f"成功连接到MySQL数据库: {self.db_config['database']}")
                return connection
        except Error as e:
            logger.error(f"数据库连接失败: {e}")
            raise

    def _process_price_data(self, price_df: pd.DataFrame) -> List[Tuple]:
        """
        处理电价数据

        Args:
            price_df: 电价DataFrame

        Returns:
            处理后的数据列表
        """
        try:
            price_df = price_df.transpose()
            all_data = []

            # 提取日期
            date_str = price_df.iloc[0, 0][:10]  # 取前10个字符作为日期

            # 处理数据行
            for i in range(2, len(price_df) - 1):  # 跳过前两行和最后一行
                time_slot = price_df.index[i]
                values = price_df.iloc[i].values

                day_ahead_price = values[0] if len(values) > 0 else 9999
                real_time_price = values[1] if len(values) > 1 else 9999

                data = (date_str, time_slot, day_ahead_price, real_time_price)
                all_data.append(data)

            logger.info(f"成功处理电价数据，共 {len(all_data)} 条记录")
            return all_data

        except Exception as e:
            logger.error(f"处理电价数据时发生错误: {e}")
            raise

    def _process_bidding_data(self, bidding_df: pd.DataFrame) -> List[Tuple]:
        """
        处理竞价空间数据

        Args:
            bidding_df: 竞价空间DataFrame

        Returns:
            处理后的数据列表
        """
        try:
            all_data = []
            values_list = bidding_df.values.tolist()

            for row in values_list:
                if len(row) < 12:  # 确保有足够的列
                    logger.warning(f"数据行列数不足: {len(row)}")
                    continue

                data = (
                    row[0],  # date_time
                    row[1],  # time96
                    row[2],  # load_forecast
                    row[3],  # wind_power_forecast
                    row[4],  # pv_forecast
                    row[5],  # nuclear_power_forecast
                    row[6],  # local_power_forecast
                    row[7],  # hydro_power_forecast
                    row[8],  # non_market_forecast
                    row[9],  # tie_line_forecast
                    row[11] if len(row) > 11 else None  # capacity_online_forecast
                )
                all_data.append(data)

            logger.info(f"成功处理竞价空间数据，共 {len(all_data)} 条记录")
            return all_data

        except Exception as e:
            logger.error(f"处理竞价空间数据时发生错误: {e}")
            raise

    def _calculate_operation_stats(self, total_rows: int, data_count: int) -> Tuple[int, int]:
        """
        计算操作统计信息

        Args:
            total_rows: 总影响行数
            data_count: 数据条数

        Returns:
            (inserted_count, replaced_count) 插入数量和替换数量
        """
        # REPLACE逻辑: 如果存在则删除后插入(2行影响)，不存在则直接插入(1行影响)
        # total_rows = replaced_count * 2 + inserted_count * 1
        # data_count = replaced_count + inserted_count
        replaced_count = total_rows - data_count
        inserted_count = data_count - replaced_count
        return inserted_count, replaced_count

    def insert_batch_price_data(self, price_df: pd.DataFrame) -> bool:
        """
        批量插入电价数据

        Args:
            price_df: 电价数据DataFrame

        Returns:
            操作是否成功
        """
        connection = None
        cursor = None

        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            # 处理数据
            data_list = self._process_price_data(price_df)
            if not data_list:
                logger.warning("没有可插入的电价数据")
                return False

            # 执行插入
            insert_query = """
                           REPLACE \
                           INTO electricity_prices 
            (date_time, time96, day_ahead_price, real_time_price) 
            VALUES ( \
                           %s, \
                           %s, \
                           %s, \
                           %s \
                           ) \
                           """

            cursor.executemany(insert_query, data_list)
            connection.commit()

            # 统计操作结果
            inserted_count, replaced_count = self._calculate_operation_stats(
                cursor.rowcount, len(data_list)
            )

            logger.info(
                f"电价数据批量操作完成: "
                f"总共处理 {cursor.rowcount} 行, "
                f"新增 {inserted_count} 条, "
                f"替换 {replaced_count} 条"
            )
            return True

        except Error as e:
            logger.error(f"批量插入电价数据失败: {e}")
            if connection:
                connection.rollback()
            return False

        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
                logger.debug("数据库连接已关闭")

    def insert_batch_bidding_data(self, bidding_df: pd.DataFrame) -> bool:
        """
        批量插入竞价空间数据

        Args:
            bidding_df: 竞价空间数据DataFrame

        Returns:
            操作是否成功
        """
        connection = None
        cursor = None

        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            # 处理数据
            data_list = self._process_bidding_data(bidding_df)
            if not data_list:
                logger.warning("没有可插入的竞价空间数据")
                return False

            # 执行插入
            insert_query = """
                           REPLACE \
                           INTO bidding_space_forecast 
            (date_time, time96, load_forecast, wind_power_forecast, 
             pv_forecast, nuclear_power_forecast, local_power_forecast, 
             hydro_power_forecast, non_market_forecast, tie_line_forecast, 
             capacity_online_forecast) 
            VALUES ( \
                           %s, \
                           %s, \
                           %s, \
                           %s, \
                           %s, \
                           %s, \
                           %s, \
                           %s, \
                           %s, \
                           %s, \
                           %s \
                           ) \
                           """

            cursor.executemany(insert_query, data_list)
            connection.commit()

            # 统计操作结果
            inserted_count, replaced_count = self._calculate_operation_stats(
                cursor.rowcount, len(data_list)
            )

            logger.info(
                f"竞价空间数据批量操作完成: "
                f"总共处理 {cursor.rowcount} 行, "
                f"新增 {inserted_count} 条, "
                f"替换 {replaced_count} 条"
            )
            return True

        except Error as e:
            logger.error(f"批量插入竞价空间数据失败: {e}")
            if connection:
                connection.rollback()
            return False

        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
                logger.debug("数据库连接已关闭")


if __name__ == "__main__":
    """主函数"""
    # 配置
    DATA_PATH = "C://data/new_data/"
    DB_CONFIG = {
        'host': 'localhost',
        'database': 'electridata',
        'user': 'root',
        'password': '123456'
    }

    # 初始化插入器
    inserter = ElectricityDataInserter(**DB_CONFIG)

    try:
        # 获取数据文件
        files = [f for f in os.listdir(DATA_PATH) if f.endswith(('.xlsx', '.xls'))]

        if not files:
            logger.warning(f"在 {DATA_PATH} 中没有找到Excel文件")
        else:
            logger.info(f"找到 {len(files)} 个数据文件")

            # 处理每个文件
            for filename in files:
                file_path = os.path.join(DATA_PATH, filename)
                logger.info(f"处理文件: {filename}")

                try:
                    data = pd.read_excel(file_path)

                    # 根据数据特征判断文件类型
                    if len(data) == 3:  # 电价数据通常有3行
                        success = inserter.insert_batch_price_data(data)
                    else:  # 竞价空间数据
                        success = inserter.insert_batch_bidding_data(data)

                    if success:
                        logger.info(f"文件 {filename} 处理成功")
                    else:
                        logger.error(f"文件 {filename} 处理失败")

                except Exception as e:
                    logger.error(f"处理文件 {filename} 时发生错误: {e}")
                    continue

    except Exception as e:
        logger.error(f"程序执行失败: {e}")