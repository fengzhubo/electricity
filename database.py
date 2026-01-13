# CREATE TABLE electricity_prices (
#     id BIGINT AUTO_INCREMENT PRIMARY KEY,
#     date_time DATE NOT NULL COMMENT '日期',
#     time96 VARCHAR(10) NOT NULL COMMENT '96时刻',
#     day_ahead_price DECIMAL(10, 4) NULL COMMENT '日前电价 (元/MWh)',
#     real_time_price DECIMAL(10, 4) NULL COMMENT '实时电价 (元/MWh)',
#     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     UNIQUE KEY uk_datetime (date_time, time96)
# ) COMMENT='电价表';
# CREATE TABLE bidding_space_forecast (
#     id BIGINT AUTO_INCREMENT PRIMARY KEY,
#     date_time Dbidding_space_forecastATE NOT NULL COMMENT '日期',
#     time96 VARCHAR(10) NOT NULL COMMENT '96时刻',
#     load_forecast DECIMAL(10,2) COMMENT '全网负荷预测',
#     wind_power_forecast DECIMAL(10,2) COMMENT '风电预测',
#     pv_forecast DECIMAL(10,2) COMMENT '光伏预测',
#     nuclear_power_forecast DECIMAL(10,2) COMMENT '核电',
#     local_power_forecast DECIMAL(10,2)bidding_space_forecast COMMENT '地方燃煤',
#     hydro_power_forecast DECIMAL(10,2) COMMENT '水电',
#     non_market_forecast DECIMAL(10,2) COMMENT '除水电外非市场发电总加预测',
#     tie_line_forecast DECIMAL(10,2) COMMENT '联络线',
#     capacity_online_forecast DECIMAL(10,2) COMMENT '开机容量',
#     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     UNIQUE KEY uk_datetime (date_time, time96)
# ) COMMENT='日前竞价空间表';
