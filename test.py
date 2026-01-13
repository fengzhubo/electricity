def find_min_price_periods_optimized(prices, segment_length=44, min_gap=12, num_segments=3):
    """
    优化版本：使用动态规划
    """
    n = len(prices)

    # 预处理：计算所有连续11小时的总价格
    segment_sums = []
    for i in range(n - segment_length + 1):
        total = sum(prices[i:i + segment_length])
        segment_sums.append((i, i + segment_length - 1, total))

    m = len(segment_sums)

    # 初始化DP数组
    # dp[i][k] = 考虑到第i个时间段，选择k段的最小总价格
    # prev[i][k] = 记录前一个时间段的位置
    dp = [[float('inf')] * (num_segments + 1) for _ in range(m + 1)]
    prev = [[-1] * (num_segments + 1) for _ in range(m + 1)]

    # 边界条件：选择0段的总价格为0
    for i in range(m + 1):
        dp[i][0] = 0

    # 动态规划
    for i in range(1, m + 1):

        start_i, end_i, price_i = segment_sums[i - 1]

        for k in range(1, min(num_segments, i) + 1):
            # 不选择第i个时间段
            dp[i][k] = dp[i - 1][k]
            prev[i][k] = prev[i - 1][k]

            # 选择第i个时间段
            # 找到上一个可以选的时间段
            for j in range(i - 1, 0, -1):
                start_j, end_j, _ = segment_sums[j - 1]
                if end_j + min_gap <= start_i:  # 满足间隔条件
                    if dp[j][k - 1] + price_i < dp[i][k]:
                        dp[i][k] = dp[j][k - 1] + price_i
                        prev[i][k] = j
                    break
            else:
                # 如果没有前面的时间段，且这是第一段
                if k == 1 and price_i < dp[i][k]:
                    dp[i][k] = price_i
                    prev[i][k] = i

    # 重构结果
    if dp[m][num_segments] == float('inf'):
        return None

    # 找到最小总价格
    min_total = dp[m][num_segments]

    # 回溯找到具体时间段
    segments = []
    i, k = m, num_segments
    while k > 0 and i > 0:
        if prev[i][k] != -1 and prev[i][k] != prev[i - 1][k]:
            segments.append(segment_sums[i - 1])
            i = prev[i][k]
            k -= 1
        else:
            i -= 1

    segments.reverse()
    return min_total, segments