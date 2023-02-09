import rqdatac as rq
import pandas as pd
import factor_volmom_functions as ff
import math

rq.init(uri='tcp://rice:rice@180.166.204.246:16010')

# =================================== 参数设置 ========================================
start_section = 24
end_date = '2022-03-16'

# =================================== 获取数据 ========================================
# 股票池及行业分类
stock_origin_data = pd.read_excel('.\\data_input\\高低估值行业划分.xlsx', sheet_name='个股', index_col=0)
# 各指数每一期包含的行业
index_holding_indus = {}
for index_style in ['Growth', 'Value', 'Balanced', 'Cycle']:
    index_holding_indus[index_style] = pd.read_excel('.\\data_input\\style_index.xlsx',
                                                     sheet_name='{}_holding'.format(index_style), index_col=0)
# 行业指数
indus_index = pd.read_excel('.\\data_input\\indus_index.xlsx', index_col=0)
indus_index_profit = indus_index.pct_change().dropna()
# 月度调仓，每20个交易日
start_date = list(index_holding_indus['Growth'].columns)[start_section]
trading_dates = rq.get_trading_dates(start_date, end_date)
adjust_dates = trading_dates[::20]
# 计算陆股通因子数据
index_northflow = ff.north_flow(stock_origin_data, index_holding_indus, start_section, end_date, adjust_dates)

# =================================== 因子选行业 ========================================
# 每一种风格
indus_choose = {}
for each_style in ['Growth', 'Value', 'Balanced', 'Cycle']:
    indus_choose_eachday = {}
    for each_date in adjust_dates:
        # 获取当日动量因子数据
        mom_factors = index_northflow[each_style][each_date]
        # 每个动量因子
        indus_choose_factor = pd.DataFrame([])
        for each_factor in mom_factors.columns:
            caled_factor_value = mom_factors[each_factor]
            # 按照1/3的分割线将当期行业分为三分
            line = math.ceil(len(mom_factors.index) / 3)
            factor_sorted = caled_factor_value.sort_values(ascending=False)
            top_indus = list(factor_sorted.iloc[:line].index)
            bottom_indus = list(factor_sorted.iloc[-line:].index)
            indus_choose_factor = pd.concat([indus_choose_factor, pd.Series([top_indus, bottom_indus],
                                                                            index=['TOP', 'BOTTOM'], name=each_factor)],
                                            axis=1)
        indus_choose_eachday[each_date] = indus_choose_factor
    indus_choose[each_style] = indus_choose_eachday

# =================================== 净值计算 ========================================
writer1 = pd.ExcelWriter('.\\results_陆股通\\net_value.xlsx')
# 每一种风格
for each_style in ['Growth', 'Value', 'Balanced', 'Cycle']:
    writer2 = pd.ExcelWriter('.\\results_陆股通\\indus_choose_{}.xlsx'.format(each_style))
    # 每一个调仓日
    style_profit = pd.DataFrame([])
    for each_date in adjust_dates:
        # 获取阶段结束日
        each_section_num = adjust_dates.index(each_date)
        if each_date == adjust_dates[-1]:
            each_section_end = end_date
        else:
            each_section_end = rq.get_previous_trading_date(adjust_dates[each_section_num + 1])
        # 每一个因子
        factor_profit = pd.DataFrame([])
        for each_factor in ['NF20', 'NF60', 'NF180', 'NF240']:
            for each_line in ['TOP', 'BOTTOM']:
                return_all = pd.DataFrame([])
                # 取出当期行业和该段收益率序列
                target_indus = indus_choose[each_style][each_date][each_factor][each_line]
                target_indus_profit = indus_index_profit.loc[each_date:each_section_end, target_indus]
                # 期间交易日
                interval_days = rq.get_trading_dates(each_date, each_section_end)
                # 每个交易日
                last_weight = pd.Series([1/len(target_indus)]*len(target_indus), index=target_indus)
                for each_day in interval_days:
                    # 计算每天各标的的收益率
                    return_today = pd.DataFrame([])
                    for items in target_indus:
                        profit_today = last_weight.loc[items] * target_indus_profit.loc[each_day, items]
                        return_today = pd.concat(
                            [return_today, pd.DataFrame([profit_today], index=[each_day], columns=[items])],
                            axis=1)
                    return_all = pd.concat([return_all, return_today], axis=0)
                    # 计算每个标的的新仓位和总仓位
                    weight_sum = 0
                    new_weight = pd.Series([])
                    for items in target_indus:
                        single_weight = last_weight.loc[items] * (1 + target_indus_profit.loc[each_day, items])
                        weight_sum += single_weight
                        new_weight[items] = single_weight
                    # 归一化
                    last_weight = new_weight / weight_sum
                return_sum = return_all.sum(axis=1)
                return_sum.name = each_factor + '_' + each_line
                factor_profit = pd.concat([factor_profit, return_sum], axis=1)
        style_profit = pd.concat([style_profit, factor_profit], axis=0)
        indus_choose[each_style][each_date].to_excel(writer2, sheet_name=str(each_date))
    writer2.save()
    writer2.close()
    style_profit.to_excel(writer1, sheet_name=each_style)
writer1.save()
writer1.close()
