import pandas as pd
import json

df = pd.read_csv('mf_data.csv', parse_dates=['date'])

start_date = pd.Timestamp('2019-01-01')
end_date = df['date'].max()
initial_investment = 1000000
# Rebalance every 6 months
rebalance_dates = pd.date_range(start=start_date, end=end_date, freq='6MS')

portfolio = {}
rebalance_log = []
cash = initial_investment

for i, rebalance_date in enumerate(rebalance_dates):
    five_years_ago = rebalance_date - pd.DateOffset(years=1)
    rolling_returns = {}
    for fund in df['Scheme Name'].unique():
        fund_df = df[df['Scheme Name'] == fund]
        nav_now_row = fund_df[fund_df['date'] <= rebalance_date].tail(1)
        nav_past_row = fund_df[fund_df['date'] <= five_years_ago].tail(1)
        if not nav_now_row.empty and not nav_past_row.empty:
            nav_now = nav_now_row['nav'].values[0]
            nav_past = nav_past_row['nav'].values[0]
            rolling_return = (nav_now / nav_past) - 1
            rolling_returns[fund] = rolling_return

    top5 = sorted(rolling_returns, key=rolling_returns.get, reverse=True)[:5]
    top5_set = set(top5)

    navs = {}
    for fund in top5:
        nav_row = df[(df['Scheme Name'] == fund) & (df['date'] <= rebalance_date)].tail(1)
        if not nav_row.empty:
            navs[fund] = nav_row['nav'].values[0]

    portfolio_value = cash
    portfolio_value_breakdown = []
    for fund, pos in portfolio.items():
        nav_row = df[(df['Scheme Name'] == fund) & (df['date'] <= rebalance_date)].tail(1)
        if not nav_row.empty:
            nav = nav_row['nav'].values[0]
            portfolio[fund]['last_nav'] = nav
            value = pos['units'] * nav
            portfolio_value += value
            portfolio_value_breakdown.append({
                'fund': fund,
                'units': pos['units'],
                'nav': nav,
                'value': value
            })
        else:
            portfolio[fund]['last_nav'] = None
            portfolio_value_breakdown.append({
                'fund': fund,
                'units': pos['units'],
                'nav': None,
                'value': 0
            })
    if cash > 0:
        portfolio_value_breakdown.append({'fund': 'CASH', 'units': '', 'nav': '', 'value': cash})

    sold_funds = list(set(portfolio.keys()) - top5_set)
    pl_report = []
    for fund in sold_funds:
        pos = portfolio[fund]
        sell_nav = pos['last_nav']
        if sell_nav is not None:
            proceeds = pos['units'] * sell_nav
            cash += proceeds
            pl = (sell_nav - pos['buy_nav']) * pos['units']
        else:
            proceeds = 0
            pl = None
        pl_report.append({
            'fund': fund,
            'units': pos['units'],
            'buy_nav': pos['buy_nav'],
            'buy_date': str(pos['buy_date']),
            'sell_nav': sell_nav,
            'sell_date': str(rebalance_date),
            'profit_loss': pl
        })
        del portfolio[fund]

    held_funds = list(top5_set & set(portfolio.keys()))
    held_value = 0
    for fund in held_funds:
        nav = portfolio[fund]['last_nav']
        held_value += portfolio[fund]['units'] * nav

    total_to_invest = cash + held_value
    amount_per_fund = total_to_invest / 5 if len(navs) == 5 else 0

    new_portfolio = {}
    bought_funds = []
    for fund in top5:
        nav = navs[fund]
        if fund in portfolio:
            units = amount_per_fund / nav
            new_portfolio[fund] = {
                'units': units,
                'buy_nav': portfolio[fund]['buy_nav'],
                'buy_date': portfolio[fund]['buy_date'],
                'last_nav': nav
            }
        else:
            units = amount_per_fund / nav
            new_portfolio[fund] = {
                'units': units,
                'buy_nav': nav,
                'buy_date': rebalance_date,
                'last_nav': nav
            }
            bought_funds.append(fund)
    portfolio = new_portfolio
    cash = 0

    rebalance_log.append({
        'rebalance_date': str(rebalance_date.date()),
        'top5': [(fund, round(rolling_returns[fund]*100,2)) for fund in top5],
        'bought_funds': bought_funds,
        'sold_funds': sold_funds,
        'pl_report': pl_report,
        'portfolio_value': round(sum([v['units'] * v['last_nav'] for v in portfolio.values()]),2),
        'portfolio_value_breakdown': portfolio_value_breakdown
    })

final_value = sum([v['units'] * v['last_nav'] for v in portfolio.values()])
today = df['date'].max()
print(f"\nFinal portfolio value as of {today.date()}: {final_value:.2f} INR")
print(f"Total return: {((final_value/initial_investment - 1)*100):.2f}%")

# Save readable rebalance log
readable_rows = []
for log in rebalance_log:
    breakdown_str = json.dumps(log['portfolio_value_breakdown'])
    if log['pl_report']:
        for pl in log['pl_report']:
            readable_rows.append({
                'rebalance_date': log['rebalance_date'],
                'top5': ', '.join([f'{f} ({ret}%)' for f, ret in log['top5']]),
                'bought_funds': ', '.join(log['bought_funds']),
                'sold_funds': ', '.join(log['sold_funds']),
                'sold_fund': pl['fund'],
                'units': round(pl['units'], 2),
                'buy_nav': round(pl['buy_nav'], 2),
                'buy_date': pl['buy_date'],
                'sell_nav': round(pl['sell_nav'], 2) if pl['sell_nav'] is not None else '',
                'sell_date': pl['sell_date'],
                'profit_loss': round(pl['profit_loss'], 2) if pl['profit_loss'] is not None else '',
                'portfolio_value': log['portfolio_value'],
                'portfolio_value_breakdown': breakdown_str
            })
    else:
        readable_rows.append({
            'rebalance_date': log['rebalance_date'],
            'top5': ', '.join([f'{f} ({ret}%)' for f, ret in log['top5']]),
            'bought_funds': ', '.join(log['bought_funds']),
            'sold_funds': ', '.join(log['sold_funds']),
            'sold_fund': '',
            'units': '',
            'buy_nav': '',
            'buy_date': '',
            'sell_nav': '',
            'sell_date': '',
            'profit_loss': '',
            'portfolio_value': log['portfolio_value'],
            'portfolio_value_breakdown': breakdown_str
        })

pd.DataFrame(readable_rows).to_csv('rebalance_log_readable_6m.csv', index=False)
def convert_dates(obj):
    if isinstance(obj, dict):
        return {k: convert_dates(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates(i) for i in obj]
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        return obj

rebalance_log_serializable = convert_dates(rebalance_log)
with open('rebalance_log_readable_6m.json', 'w', encoding='utf-8') as f:
    json.dump(rebalance_log_serializable, f, indent=2, ensure_ascii=False)
print('Readable rebalance log saved to rebalance_log_readable_6m.csv and rebalance_log_readable_6m.json')
