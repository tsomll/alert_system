from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandahouse

# подключение к бд
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student',
    'password': 'dpo_python_2020',
    'database': 'simulator_20250220'
}

def check_anomaly(df, metric, a=4, n=5):
    # Сбросим индекс для работы с группами
    df_reset = df.copy().reset_index(drop=True)

    # расчет квартилей и IQR для 15-минутных интервалов
    df_reset['q25'] = df_reset.groupby('date')[metric].shift(1).rolling(n).quantile(0.25)
    df_reset['q75'] = df_reset.groupby('date')[metric].shift(1).rolling(n).quantile(0.75)
    df_reset['iqr'] = df_reset['q75'] - df_reset['q25']
    df_reset['up'] = df_reset['q75'] + a * df_reset['iqr']
    df_reset['low'] = df_reset['q25'] - a * df_reset['iqr']

    # сглаживаем границы up и low для 15-минутных интервалов
    df_reset['up'] = df_reset.groupby('date')['up'].rolling(n, center=True, min_periods=1).mean().reset_index(level=0, drop=True)
    df_reset['low'] = df_reset.groupby('date')['low'].rolling(n, center=True, min_periods=1).mean().reset_index(level=0, drop=True)

        # расчет квартилей и IQR для предыдущего дня
    df_reset['q25_prev'] = df_reset.groupby('date')[metric].rolling(n).quantile(0.25).reset_index(level=0, drop=True)
    df_reset['q75_prev'] = df_reset.groupby('date')[metric].rolling(n).quantile(0.75).reset_index(level=0, drop=True)
    df_reset['iqr_prev'] = df_reset['q75_prev'] - df_reset['q25_prev']
    df_reset['up_prev'] = df_reset['q75_prev'] + a * df_reset['iqr_prev']
    df_reset['low_prev'] = df_reset['q25_prev'] - a * df_reset['iqr_prev']

    # сглаживаем границы up и low для 15-минутных интервалов
    df_reset['up_prev'] = df_reset.groupby('date')['up_prev'].rolling(n, center=True, min_periods=1).mean().reset_index(level=0, drop=True)
    df_reset['low_prev'] = df_reset.groupby('date')['low_prev'].rolling(n, center=True, min_periods=1).mean().reset_index(level=0, drop=True)

    # расчёт отклонения от предыдущего дня для сравнения
    df_reset['prev_day'] = df_reset[metric].shift(96)

    prev_day_diff = (df_reset[metric].iloc[-1] - df_reset['prev_day'].iloc[-1]) / df_reset['prev_day'].iloc[-1]

    # проверка на аномалию для текущего дня и предыдущего
    if df_reset[metric].iloc[-1] < df_reset['low'].iloc[-1] or df_reset[metric].iloc[-1] > df_reset['up'].iloc[-1]:
        is_alert = 1
    elif df_reset[metric].iloc[-1] < df_reset['low_prev'].iloc[-1] or df_reset[metric].iloc[-1] > df_reset['up_prev'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df_reset, prev_day_diff

def run_alerts(df_main, metrics_list, chat_id, bot, table_name):
    for metric in metrics_list:
        df = df_main[['ts', 'date', 'hm', metric]].copy()
        is_alert, df, prev_day_diff = check_anomaly(df,metric)

        if is_alert:
            # msg = '''Метрика {metric} в срезе {table_name}:\n текущее значение: {current_val:.2f}\n отклонение от предыдущего значения: {last_val_diff:.2f}\n отклонение от предыдущего дня: {prev_val_diff:.2f}'''.format(metric = metric,
            #                                                     table_name=table_name,
            #                                                      current_val=df[metric].iloc[-1],
            #                                                      last_val_diff= abs(1 - (df[metric].iloc[-1]/df[metric].iloc[-2])),
            #                                                      prev_val_diff=abs(prev_day_diff))
            current_val = df[metric].iloc[-1]
            prev_val = df[metric].iloc[-2]
            last_val_diff = abs(1 - (current_val / prev_val))
            prev_day_val = df['prev_day'].iloc[-1]
            prev_val_diff = abs(prev_day_diff)

            low = df['low'].iloc[-1]
            up = df['up'].iloc[-1]
            low_prev = df['low_prev'].iloc[-1]
            up_prev = df['up_prev'].iloc[-1]

            if current_val < low:
                bound_type = f"Ниже текущей нижней границы: {low:.2f}"
            elif current_val > up:
                bound_type = f"Выше текущей верхней границы: {up:.2f}"
            elif current_val < low_prev:
                bound_type = f"Ниже нижней границы предыдущего дня: {low_prev:.2f}"
            elif current_val > up_prev:
                bound_type = f"Выше верхней границы предыдущего дня: {up_prev:.2f}"
            else:
                bound_type = "Неожиданное поведение"

            msg = f'''Обнаружена аномалия по метрике *{metric}* в срезе *{table_name}*:

             Текущее значение: {current_val:.2f}
             Отклонение от предыдущего значения (15 мин назад): {last_val_diff:.2%}
             Отклонение от значения в это же время вчера: {prev_val_diff:.2%}
            🚩 {bound_type}
            '''
            sns.set(rc={'figure.figsize':(16,10)})
            plt.tight_layout()

            ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
            ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
            ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')

            for ind,label in enumerate(ax.get_xticklabels()):
                if ind %2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel='time')
            ax.set(ylabel=metric)

            ax.set_title(metric)
            ax.set(ylim=(0, None))

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

def generate_alert(chat=None):
    #доступ к боту
    chat_id=chat if chat else '818972037' 
    my_token = '8152376546:AAHJDyX118SFFsp0VQqZqNLJP5HmHsnksrc' # токен бота
    bot = telegram.Bot(token=my_token)

    #запрос к таблице feed_actions
    q_feed = """
    SELECT 
        toStartOfInterval(time, INTERVAL 15 MINUTE) AS ts, 
        toDate(time) AS date,
        formatDateTime(toStartOfInterval(time, INTERVAL 15 MINUTE), '%R') AS hm,
        countIf(user_id, action = 'view') AS views,
        countIf(user_id, action = 'like') AS likes,
        count(DISTINCT user_id) AS dau,
        likes/views as ctr
    FROM simulator_20250220.feed_actions
    WHERE time >= today() - INTERVAL 1 DAY
      AND time < toStartOfInterval(now(), INTERVAL 15 MINUTE)
    GROUP BY ts, date, hm
    ORDER BY ts


    """

    df_feed = pandahouse.read_clickhouse(q_feed, connection=connection)

    #проверка наличия алертов для таблицы feed_actions
    metrics_list = ['views', 'likes', 'dau', 'ctr']
    run_alerts(df_feed, metrics_list, chat_id, bot, 'feed_actions')
    
    #запрос к таблице message_actions
    q_message = """
    SELECT 
        toStartOfInterval(time, INTERVAL 15 MINUTE) AS ts, 
        toDate(time) AS date,
        formatDateTime(toStartOfInterval(time, INTERVAL 15 MINUTE), '%R') AS hm,
        count(DISTINCT user_id) AS dau,
        COUNT(*) as count_messages
    FROM simulator_20250220.message_actions
    WHERE time >= today() - INTERVAL 1 DAY
      AND time < toStartOfInterval(now(), INTERVAL 15 MINUTE)
    GROUP BY ts, date, hm
    ORDER BY ts


    """

    df_message = pandahouse.read_clickhouse(q_message, connection=connection)
    
    #проверка наличия алертов для таблицы message_actions
    metrics_list = ['dau', 'count_messages']
    run_alerts(df_message, metrics_list, chat_id, bot, 'message_actions')

# параметры dag
default_args = {
    'owner': 'adel.valishina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 5),
}

# интервал запуска
schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_alert():
    
    @task()
    def check_alert():
        generate_alert()
    check_alert()
    
dag_alert = dag_alert()
