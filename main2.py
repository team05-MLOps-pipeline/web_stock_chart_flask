# 필요한 라이브러리를 임포트합니다.
import yfinance as yf
import pandas as pd
import plotly.express as px
import plotly.io as pio
from flask import Flask, render_template, request
from pyhive import hive
from datetime import datetime, timedelta
import json
import plotly.graph_objects as go



today = datetime.today().strftime('%Y-%m-%d')

# Flask 앱을 생성합니다.
app = Flask(__name__)


def check_time_range(time_str):
    try:
        # 입력된 시간을 파싱하여 시간과 분을 추출
        time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

        # 분 단위로 변환
        total_minutes =  time_obj.minute
        tmp_min = 0
        
        # 분 단위로 범위를 확인하고 출력
        if 0 <= total_minutes < 5:
            tmp_min = 5
        elif 5 <= total_minutes < 10:
            tmp_min = 10
        elif 10 <= total_minutes < 15:
            tmp_min = 15
        elif 15 <= total_minutes < 20:
            tmp_min = 20
        elif 20 <= total_minutes < 25:
            tmp_min = 25
        elif 25 <= total_minutes < 30:
            tmp_min = 30
        elif 30 <= total_minutes < 35:
            tmp_min = 35
        elif 35 <= total_minutes < 40:
            tmp_min = 40
        elif 40 <= total_minutes < 45:
            tmp_min = 45
        elif 45 <= total_minutes < 50:
            tmp_min = 50
        elif 50 <= total_minutes < 55:
            tmp_min = 55
        elif 55 <= total_minutes:
            time_obj = time_obj + timedelta(hours=1)
            tmp_min = 0
        else:
            return "기타"
    except Exception as e:
       return "잘못된 형식의 입력입니다."
    
    return time_obj.replace(minute=tmp_min, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


# 주식 데이터를 가져오는 함수를 정의합니다.
def get_stock_data(ticker, start_date, end_date):
    data = yf.download(ticker, start=start_date, end=end_date)
    return data



def fetch_news_data(news_date):
    # Hive 쿼리 작성
    query = f"""
    SELECT *
    FROM news_hive
    WHERE SUBSTR(create_date, 1, 10) = '{news_date}'
    """
    
    # Hive 서버에 연결
    conn = hive.connect(host='43.201.197.216', port=10000, username='root')
    
    # 쿼리 실행
    cursor = conn.cursor()
    cursor.execute(query)
    
    # 결과 데이터를 판다스 데이터프레임으로 가져오기
    news_data = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    
    # 연결 닫기
    conn.close()

    news_data['news_hive.themes'] = news_data['news_hive.themes'].apply(lambda x: json.loads(x))
    news_data['news_hive.predicted_class_probabilities'] = news_data['news_hive.predicted_class_probabilities'].apply(lambda x: json.loads(x))
    
    return news_data

def fetch_stock_data(symbol, date):
    # Hive 쿼리 작성
    query = f"""
    SELECT *
    FROM stock_hive
    WHERE symbol = '{symbol}' AND CAST(SUBSTR(`timestamp`, 1, 10) AS DATE) = DATE '{date}'
    """
    
    # Hive 서버에 연결
    conn = hive.connect(host='43.201.197.216', port=10000, username='root')
    
    # 쿼리 실행
    cursor = conn.cursor()
    cursor.execute(query)
    
    # 결과 데이터를 판다스 데이터프레임으로 가져오기
    data = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    
    # 연결 닫기
    conn.close()
    
    return data


def process_data():

    # 종목 코드와 날짜 입력
    date = "2023-11-01"  # 원하는 날짜 입력

    # 함수 호출하여 데이터 가져오기
    news_data = fetch_news_data(date)

    symbol = "005930"  # 종목 코드 입력

    # 함수 호출하여 데이터 가져오기
    stock_data = fetch_stock_data(symbol, date)
    stock_data['stock_hive.timestamp'] = pd.to_datetime(stock_data['stock_hive.timestamp'])

    news_data['news_hive.create_date'] = news_data['news_hive.create_date'].apply(check_time_range)
    news_data['news_hive.create_date'] = pd.to_datetime(news_data['news_hive.create_date'])

    news_data = news_data.drop(columns=['news_hive.category', 'news_hive.site_name', 'news_hive.content','news_hive.url'])
    stock_data = stock_data.drop(columns=['stock_hive.symbol'])

    news_data = news_data[news_data['news_hive.themes'].apply(lambda x: bool(x))]

    news_data.set_index('news_hive.create_date', inplace=True)
    stock_data.set_index('stock_hive.timestamp', inplace=True)

    
    df_result = pd.DataFrame()  
    #selected_index = ['2023-11-01 11:30:00']

    for index in stock_data.index:
        #print(index)
        news_result = news_data[news_data.index.isin([index])]  # df2에서 해당 행을 선택하여 result에 추가
        stock_result =  stock_data[stock_data.index.isin([index])] 


        #print(stock_result)
        selected_  = pd.DataFrame()  
        #selected_ = pd.concat([selected_, stock_result], axis=1)
        #print(selected_)
        
        cnt = 1
        for index, row in news_result.iterrows():
            temp_df = row.to_frame().T
            temp_df.columns = [f'{col}_{cnt}' for col in temp_df.columns]
            selected_ = pd.concat([selected_, temp_df], axis=1)
            cnt += 1
            if cnt > 5:
                break

        df_result = pd.concat([df_result, selected_], axis=0)


    selected_
    df_result

    col=[i for i in df_result.columns]
    
    joined_df = stock_data.join(df_result, how='outer')

    return joined_df, col



# 주식 데이터를 차트로 그리는 함수를 정의합니다.
def plot_stock_chart(joined_df, col):

    hovertext = []
    for index, row in joined_df.iterrows():
        text_parts = [f'{row[col]}' for col in col]
        hovertext.append('<br>'.join(text_parts))

    fig = go.Figure(data=[go.Candlestick(x=joined_df.index,
                    open=joined_df['stock_hive.open'], high=joined_df['stock_hive.high'],
                    low=joined_df['stock_hive.low'], close=joined_df['stock_hive.close'],
                    hovertext=hovertext, hoverinfo='text')
                        ])

    fig.update_layout(title='주식 차트',
                    xaxis_title='날짜',
                    yaxis_title='가격',
                    xaxis_rangeslider_visible=False)

    return fig



@app.route('/')
def dashboard():
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']  # 대시보드에 표시할 종목 리스트입니다.
    return render_template('stock_dashboard.html', tickers=tickers)  # 종목 리스트를 HTML에 전달합니다.




@app.route('/chart/<ticker>', methods=['GET', 'POST'])  # POST 메소드를 추가합니다.
def stock_chart(ticker):
    if request.method == 'POST':  # POST 요청이면 사용자가 입력한 날짜를 가져옵니다.
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
    else:  # GET 요청이면 기본 날짜를 설정합니다.
        start_date = today 
        end_date = today 

    data, col = process_data()
    chart = plot_stock_chart(data, col)

    chart_html = pio.to_html(chart, full_html=False)

    return render_template('stock_chart.html', chart=chart_html, ticker=ticker, start_date=start_date, end_date=end_date)  # 시작 및 종료 날짜도 템플릿에 전달합니다.


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
