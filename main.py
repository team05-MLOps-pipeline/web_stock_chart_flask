# 필요한 라이브러리를 임포트합니다.
import yfinance as yf
import pandas as pd
import plotly.express as px
import plotly.io as pio
from flask import Flask, render_template, request
from datetime import datetime


today = datetime.today().strftime('%Y-%m-%d')

# Flask 앱을 생성합니다.
app = Flask(__name__)

# 주식 데이터를 가져오는 함수를 정의합니다.
def get_stock_data(ticker, start_date, end_date):
    data = yf.download(ticker, start=start_date, end=end_date)
    return data


# 주식 데이터를 차트로 그리는 함수를 정의합니다.
def plot_stock_chart(data):
    fig = px.line(data, x=data.index, y="Close", title="Stock Price Chart")
    fig.update_layout(autosize=True)  # 그래프를 반응형으로 만듭니다.
    return fig



@app.route('/')
def dashboard():
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']  # 대시보드에 표시할 종목 리스트입니다.
    return render_template('stock_dashboard6.html', tickers=tickers)  # 종목 리스트를 HTML에 전달합니다.




@app.route('/chart/<ticker>', methods=['GET', 'POST'])  # POST 메소드를 추가합니다.
def stock_chart(ticker):
    if request.method == 'POST':  # POST 요청이면 사용자가 입력한 날짜를 가져옵니다.
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
    else:  # GET 요청이면 기본 날짜를 설정합니다.
        start_date = today 
        end_date = today 

    data = get_stock_data(ticker, start_date, end_date)
    chart = plot_stock_chart(data)

    chart_html = pio.to_html(chart, full_html=False)

    return render_template('stock_chart6.html', chart=chart_html, ticker=ticker, start_date=start_date, end_date=end_date)  # 시작 및 종료 날짜도 템플릿에 전달합니다.


if __name__ == '__main__':
    app.run()
