from datetime import datetime, timedelta
import websocket
import threading
import json
import psycopg2


coins_list = [
    "xrpusdt",
    "adausdt",
    "maticusdt",
    "atomusdt"
]


class DataBase:
    def __init__(self):
        self.user = "postgres"
        self.password = "0000"
        self.host = "localhost"
        self.port = 5432
        self.database = "StreamSignal"

    def connect(self):
        return psycopg2.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database
        )

    def insert_trades(self, coin, timestamp, quantity, price, side):
        conn = self.connect()
        cursor = conn.cursor()
        if side:
            cursor.execute(f"INSERT INTO trade_buyers_{coin} (timestamp, quantity, side, price) VALUES (%s, %s, %s, %s)",
                           (timestamp, quantity, side, price))
        else:
            cursor.execute(f"INSERT INTO trade_sellers_{coin} (timestamp, quantity, side, price) VALUES (%s, %s, %s, %s)",
                           (timestamp, quantity, side, price))
        conn.commit()

    def calculate_aggression_buyers(self, coin):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f"SELECT SUM(quantity) AS total_quantity, COUNT(*) AS total_count FROM trade_buyers_{coin};")
        record = cursor.fetchone()
        print(f"Total {coin} Buyers Quantity:", record[0])
        print(f"Total {coin} Buyers Count:", record[1])

    def calculate_aggression_sellers(self, coin):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f"SELECT SUM(quantity) AS total_quantity, COUNT(*) AS total_count FROM trade_sellers_{coin};")
        record = cursor.fetchone()
        print(f"Total {coin} Sellers Quantity:", record[0])
        print(f"Total {coin} Sellers Count:", record[1])

    def delete_rows_by_time(self, coin, table_name, timestamp_column, start_time, end_time):
        conn = self.connect()
        cursor = conn.cursor()
        delete_query = f"DELETE FROM {table_name}_{coin} WHERE {timestamp_column} >= %s AND {timestamp_column} < %s;"
        cursor.execute(delete_query, (start_time, end_time))
        conn.commit()


class Streaming(websocket.WebSocketApp):
    def __init__(self, coin, url):
        self.coin = coin
        super().__init__(url=url, on_open=self.on_open, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close)
        self.run_forever()

    def on_open(self, ws):
        print(f'Websocket for {self.coin} was opened')

    def on_message(self, ws, msg):
        data = json.loads(msg)
        # Process the incoming message
        self.process_message(data)

    def on_error(self, ws, e):
        print(f'Error for {self.coin}', e)

    def on_close(self, ws):
        print(f'Closing websocket for {self.coin}')

    def process_message(self, data):
        # Initialize your DataBase instance
        db = DataBase()

        # Get the current time
        current_time = datetime.now()

        # Update rows every 15 minutes
        if current_time.minute % 15 == 0:
            start_time = current_time - timedelta(minutes=15)
            end_time = current_time

            # Delete rows within the last 15 minutes
            db.delete_rows_by_time(self.coin, 'trade_buyers', 'timestamp', start_time, end_time)
            db.delete_rows_by_time(self.coin, 'trade_sellers', 'timestamp', start_time, end_time)

        # Insert the current trade
        db.insert_trades(self.coin, current_time, data['q'], data['p'], data['m'])

        # Calculate aggression
        db.calculate_aggression_buyers(self.coin)
        db.calculate_aggression_sellers(self.coin)


if __name__ == '__main__':
    for coin in coins_list:
        threading.Thread(target=Streaming, args=(coin, f'wss://fstream.binance.com/ws/{coin.lower()}@aggTrade',)).start()
