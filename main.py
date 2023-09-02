import sqlite3
import pandas as pd
import datetime

DATABASE_NAME = "cheaters.db"
con = sqlite3.connect(DATABASE_NAME)
cur = con.cursor()


def get_info_by_date(date: str):
    global con, cur

    specific_date = datetime.datetime.strptime(date, "%d.%m.%Y").date()
    df_client = pd.read_csv('client.csv')

    df_client['timestamp'] = pd.to_datetime(df_client['timestamp'], unit='s')
    filtered_data_client = df_client[df_client['timestamp'].dt.date == specific_date]

    df_server = pd.read_csv('server.csv')

    df_server['timestamp'] = pd.to_datetime(df_server['timestamp'], unit='s')

    filtered_data_server = df_server[df_server['timestamp'].dt.date == specific_date]

    merged_df = pd.merge(filtered_data_client, filtered_data_server, on='error_id', how='inner')

    select_all_sql = """
        SELECT * FROM cheaters
    """
    res = cur.execute(select_all_sql)
    res = res.fetchall()
    con.commit()

    df_cheater = pd.DataFrame(res, columns=['player_id', 'ban_time'])
    df_cheater['ban_time'] = pd.to_datetime(df_cheater['ban_time'])

    temp_filter_df = pd.merge(merged_df, df_cheater, on='player_id', how='inner')
    temp_filter_df['threshold_date'] = temp_filter_df['timestamp_y'] - pd.Timedelta(days=1)
    filtered_df = temp_filter_df[temp_filter_df['ban_time'] > temp_filter_df['threshold_date']]
    filtered_df = filtered_df.drop('timestamp_x', axis=1)
    filtered_df = filtered_df.drop('ban_time', axis=1)
    filtered_df = filtered_df.drop('threshold_date', axis=1)

    for index, row in filtered_df.iterrows():
        event_id = row['event_id']
        timestamp = row['timestamp_y']
        player_id = row['player_id']
        error_id = row['error_id']
        json_server = row['description_y']
        json_client = row['description_x']

        insert_sql = "INSERT INTO players (" \
                     "event_id, " \
                     "timestamp, " \
                     "player_id, " \
                     "error_id, " \
                     "json_server, " \
                     "json_client" \
                     ") VALUES (?, ?, ?, ?, ?, ?)"
        cur.execute(insert_sql, (
            event_id,
            str(int(timestamp.timestamp())),
            player_id,
            error_id,
            json_server,
            json_client
        ))
    con.commit()


if __name__ == '__main__':
    create_table_sql = '''
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER,
            timestamp DATETIME,
            player_id INTEGER,
            error_id TEXT,
            json_server TEXT,
            json_client TEXT
        )
    '''

    cur.execute(create_table_sql)
    con.commit()
    get_info_by_date("07.03.2021")
    con.close()
