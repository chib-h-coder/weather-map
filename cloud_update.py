import os
import datetime
import urllib.request
import subprocess
import pandas as pd
import json

# =========================================================
# クラウド用設定 (相対パス & Linuxコマンド)
# =========================================================
# wgrib2はGitHub Actions上でインストールされるため、パス指定なしでOK
wgrib2_cmd = 'wgrib2' 
download_file_name = 'latest_msm.bin'
csv_file_name = 'latest_output.csv'
json_file_name = 'weather_data.json'

def get_msm_url():
    now = datetime.datetime.now(datetime.timezone.utc)
    target_time = now - datetime.timedelta(hours=4)
    hour = (target_time.hour // 3) * 3
    target_time = target_time.replace(hour=hour, minute=0, second=0, microsecond=0)
    
    year = target_time.strftime('%Y')
    month = target_time.strftime('%m')
    day = target_time.strftime('%d')
    time_str = target_time.strftime('%Y%m%d%H0000')
    
    filename = f"Z__C_RJTD_{time_str}_MSM_GPV_Rjp_Lsurf_FH00-15_grib2.bin"
    url = f"http://database.rish.kyoto-u.ac.jp/arch/jmadata/data/gpv/original/{year}/{month}/{day}/{filename}"
    print(f"Target Time (UTC): {target_time.strftime('%Y-%m-%d %H:%M')}")
    return url

def main():
    # 1. Download
    try:
        url = get_msm_url()
        print(f"1. Start Download: {url}")
        urllib.request.urlretrieve(url, download_file_name)
        print("   Download Complete!")
    except Exception as e:
        print(f"Error: Download failed.\n{e}")
        return

    # 2. Convert to CSV (Linux command)
    print("2. Converting to CSV...")
    # Linux環境ではそのままコマンドとして実行可能
    cmd = f'{wgrib2_cmd} {download_file_name} -csv {csv_file_name}'
    subprocess.run(cmd, shell=True)
    
    # 3. Create JSON
    print("3. Creating JSON data...")
    try:
        if not os.path.exists(csv_file_name):
            print("Error: CSV file not found.")
            return

        df = pd.read_csv(csv_file_name, header=None)
        df.columns = ['time1', 'time2', 'variable', 'level', 'lon', 'lat', 'value']

        rain_df_all = df[df['variable'].str.contains('APCP|Precipitation', case=False)]
        temp_df_all = df[df['variable'].str.contains('TMP|Temperature', case=False)]
        u_df_all = df[df['variable'].str.contains('UGRD', case=False)]
        v_df_all = df[df['variable'].str.contains('VGRD', case=False)]

        unique_times = sorted(rain_df_all['time2'].unique())
        output_data = {"times": unique_times, "datasets": {}}

        for t in unique_times:
            # Rain
            r_df = rain_df_all[rain_df_all['time2'] == t]
            r_df = r_df[r_df['value'] > 0] 
            r_list = r_df.iloc[::5][['lat', 'lon', 'value']].values.tolist()

            # Temp
            t_df = temp_df_all[temp_df_all['time2'] == t].copy()
            t_df['value'] = t_df['value'] - 273.15
            t_list = t_df.iloc[::10][['lat', 'lon', 'value']].values.tolist()

            # Wind
            u_frame = u_df_all[u_df_all['time2'] == t].reset_index(drop=True)
            v_frame = v_df_all[v_df_all['time2'] == t].reset_index(drop=True)
            wind_data = []
            step = 15
            
            import math
            if len(u_frame) == len(v_frame):
                for i in range(0, len(u_frame), step):
                    u = u_frame.at[i, 'value']
                    v = v_frame.at[i, 'value']
                    lat = u_frame.at[i, 'lat']
                    lon = u_frame.at[i, 'lon']
                    speed = math.sqrt(u*u + v*v)
                    angle_deg = math.degrees(math.atan2(v, u))
                    if speed > 1.0:
                        wind_data.append([lat, lon, round(speed,1), round(angle_deg,1)])
            
            output_data["datasets"][t] = {"rain": r_list, "temp": t_list, "wind": wind_data}

        with open(json_file_name, 'w') as f:
            json.dump(output_data, f)
        print("   Success! JSON saved.")

        # クリーンアップ (容量節約のため中間ファイルを消す)
        if os.path.exists(download_file_name): os.remove(download_file_name)
        if os.path.exists(csv_file_name): os.remove(csv_file_name)

    except Exception as e:
        print(f"Error: Processing failed.\n{e}")

if __name__ == "__main__":
    main()
