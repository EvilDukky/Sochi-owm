# Подключаем библиотеки
import psycopg2
import requests
import json
import time
import schedule
import math
import os

# Открытие и вытаскивание данных из файла конфигурации
f = open('config.json', 'r', encoding='utf-8')  # открываем конфигурационный файл json
text = json.load(f)  # загнали все из файла в переменную

for txt in text['owm']:  # создали цикл, который будет работать построчно
    lat = (txt['lat'])  # вытаскиваем переменные из файла
    lon = (txt['lon'])
    appid = (txt['appid'])

for txt in text['postgres']:  # создали цикл, который будет работать построчно
    host = (txt['host'])  # вытаскиваем переменные из файла
    user = (txt['user'])
    password = (txt['password'])
    database = (txt['database'])
    port = (txt['port'])


# Функция отправки сообщения в беседу телеграмм
def telega_msg(msg, file):
    bot_tokken = '5539654282:AAG5-ac6Fsf_PINWHv9I_s9mGfqaL-x79_k'
    data1 = {'chat_id': '-834541841', 'text': msg}
    files = {'document': file}
    data2 = {'chat_id': '-834541841'}
    req1 = requests.post('https://api.telegram.org/bot' + bot_tokken + '/sendMessage', data=data1)
    # print(req1)
    req2 = requests.post('https://api.telegram.org/bot' + bot_tokken + '/sendDocument', data=data2, files=files)
    # print(req2)


def weather():
    timecount = str(time.strftime("%b_%d_%H"))

    os.system('sudo touch /home/ubuntu/owm_logs_' + timecount + '.json')
    os.system('sudo chmod 666 /home/ubuntu/owm_logs_' + timecount + '.json')
    sendFile = '/home/ubuntu/owm_logs_' + timecount + '.json'
    # sendFile = ('owm_logs_' +timecount + '.json')
    msg = '@LoshkarevVA, аларм, бегом чекать логи Sochi:'

    try:  # парсинг погоды
        res = requests.get("http://api.openweathermap.org/data/2.5/weather",
                           params={'lat': lat, 'lon': lon, 'units': 'metric', 'lang': 'eng',
                                   'APPID': appid})  # записываем полученные данные в переменную
        data = res.json()
        cod = data['cod']
        if cod != 200:
            code = str(cod)
            file = open(sendFile, "w")
            simbol = "{"
            file.write(simbol + "\n")
            simbol = "\"weather:\""
            file.write(simbol + "\n")
            simbol = "\"error code: \" ,"
            file.write(simbol + code + "\n")
            simbol = "}"
            file.write(simbol)
            file.close()
            file = open(sendFile, "r")
            telega_msg(msg, file)
        coord_lat = data['coord']['lat']  # вытаскиваем переменные из массива
        coord_lon = data['coord']['lon']
        unix_time = data['dt']
        temp = data['main']['temp']
        temp_feels = data['main']['feels_like']
        pressure = data['main']['pressure']
        pressure_sea = data['main']['sea_level']
        pressure_grnd = data['main']['grnd_level']
        humidity = data['main']['humidity']
        visibility = data['visibility']
        wind_speed = data['wind']['speed']
        wind_gust = data['wind']['gust']
        wind_deg = data['wind']['deg']
        clouds = data['clouds']['all']
        sunrise_unix = data['sys']["sunrise"]
        sunset_unix = data['sys']["sunset"]

    except Exception as e:

        pass

    try:
        snow_1h = data['snow']['1h']
    except Exception as e:

        snow_1h = 0
        pass

    try:
        rain_1h = data['rain']['1h']
    except Exception as e:

        rain_1h = 0
        pass

        os.system('sudo rm ubuntu/owm.logs.' + timecount + '.json')
    wind_direction = "None"
    if wind_deg < 11.25 or wind_deg > 348.75:
        wind_direction = "N"
    elif wind_deg > 11.25 or wind_deg < 33.75:
        wind_direction = "NNE"
    elif wind_deg > 33.75 or wind_deg < 56.25:
        wind_direction = "NE"
    elif wind_deg > 56.25 or wind_deg < 78.75:
        wind_direction = "ENE"
    elif wind_deg > 78.75 or wind_deg < 101.25:
        wind_direction = "E"
    elif wind_deg > 101.25 or wind_deg < 123.75:
        wind_direction = "ESE"
    elif wind_deg > 123.75 or wind_deg < 146.25:
        wind_direction = "SE"
    elif wind_deg > 146.25 or wind_deg < 168.75:
        wind_direction = "SSE"
    elif wind_deg > 168.75 or wind_deg < 191.25:
        wind_direction = "S"
    elif wind_deg > 191.25 or wind_deg < 213.75:
        wind_direction = "SSW"
    elif wind_deg > 213.75 or wind_deg < 236.25:
        wind_direction = "SW"
    elif wind_deg > 236.25 or wind_deg < 258.75:
        wind_direction = "WSW"
    elif wind_deg > 258.75 or wind_deg < 281.25:
        wind_direction = "W"
    elif wind_deg > 281.25 or wind_deg < 303.75:
        wind_direction = "WNW"
    elif wind_deg > 303.75 or wind_deg < 326.25:
        wind_direction = "NW"
    elif wind_deg > 326.25 or wind_deg < 348.75:
        wind_direction = "NNW"

        timecount = str(time.strftime("%b_%d_%H"))

        os.system('sudo touch /home/ubuntu/owm_logs_' + timecount + '.json')
        os.system('sudo chmod 666 /home/ubuntu/owm_logs_' + timecount + '.json')
        sendFile = '/home/ubuntu/owm_logs_' + timecount + '.json'
        # sendFile = ('owm_logs_' + timecount + '.json')
        msg = '@LoshkarevVA, аларм, бегом чекать логи Sochi:'
        # подключаемся к бд
        try:
            connection = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=port
            )

            # подключаемся к таблице и заполняем ее данными которые получили
            cur = connection.cursor()
            cur.execute(
                '''INSERT INTO sochi_openweather
                (coord_lat,coord_lon,unix_time,temp,temp_feels,
                pressure,pressure_sea,pressure_grnd,humidity,
                visibility,wind_speed,wind_gust,wind_deg,wind_direction,snow_1h,rain_1h,
                clouds,sunrise_unix,sunset_unix)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''',
                (coord_lat, coord_lon, unix_time, temp, temp_feels,
                 pressure, pressure_sea, pressure_grnd, humidity,
                 visibility, wind_speed, wind_gust, wind_deg, wind_direction, snow_1h, rain_1h,
                 clouds, sunrise_unix, sunset_unix)
            )
            connection.commit()

        except Exception:
            file = open(sendFile, "w")
            simbol = "{"
            file.write(simbol + "\n")
            simbol = "\"check:\""
            file.write(simbol + "\n")
            simbol = "\"Database has not open\","
            file.write(simbol + "\n")
            simbol = "\"error:\","
            file.write(simbol + "\n")
            simbol = "}"
            file.write(simbol)
            file.close()
            file = open(sendFile, "r")
            telega_msg(msg, file)
            os.system('sudo rm /home/ubuntu/owm_logs_' + timecount + '.json')
        if connection:
            # cur.close()
            connection.close()
            # print("Соединение с PostgreSQL закрыто")

    timecount = str(time.strftime("%b_%d_%H"))

    os.system('sudo touch /home/ubuntu/owm_logs_' + timecount + '.json')
    os.system('sudo chmod 666 /home/ubuntu/owm_logs_' + timecount + '.json')
    sendFile = '/home/ubuntu/owm_logs_' + timecount + '.json'
    # sendFile = ('owm_logs_' +timecount + '.json')
    msg = 'LoshkarevVA, код успешно задеплоен! Sochi:'

    file = open(sendFile, "w")

    # Проверка на потерю данных за час
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
    )
    simbol = "{"
    file.write(simbol + "\n")
    simbol = "\"check:\""
    file.write(simbol + "\n")
    simbol = "\"Database opened successfully\","
    file.write(simbol + "\n")

    # print("Database opened successfully")

    cur = connection.cursor()
    cur.execute(
        '''SELECT unix_time from owm_python_parser''')
    rows = cur.fetchall()
    for row in rows: unix = row[0]

    t = math.trunc(time.time())
    t = t / 3600
    t = math.trunc(t)
    t = t * 3600
    if unix < (t + 600) and unix > (t - 600):
        simbol = "\"true\""
        file.write(simbol + "\n")
        simbol = "}"
        file.write(simbol)
        file.close()
        file = open(sendFile, "r")
        telega_msg(msg, file)
        os.system('sudo rm /home/ubuntu/owm_logs_' + timecount + '.json')
    else:
        simbol = "\"false\""
        file.write(simbol + "\n")
        simbol = "}"
        file.write(simbol)
        file.close()
        file = open(sendFile, "r")
        telega_msg(msg, file)
        os.system('sudo rm /home/ubuntu/owm_logs_' + timecount + '.json')
        weather()

    # Указываем время в которое будет срабатывать сбор погодных данных
    schedule.every().hour.at(":00").do(weather)

    # бесконечный цикл, проверяющий каждую секунду, не пора ли запустить задание
    while 1:
        schedule.run_pending()