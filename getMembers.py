import requests
import json
import csv
import math
import time
from settings import access_token, group_id, offset, count, max_count

# Получаем количество участников группы
get_group_members_count = requests.post(
    'https://api.vk.com/method/execute.groupsMembersCnt?group_id=' + group_id + '&access_token=' + access_token)
group_members_count = get_group_members_count.json()['response']
time.sleep(1)

members = []
lower_bound = offset
upper_bound = max_count
start_time = time.time()
procedure_start_time = start_time
end_time = start_time
counter = 0

# Вычисляем количество вызовов процедуры
requests_count = math.ceil(group_members_count / (max_count - offset))  # max = 3 api call per second

# Создаём список участников группы
for i in range(requests_count):
    # Если выполнили 3 запроса и врмение прошло меньше 1 секунды, то задерживаем скрипт на (1.1 - время выполнения)
    if counter % 3 == 0 and end_time - start_time < 1:
        time.sleep(1.1 - (end_time - start_time))
        start_time = time.time()
    req = requests.post(
        'https://api.vk.com/method/execute.groupMembers?group_id=' + group_id + '&lower_bound=' + str(lower_bound) +
        '&count=' + str(count) + '&upper_bound=' + str(upper_bound) + '&access_token=' + access_token)
    members.extend(req.json()['response'])
    lower_bound = upper_bound
    upper_bound = upper_bound + max_count
    end_time = time.time()
    counter += 1

# Чистим даты рождения
for i in members:
    try:
        time.strptime(i['bdate'], '%d.%m.%Y')
    except (ValueError, KeyError):
        i['bdate'] = ''

# Записываем в файлы json и csv
with open('bankdruzey_members.json', 'w') as f:
    f.write(json.dumps(members))

with open('bankdruzey_members.csv', 'w') as f:
    z = csv.writer(f)
    z.writerow(["uid", "first_name", "last_name", "sex", "bdate", "country", "city", "deactivated"])
    for i in members:
        z.writerow([i.get("uid", ""),
                    i.get("first_name", ""),
                    i.get("last_name", ""),
                    i.get("sex", ""),
                    i.get("bdate", ""),
                    i.get("country", ""),
                    i.get("city", ""),
                    i.get("deactivated", "")])

print("Procedure executed and files created in " + str(time.time() - procedure_start_time) + " seconds")
