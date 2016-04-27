# Определение половозрастного состава группы "Сбербанк: Банк друзей" (vk.com/bankdruzey).
## Создаём standalone приложение в vk.com
Для запросов к методам api Вконтакте нам необходим access_token, формированием которого сейчас и займёмся.

Заходим на [страницу приложений](https://vk.com/apps?act=manage). Создаём "vk_sber".

Затем необходимо выполнить запрос вида [https://oauth.vk.com/authorize?client_id=1234567&display=page&scope=friends,photos,audio,video,docs,notes,pages,status,wall,groups,messages,email,notifications,offline&redirect_uri=https://oauth.vk.com/blank.html&display=page&v=5.5&response_type=code](https://oauth.vk.com/authorize?client_id=1234567&display=page&scope=friends,photos,audio,video,docs,notes,pages,status,wall,groups,messages,email,notifications,offline&redirect_uri=https://oauth.vk.com/blank.html&display=page&v=5.5&response_type=code), где client_id - id приложения, scope - разрешения на свой аккаунт, redirect_uri - uri, куда будет переадресован запрос, который выведет нужный нам потом code.

Получаем ответ: [https://oauth.vk.com/blank.html#code=00ab1204e0c2m24a63](https://oauth.vk.com/blank.html#code=04fb9303d1c2b24a62).

Формируем следующий запрос: [https://oauth.vk.com/access_token?client_id=1234567&client_secret=jhJuuhNJGJh6jjj&code=00ab1204e0c2m24a63&redirect_uri=https://oauth.vk.com/blank.html&display=page&v=5.5&response_type=token](https://oauth.vk.com/access_token?client_id=1234567&client_secret=jhJuuhNJGJh6jjj&code=00ab1204e0c2m24a63&redirect_uri=https://oauth.vk.com/blank.html&display=page&v=5.5&response_type=token), где client_id - id приложения, client_secret - секретное слово приложения,response_type=token - на выходе и получим нужный нам токен.

Ответ: {"access_token":"1111111111111111111111111111111111111111111111111111111111111","expires_in":0,"user_id":1111111111}

Для запросов к api нам и нужен будет access_token.

## Хранимые процедуры Вконтакте
Api соцсети имеет ограничение на вызовы. Для вызова до 25 методов на стороне сервера существуют хранимые процедуры. Синтаксис похож на JavaScript.

Нам нужно получить список пользователей.

Создаём 3 процедуры.

#### groupsMembersCnt
```javascript
return API.groups.getMembers(
    {"group_id": Args.group_id,
    "count": 0}).count;
```
#### groupMembers
```javascript
var group_id = Args.group_id;
var lower_bound = parseInt(Args.lower_bound);
var count = parseInt(Args.count);
var upper_bound = parseInt(Args.upper_bound);
while(lower_bound < upper_bound){
    members = members + API.groups.getMembers(
        {"group_id": group_id,
        "sort": "id_asc",
        "count": count,
        "fields":["sex", "bdate", "city", "country", "education"],
        "offset": lower_bound}
    ).users;
    lower_bound = lower_bound + count;
}
return members;
```
#### usersFriends
```javascript
var users_str = Args.users;
var users = users_str.split(",");
var len = users.length;
var i = 0;
var friends = [];
while(len > 0){
    friends = friends +
    [[parseInt(users[i])] +
    API.friends.get(
    {"user_id": parseInt(users[i]),
    "fields":["sex", "bdate"],
    "offset": 0}).items];
    len = len - 1;
    i = i + 1;
}
return friends;
```

## Получение списка участников группы "Сбербанк: Банк друзей"
Для этого используем скрипт getMembers.py. Параметры запросов хранятся в settings.py (access_token, group_id, offset, count, max_count).

Он скачивает данные по всем пользователя группы (65 циклов по группе Сбербанка за 352 секунды) и сохраняет в массиве в ОЗУ, затем скидывает в файл. Можно записывать данные в файл в каждой итерации, если хочется сэкономить память или если её мало.
Получили файл json размером 213 MB и csv размером 85 MB. Последний и будем использовать.

Проверяем.
Считаем в bash количество uid в получившемся файле:
```bash
$ tr ' ' '\n' < bankdruzey_members.csv | grep -c uid
```
Вышло 1 609 837, - похоже на правду.

Затем можно посмотреть содержание (первые 10 значений).
```bash
$ tr '{' '\n' < bankdruzey_members.csv | head -n 10
```
Ок, то, что нужно.

## Обработка в Hortonworks (HW)
Необходмимо загрузить данные в HW, очистить от пользователей со статусом deactivated или blocked и оставить пользователей только с полной датой рождения.

Загружаем данные.
```bash
hadoop fs -mkdir /demo/data/vk
hadoop fs -put bankdruzey_members.csv /demo/data/vk
```

Выполняем запрос в Pig.
```Pig
members = LOAD '/demo/data/vk/bankdruzey_members.csv' USING PigStorage(',')
	AS (uid:int,
    	first_name:chararray,
        last_name:chararray,
        sex:int,
        bdate:chararray,
        country:int,
        city:int,
        deactivated:chararray);
DESCRIBE members;

members2 = FOREACH (FILTER members BY SIZE(bdate) > 5 AND deactivated IS NULL)
	GENERATE uid, sex, YearsBetween(CurrentTime(), ToDate(bdate,'d.M.y')) as age;
    
members_done = FOREACH (GROUP members2 BY (sex, age))
		   GENERATE FLATTEN(group) as (sex, age),
		   COUNT(members2);
           
DUMP members_done;
STORE members_done INTO '/demo/data/vk/members_done';
```

Получили количество участников группы "Банк друзей" vk.com в разрезе пола (0 - не указан, 1 - женщины, 2 - мужчины) и возраста.

## Получение списка друзей пользователей (не готово)
Для тех, кто не заблокирован и у кого отстутствует полная дата рождения, получаем список друзей. И вычисляем средний возраст (предполагаем, что распределение нормально) друзей по каждому участнику группы. Присваиваем получившееся значение участникам группы.
Хранимая процедура vk готова - указана выше.

Скриптом Python разбиваем массив участников без даты рождения на куски по 25 и передаём в запросе к api. Хранимая процедура проходит циклом по массиву и возвращает список друзей каждого участника из массива.

Участников без даты рождения и незаблокированных получилось 841 тыс. Если предположить, что у каждого по 200 друзей, то получим около 200 млн пользователей. Которых также нужно очистить от деактивированных и без полной даты рождения.
