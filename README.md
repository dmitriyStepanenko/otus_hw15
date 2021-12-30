Домашнее задание выполнено (-на) для курса [Python Developer. Professional](https://otus.ru/lessons/python-professional/?int_source=courses_catalog&int_term=programming)

# Go Memcached Loader

Go реализация скрипта, который парсит и
заливает в мемкеш поминутную выгрузку логов трекера
установленных приложений

Пример запуска
```
go run . --pattern=/<path_to_files>/*.tsv.gz
```

Доступные опции
- "-l" или "--log", устанавливает файл для логирования, default=None - вывод в консоль
- "--pattern", паттерн по которому делается glob для взятия файлов, default="/data/appsinstalled/*.tsv.gz"
- "--idfa", адрес memcache для idfa, default="127.0.0.1:33013"
- "--gaid", адрес memcache для gaid, default="127.0.0.1:33014"
- "--adid", адрес memcache для adid, default="127.0.0.1:33015"
- "--dvid", адрес memcache для dvid, default="127.0.0.1:33016"
- "--queue_size", размер очереди, default=10
- "--workers", количество вокеров для вычитывания файлов, default=3
- "--socket_timeout", таймаут для всех вызовов к memcached в секундах, default=3
- "--max_retry", количество попыток переотправить данные, default=3
- "--time_retry", время между переотправками в секундах, default=1