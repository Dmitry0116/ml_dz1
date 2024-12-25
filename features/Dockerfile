FROM python:3.9

WORKDIR /usr/src/app

# Копируем исходный код в контейнер
COPY ./src ./

# Копируем файл зависимостей
COPY ./requirements.txt ./

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r /usr/src/app/requirements.txt

# Запускаем основной файл
CMD [ "python", "./features.py" ]
