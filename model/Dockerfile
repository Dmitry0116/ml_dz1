FROM python:3.9

WORKDIR /usr/src/app

# Копируем исходный код (включая model.py) в контейнер
COPY ./src /usr/src/app/src

# Копируем файл зависимостей
COPY ./requirements.txt /usr/src/app/requirements.txt

# Копируем файл myfile.pkl в нужное место
COPY ./src/myfile.pkl /usr/src/app/src/myfile.pkl

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r /usr/src/app/requirements.txt

# Запускаем основной файл
CMD ["python", "/usr/src/app/src/model.py"]

