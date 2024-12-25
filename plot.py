import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time

while True:
    try:
        # Чтение данных
        df = pd.read_csv("D:/lab/logs/metric_log.csv", header=None)
        
        # Печать первых строк данных для диагностики
        print(f"Первые строки данных:\n{df.head()}")
        
        # Извлечение абсолютной ошибки
        df['absolute_error'] = df.iloc[:, 3]
        
        # Построение гистограммы и кривой плотности
        plt.figure(figsize=(10, 6))
        
        # Гистограмма
        plt.hist(df['absolute_error'], bins=20, color='skyblue', edgecolor='black', density=True, alpha=0.6, label='Гистограмма')
        
        # Кривая плотности
        sns.kdeplot(df['absolute_error'], color='red', lw=2, label='Кривая плотности') 
        
        plt.title('Распределение абсолютных ошибок с кривой плотности')
        plt.xlabel('Абсолютная ошибка')
        plt.ylabel('Плотность')
        plt.legend()  # Добавление легенды
        
        # Сохранение графика
        plt.savefig("D:/lab/logs/error_distribution.png")
        plt.close()
        
        # Задержка перед следующим обновлением
        time.sleep(10)
    except Exception as e:
        print(f"Ошибка при построении гистограммы: {e}")
        time.sleep(10)  # Задержка при ошибке