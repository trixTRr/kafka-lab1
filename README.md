# kafka-lab1
Как запустить:
1. ===Зайдите в командную строку и введите:=== 
wsl -d Ubuntu
cd /mnt/c/kafka-lab1
python3 -m venv venv
source venv/bin/activate

2.Активируйте WSL интеграцию в Docker Desktop

3. ===Запустите проект в командной строке (в WSL):===
./run.sh

Конкретная задача в работе:
Обнаружение аномалий в работе IoT-датчиков в реальном времени.

Решение, реализованное в работе
Мы строим конвейер потоковой обработки, который:

1. Имитирует поток данных (producer)
Читает заранее сгенерированный датасет и отправляет в Kafka с задержками — как в реальной жизни

2. Надёжно передаёт данные (Kafka)
Данные не теряются даже при сбоях, и их можно масштабировать на тысячи датчиков. Поддерживает несколько потребителей одновременно

3. Обрабатывает и сохраняет (consumer)
Очищает данные, добавляет признаки, удаляет дубликаты и готовит данные для ML

4. Обучает модель обнаружения аномалий (ML)
Модель учится отличать нормальные показания от аномальных. Например: если температура резко выросла при нормальной вибрации — это может быть аномалия. Также устанавливается, какие датчики лучше всего предсказывают аномалии.

<img width="350" height="291" alt="image" src="https://github.com/user-attachments/assets/2605da79-96bf-4a48-bb23-a9ec625b4dbc" />


5. Визуализирует результаты (dashboard)
Показывает текущие метрики и подсвечивает аномалии.

Демонстрация различных конфигураций:

<img width="668" height="433" alt="image" src="https://github.com/user-attachments/assets/ed78ff70-e352-4f13-aa34-e1d44c27c185" />

<img width="646" height="263" alt="image" src="https://github.com/user-attachments/assets/c8b71b28-5dd8-4966-a5ae-17e267497c89" />

<img width="794" height="340" alt="image" src="https://github.com/user-attachments/assets/022e9be7-70e1-47db-b5e9-bcd7d063cc11" />

После выполнения программы можно посмотреть Kafka UI по адресу http://localhost:8080 и визуализацию по адресу http://localhost:8501.

Kafka UI:

<img width="1920" height="720" alt="image" src="https://github.com/user-attachments/assets/b57cade6-0008-4197-8f4f-abc8c827ad23" />

Брокеры:

<img width="1601" height="703" alt="image" src="https://github.com/user-attachments/assets/67d6d9d1-3013-4757-83bc-1a646904538e" />

Топики:

<img width="1621" height="741" alt="image" src="https://github.com/user-attachments/assets/10eab6b6-7026-4f49-b414-548268c2ef93" />

Сообщения в топике rf2-test:

<img width="1619" height="808" alt="image" src="https://github.com/user-attachments/assets/f568e805-9d2b-4c79-8444-6bed03aa5f32" />

Consumers:

<img width="1628" height="563" alt="image" src="https://github.com/user-attachments/assets/0bebbced-073e-4a11-9d82-ed5b4d51c7e9" />

Визуализация (Streamlit):

<img width="1920" height="826" alt="image" src="https://github.com/user-attachments/assets/9008ff07-881f-40bc-863a-c1368c55711f" />

Распределения температуры и влажности:

<img width="1920" height="713" alt="image" src="https://github.com/user-attachments/assets/16833077-68cd-4df7-9f08-401bfd2e75f6" />

Температура по времени:

<img width="1919" height="675" alt="image" src="https://github.com/user-attachments/assets/9bba8fcd-ae4e-40ec-9328-9ef759cdbe4c" />

Данные:

<img width="1920" height="840" alt="image" src="https://github.com/user-attachments/assets/ccc8861a-4f25-4c00-a4ad-1a2aec7d1eb8" />
