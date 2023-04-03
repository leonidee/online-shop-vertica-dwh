# Project Summary

Проект представляет собой пайплайн, завернутый в docker-compose с airflow.

Пайплайн забирает сырые данные из Data lake на s3, подготавливает их и складирует в хранилище по модели данных Data Vault развернутое на Vertica.

При первом запуске docker-compose airflow запускает даг инициализирующий DWH. Этот даг является триггером для нижестоящих по иерархии. 

# Pipeline

### Основные шаги пайплайна:

- Stage 0  - Создает оба слоя DWH на Vertica (выполняется только при первом запуске docker-compose)

- Stage 1 - Забирает данные из сырого слоя Data lake на s3

- Stage 2 - Очищает и генерирует дополнительные атрибуты к данным. Загружает данные  на чистый слой s3

- Stage 3 - Забирает с s3 данные и заполняет ими STG слой хранилища AS IS

- Stage 4 - Заполняет Хабы, Линки и Саттелиты DDS слоя хранилища

# Raw data

За базу взят открытый датасет Kaggle - [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), к которому он-топ генерируются дополнительные атрибуты  с помощью open-source библиотеки Faker (например, имена и email таблицы customers).



# Data Model

### Основное хранилище на Vertica состоит из двух слоев:

- STG слой - Хранит очищенные данные, перелитые из s3 без связей и модели данных

- DDS слой - Реализован по модели данных Data Valult. Содержит Хабы, Линки, Саттелиты по SCD1 и исторические Саттелиты по SCD2 

![data-model](https://github.com/Leonidee/online-shop-vertica-dwh/blob/master/addons/dds-layer-data-model.png?raw=true)

# DAGs

- `dwh-creator-dag` - Инициализирует DWH в Vertica. Создает все таблицы и связи между ними. Запускается один раз при первой развертке docker-compose. Является триггером для `get-data-dag`

![dwh-creator-dag](https://github.com/Leonidee/online-shop-vertica-dwh/blob/master/addons/dwh-creator-dag.png?raw=true)

- `get-data-dag` - Забирает сырые данные с s3, очищает и подготавливает их, загружает в чистый слой s3. Запускается каждый день в 01:00 по UTC и является триггером для `stg-data-loader-dag`

![get-data-dag](https://github.com/Leonidee/online-shop-vertica-dwh/blob/master/addons/get-data-dag.png?raw=true)

- `stg-data-loader-dag` - Заполняет STG слои и является триггером для `dds-data-loader-dag`

![dwh-creator-dag](https://github.com/Leonidee/online-shop-vertica-dwh/blob/master/addons/dwh-creator-dag.png?raw=true)

- `dds-data-loader-dag` - Финальный даг, заполняющий DDS слой хранилища

![dwh-creator-dag](https://github.com/Leonidee/online-shop-vertica-dwh/blob/master/addons/dwh-creator-dag.png?raw=true)
