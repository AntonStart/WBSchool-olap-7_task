# WBSchool-olap-7_task
Airflow + Clickhouse + Postgre

**Задание**

Установить локально Airflow, ClickHouse, Postgres. Не забыть про то, что им нужно общаться, решайте сами, кому как удобно - через ip хоста или через добавления ClickHouse и Postgres в сеть Airflow.
Сделать даг, берущий данные из вашего локального ClickHouse, как-то их трансформирующий (не обязательно, но желательно), а затем кладущий в витрину на том же клике (сделать схему reports). После этого даг должен взять данные из только что созданной витрины, преобразовать в датафрейм пандас, и заинсертить их в Postgres по методологии, рассказанной Львом на лекции по Postgres (процедура импорта).

В репозиторий гит выложить:

1. Даг
2. Скрин, что даг успешно отработал
3. Скрин с данными из витрины Postgres

#
**Решение**

1. [Ссылка на DAG](https://github.com/AntonStart/WBSchool-olap-7_task/blob/main/my_dag.py)
2. [Cкрин, что даг успешно отработал](https://github.com/AntonStart/WBSchool-olap-7_task/blob/main/%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%202024-07-30%20134331.png)
3. [Скрин с данными из витрины Postgre](https://github.com/AntonStart/WBSchool-olap-7_task/blob/main/%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%202024-07-30%20134711.png)
4. [Ссылка на код процедуры импорта](https://github.com/AntonStart/WBSchool-olap-7_task/blob/main/PG.sql)
