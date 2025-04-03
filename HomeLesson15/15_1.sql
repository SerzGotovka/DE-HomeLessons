Описание структуры базы данных "Студенты и курсы" для самостоятельной реализации
1. Таблица "Студенты" (students):
- Должна содержать уникальный идентификатор студента
- Поля для хранения имени и фамилии студента
- Поле для электронной почты
- Дата поступления
2. Таблица "Курсы" (courses):
- Уникальный идентификатор курса
- Название курса
- Имя преподавателя
- Количество кредитов - это показатель трудоемкости курса
3. Таблица "Регистрации" (registrations):
- Уникальный идентификатор записи
- Ссылка на студента
- Ссылка на курс
- Дата регистрации
- Поле для оценки
4*. Связи между таблицами:
- Один студент может записаться на несколько курсов
- На один курс может записаться несколько студентов
- Таблица регистраций связывает студентов и курсы (многие-ко-многим)

- Задание 1. Создайте таблицы
- Задание 2. Заполните таблицы данными, не менее 5 строчек
- Задание 3. Измените email студента с фамилией "<Имя>"
- Задание 4. Удалите регистрацию студента <Имя> на курс "Базы данных":

CREATE TABLE students(
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	first_name VARCHAR(100),
	last_name VARCHAR(100),
	email VARCHAR(100),
	date_receipt DATETIME,
	
);

CREATE TABLE courses(
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name_course VARCHAR(100),
	name_teacher VARCHAR(100),
	credit FLOAT
);


CREATE TABLE registrations(
	id INTEGER PRIMARY KEY AUTOINCREVENT,
	id_student INTEGER,
	id_course INTEGER,
	date_registration DATETIME DEFAULT CURRENT TIMESTAMP,
	
	FOREIGN KEY id_student REFERENCES students(id),
	FOREIGN KEY id_course REFERENCES courses(id)
);







