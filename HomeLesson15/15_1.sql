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
- Задание 4. Удалите регистрацию студента <Имя> на курс <Имя курса>:


-- Задание 1. Создайте таблицы
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
	date_registration TEXT DEFAULT CURRENT TIMESTAMP,
	
	FOREIGN KEY id_student REFERENCES students(id),
	FOREIGN KEY id_course REFERENCES courses(id)
);


-- Задание 2. Заполните таблицы данными, не менее 5 строчек
INSERT INTO students (first_name, last_name, email, date_receipt)
VALUES 
('Иван', 'Иванов', 'ivan.ivanov@example.com', '2023-09-01'),
('Мария', 'Петрова', 'maria.petrova@example.com', '2023-09-01'),
('Алексей', 'Сидоров', 'alexey.sidorov@example.com', '2023-09-01'),
('Ольга', 'Кузнецова', 'olga.kuznetsova@example.com', '2023-09-01'),
('Дмитрий', 'Смирнов', 'dmitry.smirnov@example.com', '2023-09-01');

INSERT INTO courses (name_course, name_teacher, credit) VALUES
('Программирование на Python', 'Анна Васильева', 4.0),
('JAVA', 'Сергей Петров', 3.0),
('Алгоритмы и структуры данных', 'Елена Николаева', 5.0),
('Веб-разработка', 'Игорь Сидоров', 4.0),
('Машинное обучение', 'Мария Кузнецова', 5.0);

INSERT INTO registrations (id_student, id_course) VALUES
(1, 1),  
(1, 2), 
(2, 1), 
(3, 3),  
(4, 4), 
(5, 5); 


-- Задание 3. Измените email студента с фамилией "<Имя>"
UPDATE students
SET email='123345@google.com' WHERE last_name='Сидоров';


-- Задание 4. Удалите регистрацию студента <Имя> на курс <Имя курса>":
DELETE FROM registrations 
WHERE id_student = (select id from students where first_name = 'Иван')
and id_course = (select id from courses where name_course = 'Программирование на Python');





