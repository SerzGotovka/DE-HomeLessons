Задача для самостоятельной работы:
Вы разрабатываете систему управления онлайн-курсами.
В системе участвуют преподаватели, студенты, курсы, модули, уроки, домашние задания и оценки.

- --Преподаватель может вести несколько курсов.
- --Курс состоит из нескольких модулей
- Модуль содержит несколько уроков
- Урок может содержать домашнее задание
- --Студент может записаться на несколько курсов.
- Студент выполняет домашние задания.
- Преподаватель ставит оценку за каждое выполненное задание.
- --У каждого студента есть профиль , содержащий имя, email, дату регистрации.
- --Каждый курс имеет категорию (например, "Программирование", "Маркетинг").
- Курс может иметь отзывы от студентов.

1. Выделить все сущности и их атрибуты (указать существующие и придумать по 1 или более дополнительному по необходимости)
2. Найдите связи между сущностями. Укажите типы связей: один ко многим (1:M), один к одному (1:1), многие ко многим (M:N)
3. Постройте ER-диаграмму


CREATE SCHEMA courses;

CREATE TABLE courses.teachers(
	id_teacher SERIAL PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	email VARCHAR(100) NOT NULL UNIQUE,
	bio TEXT,
	date_registration DATE DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE courses.students(
	id_students SERIAL PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	email VARCHAR(100) NOT NULL UNIQUE,
	bio TEXT,
	date_registration DATE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE courses.category(
	id_category SERIAL PRIMARY KEY,
	name VARCHAR(100) NOT NULL
);

CREATE TABLE courses.modules(
	id_module SERIAL PRIMARY KEY,
	title VARCHAR(100) NOT NULL,
	description TEXT NOT NULL,
	course_id INT,
	
	FOREIGN KEY (course_id) REFERENCES courses.courses(id_course) ON DELETE CASCADE	
);


CREATE TABLE courses.courses(
	id_course SERIAL PRIMARY KEY,
	name VARCHAR(100) NOT NULL,	
	prict DECIMAL(10, 2),
	category_id INT
	
	FOREIGN KEY category_id REFERENCES category(id_category) ON DELETE CASCADE
);

CREATE TABLE courses.teachers_courses(
	id SERIAL,
	id_teacher INT,
    id_course INT,
    PRIMARY KEY (id, id_teacher, id_course),
    FOREIGN KEY (id_teacher) REFERENCES courses.teachers(id_teacher) ON DELETE CASCADE,
    FOREIGN KEY (id_course) REFERENCES courses.courses(id_course) ON DELETE CASCADE
);


CREATE TABLE courses.students_courses(
	id SERIAL,
	id_student INT,
    id_course INT,
    PRIMARY KEY (id, id_student, id_course),
    FOREIGN KEY (id_student) REFERENCES courses.student(id_student) ON DELETE CASCADE,
    FOREIGN KEY (id_course) REFERENCES courses.courses(id_course) ON DELETE CASCADE
);


CREATE TABLE courses.lessons(
	id_lesson SERIAL PRIMARY KEY,
	title VARCHAR(100) NOT NULL,
    description TEXT,
    module_id INT,
    FOREIGN KEY (module_id) REFERENCES modules(id_module)
	
);


CREATE TABLE completed_homework (
	id_completed_homework INT PRIMARY KEY,
    student_id INT,
    lesson_id INT,
    answer TEXT NOT NULL,
    submission_date DATETIME NOT NULL,
    FOREIGN KEY (student_id) REFERENCES student(id_student),
    FOREIGN KEY (lesson_id) REFERENCES homework(id_lesson)
);


CREATE TABLE grade(
	grade_id INT PRIMARY KEY,
    completed_homework_id INT,
    teacher_id INT,
    score INT NOT NULL,
    coment TEXT,
    FOREIGN KEY (completed_homework_id) REFERENCES completed_homework(id_completed_homework),
    FOREIGN KEY (teacher_id) REFERENCES teacher(id_teacher)
);


CREATE TABLE review (
    review_id INT PRIMARY KEY,
    student_id INT,
    course_id INT,
    texts TEXT NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    FOREIGN KEY (student_id) REFERENCES student(id_student),
    FOREIGN KEY (course_id) REFERENCES course(id_course)