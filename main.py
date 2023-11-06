from sqlalchemy import func, desc, select, and_
from sqlalchemy.orm import joinedload, subqueryload

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.connect_db import session


# Запит 1.
# 5 студентів із найбільшим середнім балом з усіх предметів.
def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM
        students AS s
    JOIN
        grades AS g ON s.id = g.student_id
    GROUP BY
        s.id
    ORDER BY
        average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


# Запит 2.
# Студент із найвищим середнім балом з певного предмета.
def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM
        grades AS g
    JOIN
        students AS s ON s.id = g.student_id
    WHERE
        g.subject_id = 1
    GROUP BY
        s.id
    ORDER BY
        average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


# Запит 3.
# Середній бал у групах з певного предмета.
def select_03():
    """
    SELECT s.group_id, AVG(g.grade) AS average_grade
    FROM students s
    INNER JOIN grades g ON s.id = g.student_id
    INNER JOIN subjects sub ON g.subject_id = sub.id
    WHERE sub.name = 'fast' -- Замініть на назву потрібного предмета
    GROUP BY s.group_id;
    """
    # Замініть 'fast' на назву потрібного предмета
    subject_name = 'fast'

    result = session.query(Student.group_id, func.avg(Grade.grade).label('average_grade')) \
        .join(Grade) \
        .join(Subject) \
        .filter(Subject.name == subject_name) \
        .group_by(Student.group_id) \
        .all()
    return result


# Запит 4.
# Середній бал на потоці (по всій таблиці оцінок).
def select_04():
    """
    SELECT AVG(grade) AS average_grade
    FROM grades;
    """
    result = session.query(func.avg(Grade.grade).label('average_grade')).scalar()
    return result


# Запит 5.
# Які курси на потоці читає певний викладач.
def select_05():
    """
    SELECT sub.name AS course_name
    FROM subjects sub
    INNER JOIN teachers t ON sub.teacher_id = t.id
    WHERE t.fullname = 'Robert Lang' -- Замініть 'ПІБ викладача' на ім'я викладача, якого ви шукаєте
    """
    # Замініть 'Robert Lang' на ім'я викладача, якого ви шукаєте
    teacher_name = 'Robert Lang'

    result = session.query(Subject.name.label('course_name')) \
        .join(Teacher) \
        .filter(Teacher.fullname == teacher_name) \
        .all()
    return result


# Запит 6.
# Cписок студентів у певній групі.
def select_06():
    """
    SELECT fullname
    FROM students
    WHERE group_id = 3; -- Замініть на ідентифікатор групи, яку ви шукаєте
    """
    result = session.query(Student.fullname).filter(Student.group_id == 3).all()
    return result


# Запит 7.
# Оцінки студентів у окремій групі з певного предмета.
def select_07():
    """
    SELECT s.fullname AS student_name, g.grade, g.grade_date
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON g.subject_id = sub.id
    WHERE s.group_id = 2 -- Замініть на ідентифікатор групи, яку ви шукаєте
      AND sub.name = 'fast'; -- Замініть на назву потрібного предмета
    """
    result = session.query(Student.fullname.label('student_name'), Grade.grade, Grade.grade_date) \
        .join(Grade).join(Subject) \
        .filter(Student.group_id == 1, Subject.name == 'fast') \
        .all()
    return result


# Запит 8.
# Середній бал, який ставить певний викладач зі всiх своїх предметів.
def select_08():
    """
    SELECT t.fullname AS teacher_name, AVG(g.grade) AS average_grade
    FROM teachers t
    JOIN subjects sub ON t.id = sub.teacher_id
    JOIN grades g ON sub.id = g.subject_id
    GROUP BY t.fullname;
    """
    result = session.query(Teacher.fullname.label('teacher_name'), func.avg(Grade.grade).label('average_grade')) \
        .select_from(Teacher).join(Subject).join(Grade, Subject.id == Grade.subjects_id) \
        .group_by(Teacher.fullname) \
        .all()
    return result


# Запит 9.
# Cписок курсів, які відвідує студент
def select_09():
    """
    SELECT sub.name AS course_name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON g.subject_id = sub.id
    WHERE s.fullname = 'Sarah Williams' -- Замініть 'ПІБ студента' на ім'я студента, якого ви шукаєте
    """
    result = session.query(Subject.name.label('course_name')) \
        .select_from(Student) \
        .join(Grade, Student.id == Grade.student_id) \
        .join(Subject, Grade.subjects_id == Subject.id) \
        .filter(Student.fullname == 'Sarah Williams') \
        .all()
    return result


# Запит 10.
# Cписок курсів, які певному студенту читає певний викладач.
def select_10():
    """
    SELECT sub.name AS course_name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON g.subject_id = sub.id
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE s.fullname = 'Mark Edwards' -- Замініть 'ПІБ студента' на ім'я студента, якого ви шукаєте
      AND t.fullname = 'Regina Richard' -- Замініть 'ПІБ викладача' на ім'я викладача, якого ви шукаєте
    """
    result = session.query(Subject.name.label('course_name')) \
        .join(Grade, Subject.id == Grade.subjects_id) \
        .join(Student, Grade.student_id == Student.id) \
        .join(Teacher, Subject.teacher_id == Teacher.id) \
        .filter(Student.fullname == 'Mark Edwards', Teacher.fullname == 'Regina Richard') \
        .all()
    return result


# Запит 11.
# Середній бал, який певний викладач ставить певному студентові.
def select_11():
    """
    SELECT t.fullname AS teacher_name, s.fullname AS student_name, AVG(g.grade) AS average_grade
    FROM teachers t
    JOIN subjects sub ON t.id = sub.teacher_id
    JOIN grades g ON sub.id = g.subject_id
    JOIN students s ON g.student_id = s.id
    WHERE t.fullname = 'Heather Espinoza' -- Замініть на ім'я викладача, якого ви шукаєте
      AND s.fullname = 'Mary Park' -- Замініть на ім'я студента, якого ви шукаєте
    GROUP BY t.fullname, s.fullname;
    """
    result = session.query(
        Teacher.fullname.label('teacher_name'),
        Student.fullname.label('student_name'),
        func.avg(Grade.grade).label('average_grade')
    ).select_from(Teacher) \
        .join(Subject, Teacher.id == Subject.teacher_id) \
        .join(Grade, Subject.id == Grade.subjects_id) \
        .join(Student, Grade.student_id == Student.id) \
        .filter(Teacher.fullname == 'Heather Espinoza', Student.fullname == 'Mary Park') \
        .group_by(Teacher.fullname, Student.fullname) \
        .all()
    return result


# Запит 12.
# Оцінки студентів у певній групі з певного предмета на останньому занятті.
def select_12():
    """
   SELECT
       MAX(grade_date)
   FROM
       grades AS g
   JOIN
       students AS s ON s.id = g.student_id
   WHERE
       g.subject_id = 2 AND s.group_id = 3;


   SELECT
       s.id, s.fullname, g.grade, g.grade_date
   FROM
       grades AS g
   JOIN
       students AS s ON g.student_id = s.id
   WHERE
       g.subject_id = 2 AND s.group_id = 3 AND g.grade_date = (
           SELECT
               MAX(grade_date)
           FROM
               grades AS g
           JOIN
               students AS s ON s.id = g.student_id
           WHERE
               g.subject_id = 2 AND s.group_id = 3
           );
   """

    subquery = (select(func.max(Grade.grade_date)).join(Student)\
                .filter(and_(Grade.subjects_id == 2, Student.group_id == 3))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result


if __name__ == '__main__':
    # print(*select_01(), sep = "\n")
    # print(*select_02(), sep = "\n")
    # print(*select_03(), sep="\n")
    # print(select_04(), sep="\n")
    # print(*select_05(), sep = "\n")
    # print(*select_06(), sep = "\n")
    # print(*select_07(), sep = "\n")
    # print(*select_08(), sep = "\n")
    # print(*select_09(), sep = "\n")
    # print(*select_10(), sep="\n")
    # print(select_11(), sep = "\n")
    print(select_12(), sep="\n")
