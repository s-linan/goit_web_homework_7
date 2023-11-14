from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    SELECT
        g.subject_id,
        s.group_id,
        ROUND(AVG(g.grade), 2) as average_grade
    FROM grades g
    JOIN students s ON g.student_id = s.id
    WHERE g.subject_id = 1  -- ID предмета
    GROUP BY g.subject_id, s.group_id;
    """
    result = (session.query(Grade.subjects_id, Student.group_id, func.round(func.avg(Grade.grade), 2) \
                            .label('average_grade')).join(Student, Grade.student_id == Student.id) \
              .filter(Grade.subjects_id == 1).group_by(Grade.subjects_id, Student.group_id).all())
    return result


def select_04():
    """
    SELECT
        ROUND(AVG(grade), 2) as average_grade
        FROM grades;
    :return:
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')).first()
    return result


def select_05():
    """
    SELECT
        t.fullname AS teacher_name,
        s.name AS subject_name
    FROM teachers t
    JOIN subjects s ON t.id = s.teacher_id
    WHERE t.id = 1; -- ID викладача
    """
    result = session.query(Teacher.fullname.label('teacher_name'), Subject.name.label('subject_name')) \
        .select_from(Teacher).join(Subject).filter(Teacher.id == 1).all()
    return result


def select_06():
    """
    SELECT
        fullname
    FROM students
    WHERE group_id = 1; -- ID групи
    :return:
    """
    result = session.query(Student.fullname).filter(Student.group_id == 1).all()
    return result


def select_07():
    """
    SELECT
        s.fullname AS student_name,
        g.grade,
        g.grade_date
    FROM students s
    JOIN grades g ON s.id = g.student_id
    WHERE s.group_id = 1 AND g.subject_id = 1; -- ID групи та ID предмета
    :return:
    """
    result = (session.query(Student.fullname.label('student_name'), Grade.grade, Grade.grade_date) \
              .select_from(Student).join(Grade, Student.id == Grade.student_id) \
              .filter(Student.group_id == 1, Grade.subjects_id == 1).all())
    return result


def select_08():
    """
    SELECT
        t.fullname AS teacher_name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN subjects s ON g.subject_id = s.id
    JOIN teachers t ON s.teacher_id = t.id
    WHERE t.id = 1  -- ID викладача
    GROUP BY t.fullname;
    :return:
    """
    result = (session.query(Teacher.fullname.label('teacher_name'),
                            func.round(func.avg(Grade.grade), 2).label('average_grade')) \
              .select_from(Grade).join(Subject, Grade.subjects_id == Subject.id) \
              .join(Teacher, Subject.teacher_id == Teacher.id).filter(Teacher.id == 1).group_by(Teacher.fullname).all())
    return result


def select_09():
    """
    SELECT
        st.fullname AS student_name,
        s.name AS subject_name
    FROM students st
    JOIN grades g ON st.id = g.student_id
    JOIN subjects s ON g.subject_id = s.id
    WHERE st.id = 1; -- ID студента
    :return:
    """
    result = (session.query(Student.fullname.label('student_name'), Subject.name.label('subject_name')) \
              .select_from(Student).join(Grade, Student.id == Grade.student_id) \
              .join(Subject, Grade.subjects_id == Subject.id).filter(Student.id == 1).all())
    return result


def select_10():
    """
    SELECT
        st.fullname AS student_name,
        s.name AS subject_name,
        t.fullname AS teacher_name
    FROM subjects s
    JOIN teachers t ON s.teacher_id = t.id
    JOIN grades g ON s.id = g.subject_id
    JOIN students st ON g.student_id = st.id
    WHERE s.teacher_id = 1 AND g.student_id = 1  -- ID вчителя та ID студента
    :return:
    """
    result = (session.query(Student.fullname.label('student_name'), Subject.name.label('subject_name'),
                            Teacher.fullname.label('teacher_name')) \
              .select_from(Subject).join(Teacher, Subject.teacher_id == Teacher.id) \
              .join(Grade, Subject.id == Grade.subjects_id).join(Student, Grade.student_id == Student.id) \
              .filter(Subject.teacher_id == 1, Grade.student_id == 1).all())
    return result


def select_11():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result