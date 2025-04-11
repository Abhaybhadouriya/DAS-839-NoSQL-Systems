-- student_data.pig (Query 1 equivalent)
student_course_data = LOAD '/home/abhay/pig_data/student_data.csv' USING PigStorage(',') AS (
    Student_ID:chararray, Student_Name:chararray, Course:chararray, Program:chararray,
    Batch:chararray, Period:chararray, Faculty_Name:chararray, Course_Type:chararray,
    Classes_Attended:int, Classes_Absent:int, Avg_Attendance_Percent:float,
    Enrollment_Date:chararray, Course_Credit:float, Obtained_Marks_Grade:chararray,
    Exam_Result:chararray
);

grouped_data = GROUP student_course_data BY (Program, Course);
result1 = FOREACH grouped_data {
    filtered_students = FILTER student_course_data BY Avg_Attendance_Percent IS NOT NULL;
    distinct_students = DISTINCT filtered_students.Student_ID;
    GENERATE FLATTEN(group) AS (Program, Course),
             AVG(filtered_students.Avg_Attendance_Percent) AS Avg_Attendance,
             COUNT(distinct_students) AS Student_Count;
};
filtered_result1 = FILTER result1 BY Student_Count > 5;
ordered_result1 = ORDER filtered_result1 BY Avg_Attendance DESC;
DUMP ordered_result1;
