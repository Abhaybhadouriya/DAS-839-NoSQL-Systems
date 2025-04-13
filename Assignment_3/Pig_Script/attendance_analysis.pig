-- Load the CSV, matching the Hive schema (15 columns)
student_course_data = LOAD '/home/abhay/Desktop/Semester_2/NoSQL/Assignment_3/Pig_Script/student_data6.csv' 
    USING PigStorage(',') 
    AS (
        Student_ID:chararray, 
        Student_Name:chararray, 
        Course:chararray, 
        Program:chararray,
        Batch:chararray, 
        Faculty_Name:chararray, 
        Course_Type:chararray,
        Classes_Attended:int, 
        Classes_Absent:int, 
        Avg_Attendance_Percent:chararray, -- Load as chararray to handle NULL/empty
        Enrollment_Date:chararray, 
        Course_Credit:float, 
        Obtained_Marks_Grade:chararray,
        Exam_Result:chararray,
        Period:chararray  -- Include period as per schema
    );

-- Skip header row (adjust if header differs)
student_course_data = FILTER student_course_data BY Student_ID != 'student_id';

-- Convert Avg_Attendance_Percent to float, handle NULL/empty
student_course_data = FOREACH student_course_data GENERATE
    Student_ID, Student_Name, Course, Program, Batch, Faculty_Name, Course_Type,
    Classes_Attended, Classes_Absent,
    (Avg_Attendance_Percent IS NOT NULL AND Avg_Attendance_Percent != '' ? (float)Avg_Attendance_Percent : NULL) AS Avg_Attendance_Percent,
    Enrollment_Date, Course_Credit, Obtained_Marks_Grade, Exam_Result, Period;

-- Group by Program and Course
grouped_data = GROUP student_course_data BY (Program, Course);

-- Process each group
result1 = FOREACH grouped_data {
    -- Filter non-NULL Avg_Attendance_Percent within the group's bag
    filtered_students = FILTER student_course_data BY Avg_Attendance_Percent IS NOT NULL;
    -- Get distinct Student_IDs within the group
    distinct_students = DISTINCT filtered_students.Student_ID;
    GENERATE 
        FLATTEN(group) AS (Program, Course),
        AVG(filtered_students.Avg_Attendance_Percent) AS Avg_Attendance,
        COUNT(distinct_students) AS Student_Count;
};

-- Filter groups with Student_Count > 0 (debugging, revert to > 5 later)
filtered_result1 = FILTER result1 BY Student_Count > 0;

-- Order by Avg_Attendance descending
ordered_result1 = ORDER filtered_result1 BY Avg_Attendance DESC;

-- Output the result
-- DUMP ordered_result1;
STORE ordered_result1 INTO '/home/abhay/Desktop/Semester_2/NoSQL/Assignment_3/Pig_Script/attendance_output' USING PigStorage(',');
