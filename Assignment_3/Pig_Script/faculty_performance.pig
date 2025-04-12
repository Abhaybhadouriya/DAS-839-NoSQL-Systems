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

-- Skip header row (matches Hive column name)
student_course_data = FILTER student_course_data BY Student_ID != 'student_id';

-- Convert Avg_Attendance_Percent to float, handle NULL/empty
student_course_data = FOREACH student_course_data GENERATE
    Student_ID, Student_Name, Course, Program, Batch, Faculty_Name, Course_Type,
    Classes_Attended, Classes_Absent,
    (Avg_Attendance_Percent IS NOT NULL AND Avg_Attendance_Percent != '' ? (float)Avg_Attendance_Percent : NULL) AS Avg_Attendance_Percent,
    Enrollment_Date, Course_Credit, Obtained_Marks_Grade, Exam_Result, Period;

-- Filter records with non-NULL Exam_Result
filtered_data = FILTER student_course_data BY Exam_Result IS NOT NULL;

-- Group by Faculty_Name and Course
grouped_data = GROUP filtered_data BY (Faculty_Name, Course);

-- Process each group
result2 = FOREACH grouped_data {
    -- Filter passes within the group's bag
    pass_count = FILTER filtered_data BY Exam_Result == 'Pass';
    GENERATE 
        FLATTEN(group) AS (Faculty_Name, Course),
        COUNT(pass_count) AS Pass_Count,
        COUNT(filtered_data) AS Total_Students,
        (COUNT(pass_count) * 100.0 / COUNT(filtered_data)) AS Pass_Rate;
};

-- Order by Pass_Rate descending
ordered_result2 = ORDER result2 BY Pass_Rate DESC;
-- Output the result
--DUMP ordered_result2;
-- Store the result into a local directory
STORE ordered_result2 INTO '/home/abhay/Desktop/Semester_2/NoSQL/Assignment_3/Pig_Script/faculty_output' USING PigStorage(',');
