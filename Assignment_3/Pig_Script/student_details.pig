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

-- Filter records with non-NULL Course, Student_ID, and Program
filtered_data = FILTER student_course_data BY 
    Course IS NOT NULL AND 
    Student_ID IS NOT NULL AND 
    Program IS NOT NULL;

-- Select fields with defaults for NULLs using conditional expressions
result3 = FOREACH filtered_data GENERATE
    (Course IS NOT NULL ? Course : 'Unknown') AS Course,
    (Student_ID IS NOT NULL ? Student_ID : 'Unknown') AS Student_ID,
    (Faculty_Name IS NOT NULL ? Faculty_Name : 'N/A') AS Faculty_Name,
    (Program IS NOT NULL ? Program : 'Not Provided') AS Program,
    (Batch IS NOT NULL ? Batch : 'Unassigned') AS Batch;

-- Limit to 10 rows
limited_result3 = LIMIT result3 10;

-- Store the result into a local directory
STORE limited_result3 INTO '/home/abhay/Desktop/Semester_2/NoSQL/Assignment_3/Pig_Script/details_output' USING PigStorage(',');
