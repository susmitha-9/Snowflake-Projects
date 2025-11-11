 
CREATE OR REPLACE TABLE JSON_DB.PUBLIC.JSON_TABLE( 
DATA VARIANT  
); 
-- json flattening without hierarchy  
 
INSERT INTO JSON_TABLE(DATA) 
SELECT(  
PARSE_JSON (' 
 {"NAME": "SUSH", "AGE": 24, "CITY": "CLT" } 
') 
); 
 
SELECT * FROM JSON_TABLE; 
 
SELECT DATA:NAME::STRING AS NAME, DATA:CITY::STRING AS CITY, DATA:AGE::INTEGER AS AGE FROM JSON_TABLE; 
 
CREATE OR REPLACE TABLE JSON_DB.PUBLIC.JSON_TABLE_FLATTEN AS 
SELECT DATA:NAME::STRING AS NAME, DATA:CITY::STRING AS CITY, DATA:AGE::INTEGER AS AGE FROM JSON_TABLE; 
 
SELECT * FROM JSON_TABLE_FLATTEN; 
 
-- json flattening with hierarchy  
 
CREATE OR REPLACE TABLE JSON_DB.PUBLIC.JSON_TABLE1( 
DATA VARIANT 
); 
 
INSERT INTO JSON_TABLE1(DATA) 
SELECT PARSE_JSON( 
' {"NAME": "SUSH", "AGE": 24, "IS_STUDENT": TRUE,  
"ADDRESS":  { 
"STREET": "9544 UTD", 
"CITY": "CLT", 
"ZIP": "28262" 
 } }' 
); 
 
SELECT * FROM JSON_TABLE1; 
 
// DELETE FROM JSON_TABLE1 WHERE DATA:HOBBIES IS NOT NULL; 
 
SELECT DATA:NAME::STRING AS NAME, DATA:AGE::INTEGER AS AGE, DATA:IS_STUDENT::BOOLEAN AS IS_STUDENT, 
DATA:ADDRESS:STREET::STRING AS STREET, DATA:ADDRESS:CITY::STRING AS CITY, DATA:ADDRESS:ZIP::STRING AS ZIPCODE 
FROM JSON_TABLE1; 
 
CREATE OR REPLACE TABLE JSON_DB.PUBLIC.JSON_TABLE_FLATTEN1 AS 
SELECT DATA:NAME::STRING AS NAME, DATA:AGE::INTEGER AS AGE, DATA:IS_STUDENT::BOOLEAN AS IS_STUDENT, 
DATA:ADDRESS:STREET::STRING AS STREET, DATA:ADDRESS:CITY::STRING AS CITY, DATA:ADDRESS:ZIP::STRING AS ZIPCODE 
FROM JSON_TABLE1; 
 
SELECT * FROM JSON_TABLE_FLATTEN1; 
 
-- json flattening without hierarchy - with array 
 
CREATE OR REPLACE TABLE JSON_DB.PUBLIC.JSON_TABLE2( 
DATA VARIANT 
); 
 
INSERT INTO JSON_TABLE2(DATA) 
SELECT PARSE_JSON( 
' {"NAME": "SUSH", "AGE": 24, "IS_STUDENT": FALSE,  
"HOBBIES": [ 
"READING", 
"GARDENING", 
"COOKING" 
] }' 
); 
 
SELECT * FROM JSON_TABLE2; 
 
SELECT DATA:NAME::STRING AS NAME, DATA:AGE::INTEGER AS AGE, DATA:IS_STUDENT::BOOLEAN AS IS_STUDENT, 
-- HOBBY.* 
HOBBY.VALUE::STRING AS HOBBY 
FROM JSON_TABLE2, 
LATERAL FLATTEN (INPUT => DATA:HOBBIES) AS HOBBY; 
 
-- json flattening with hierarchy - with array 
 
CREATE OR REPLACE TABLE JSON_TABLE3( 
DATA VARIANT 
); 
 
INSERT INTO JSON_TABLE3(DATA) 
SELECT PARSE_JSON(' 
 { 
"STUDENT":  
 { 
"ID": 1, 
"INFO": 
 { 
"NAME": "SUSH", 
"AGE": 24 
 }, 
"COURSES":  
[ 
 { 
"COURSE_NAME": "MATHS", 
"DETAILS": 
 { 
"GRADE": "A", 
"CREDITS": 3 
 } 
 }, 
 { 
"COURSE_NAME": "ENGLISH", 
"DETAILS": 
 { 
"GRADE": "A", 
"CREDITS": 3 
 } 
 } 
] 
 } 
 } 
'); 
 
SELECT * FROM JSON_TABLE3; 
 
SELECT DATA:STUDENT:ID::INTEGER AS ID, DATA:STUDENT:INFO:NAME::STRING AS NAME,  
DATA:STUDENT:INFO:AGE::INTEGER AS AGE, 
COURSES.VALUE:COURSE_NAME::STRING AS COURSE_NAME, 
COURSES.VALUE:DETAILS:GRADE::STRING AS COURSE_GRADE, 
COURSES.VALUE:DETAILS:CREDITS::INTEGER AS COURSE_CREDITS 
FROM JSON_TABLE3, 
LATERAL FLATTEN (INPUT => DATA:STUDENT:COURSES) AS COURSES 
 
-- json flattening with hierarchy - with multiple array 
 
CREATE OR REPLACE TABLE JSON_TABLE4( 
DATA VARIANT 
); 
 
 
INSERT INTO JSON_TABLE4(DATA) 
SELECT PARSE_JSON (' 
 { 
  "company":  { 
    "department":  { 
      "head": "Jane Smith", 
      "name": "Engineering" 
     }, 
    "id": 201, 
    "teams": [ 
       { 
        "members": [ 
           { 
            "name": "Alice Brown", 
            "role": "UI Developer", 
            "skills": [ 
               { 
                "experience": 10, 
                "technology": [ 
                  "HTML", 
                  "CSS", 
                  "JavaScript", 
                  "React" 
                ] 
               } 
            ] 
           }, 
           { 
            "name": "Bob Green", 
            "role": "UX Designer", 
            "skills": [ 
               { 
                "experience": 7, 
                "technology": [ 
                  "Figma", 
                  "Sketch", 
                  "Adobe XD", 
                  "Illustrator" 
                ] 
               } 
            ] 
           } 
        ], 
        "team_name": "Frontend" 
       }, 
       { 
        "members": [ 
           { 
            "name": "Charlie White", 
            "role": "Backend Engineer", 
            "skills": [ 
               { 
                "experience": 15, 
                "technology": [ 
                  "Python", 
                  "Django", 
                  "PostgreSQL", 
                  "Redis" 
                ] 
               } 
            ] 
           }, 
           { 
            "name": "David Black", 
            "role": "DevOps Engineer", 
            "skills": [ 
               { 
                "experience": 10, 
                "technology": [ 
                  "AWS", 
                  "Docker", 
                  "Kubernetes", 
                  "Terraform" 
                ] 
               } 
            ] 
           } 
        ], 
        "team_name": "Backend" 
       } 
    ] 
   } 
 } 
') 
 
SELECT * FROM JSON_TABLE4; 
 
select data:company:id::integer as company_id, 
data:company:department:name::string as department_name, 
data:company:department:head::string as department_head, 
teams.value:team_name::string as team_name, 
members.value:name::string as member_name, 
members.value:role::string as member_role, 
skills.value:experience::integer as experience, 
technology.value::string as technology 
from json_table4, 
lateral flatten(input => data:company:teams) as teams, 
lateral flatten(input=> teams.value:members) as members, 
lateral flatten(input=> members.value:skills) as skills, 
lateral flatten(input=> skills.value:technology) as technology; 
 
-- array containing multiple json flattening 
 
CREATE OR REPLACE TABLE JSON_TABLE5( 
DATA VARIANT 
); 
 
 
INSERT INTO JSON_TABLE5(DATA) 
SELECT PARSE_JSON (' 
[ 
   { 
    "id": 1, 
    "name": "Alice Johnson", 
    "position": "Software Engineer", 
    "department": "IT", 
    "salary": 75000 
   }, 
   { 
    "id": 2, 
    "name": "Bob Smith", 
    "position": "Product Manager", 
    "department": "Product", 
    "salary": 85000 
   }, 
   { 
    "id": 3, 
    "name": "Carol Lee", 
    "position": "UX Designer", 
    "department": "Design", 
    "salary": 70000 
   } 
] 
') 
 
SELECT * FROM JSON_TABLE5 
 
select dt.value:id::integer as id, 
dt.value:id::string as name, 
dt.value:name::string as name, 
dt.value:department::string as department, 
dt.value:salary::integer as salary 
from json_table5, 
lateral flatten(input=> data) as dt; 
 
-- json containing multiple json at a key flattening 
 
CREATE OR REPLACE TABLE JSON_TABLE6( 
DATA VARIANT 
); 
 
 
INSERT INTO JSON_TABLE6(DATA) 
SELECT PARSE_JSON (' 
 { 
  "university":  { 
    "Faculty of Engineering":  { 
      "dean": "Dr. Emily Carter", 
      "departments":  { 
        "Computer Science":  { 
          "head": "Dr. Alan Turing", 
          "courses":  { 
            "Algorithms":  { 
              "credits": 4, 
              "enrolled_students": 120 
             }, 
            "Machine Learning":  { 
              "credits": 3, 
              "enrolled_students": 90 
             } 
           } 
         }, 
        "Mechanical Engineering":  { 
          "head": "Dr. Grace Hopper", 
          "courses":  { 
            "Thermodynamics":  { 
              "credits": 4, 
              "enrolled_students": 80 
             }, 
            "Fluid Mechanics":  { 
              "credits": 3, 
              "enrolled_students": 75 
             } 
           } 
         } 
       }, 
      "total_students": 1000, 
      "total_courses": 4 
     }, 
    "Faculty of Science":  { 
      "dean": "Dr. Carl Sagan", 
      "departments":  { 
        "Physics":  { 
          "head": "Dr. Richard Feynman", 
          "courses":  { 
            "Quantum Mechanics":  { 
              "credits": 4, 
              "enrolled_students": 70 
             }, 
            "Classical Mechanics":  { 
              "credits": 4, 
              "enrolled_students": 80 
             } 
           } 
         }, 
        "Biology":  { 
          "head": "Dr. Jane Goodall", 
          "courses":  { 
            "Genetics":  { 
              "credits": 3, 
              "enrolled_students": 100 
             }, 
            "Ecology":  { 
              "credits": 3, 
              "enrolled_students": 90 
             } 
           } 
         } 
       }, 
      "total_students": 700, 
      "total_courses": 4 
     } 
 
   }, 
  "name": "Central University", 
  "total_students": 1700, 
  "total_courses": 8 
 } 
') 
 
 
select data:name::string as univ_name, 
data:total_courses::integer as univ_total_courses, 
data:total_students::integer as univ_total_students, 
university.key::string as univ_faculty, 
university.value:dean::string as univ_faculty_dean_name, 
university.value:total_courses::integer as univ_faculty_total_courses, 
university.value:total_students::integer as univ_faculty_total_students, 
departments.key::string as univ_faculty_dept_name, 
departments.value:head::string as univ_faculty_dept_head, 
courses.key::string as univ_faculty_dept_course, 
courses.value:credits::integer as univ_faculty_dept_course_credits, 
courses.value:enrolled_students::integer as univ_faculty_dept_course_students, 
from JSON_TABLE6, 
lateral flatten(input=>data:university)as university, 
lateral flatten(input=>university.value:departments)as departments, 
lateral flatten(input=>departments.value:courses)as courses 
}
