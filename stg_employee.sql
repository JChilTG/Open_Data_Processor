select *
from (
    values
        (101, 'M', 'Male'),
        (102, 'F', 'Female'),
        (103, 'X', 'Non-binary'),
        (104, 'Z', 'Unknown'),
        (105, ' m ', 'Male'),
        (106, 'Non-Binary', 'Non-Binary'),
        (107, 'MALE', 'MALE'),
        (108, null, null)
) as t(employee_id, gender_code, raw_gender)
