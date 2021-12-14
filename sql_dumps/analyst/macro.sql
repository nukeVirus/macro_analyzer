CREATE SCHEMA IF NOT EXISTS analyst;
CREATE TABLE IF NOT EXISTS analyst.tbl_macro (

    -- required_columns
    id SERIAL PRIMARY KEY
    , State char NOT NULL
    , "Data Element" char NOT NULL
	, "Value" int NOT NULL
	, Frequency char not null
	, "Date" timestamp NOT NULL
	, Region char NOT NULL
	, Description char NOT NULL

    -- maintenance and VC columns
    , date_created timestamp NOT NULL DEFAULT NOW()
    , created_by varchar(100) NOT NULL DEFAULT CURRENT_USER
    , date_updated timestamp NOT NULL DEFAULT NOW()
    , updated_by varchar(100) NOT NULL DEFAULT CURRENT_USER
);
COMMIT;