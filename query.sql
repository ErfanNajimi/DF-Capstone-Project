CREATE TABLE IF NOT EXISTS competitions (
    id INT PRIMARY KEY,
    name TEXT,
    location TEXT,
    date DATE,
    discipline VARCHAR(8),
);

CREATE TABLE IF NOT EXISTS competitors (
    id INT PRIMARY KEY,
    first_name TEXT,
    second_name TEXT,
    sex TEXT,
    Nationality TEXT,
    Country TEXT,
    Year_Of_Birth INT,
);

CREATE TABLE IF NOT EXISTS couples (
    id INT PRIMARY KEY,
    name TEXT,
    country TEXT,
    male_id INT NOT NULL,
    female_id INT NOT NULL,
    FOREIGN KEY (male_id) REFERENCES competitors(id),
    FOREIGN KEY (female_id) REFERENCES competitors(id),
);

CREATE TABLE IF NOT EXISTS results (
    id INT PRIMARY KEY,
    couple_id INT NOT NULL, 
    rank INT,
    competition_id INT NOT NULL,
    details TEXT,
    FOREIGN KEY (couple_id) REFERENCES couples(id),
    FOREIGN KEY (competition_id) REFERENCES competitions(id)
);