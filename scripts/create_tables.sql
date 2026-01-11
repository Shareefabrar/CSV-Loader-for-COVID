-- Create tables for COVID-19 data
CREATE TABLE IF NOT EXISTS countries (
    id SERIAL PRIMARY KEY,
    iso_code VARCHAR(10) UNIQUE NOT NULL,
    continent VARCHAR(50),
    location VARCHAR(100) NOT NULL,
    population BIGINT,
    population_density DECIMAL(10, 2),
    median_age DECIMAL(4, 1),
    aged_65_older DECIMAL(5, 2),
    aged_70_older DECIMAL(5, 2),
    gdp_per_capita DECIMAL(10, 2),
    extreme_poverty DECIMAL(5, 2),
    cardiovasc_death_rate DECIMAL(6, 2),
    diabetes_prevalence DECIMAL(5, 2),
    female_smokers DECIMAL(5, 2),
    male_smokers DECIMAL(5, 2),
    handwashing_facilities DECIMAL(5, 2),
    hospital_beds_per_thousand DECIMAL(5, 2),
    life_expectancy DECIMAL(4, 1),
    human_development_index DECIMAL(4, 3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS covid_stats (
    id SERIAL PRIMARY KEY,
    iso_code VARCHAR(10) REFERENCES countries(iso_code),
    date DATE NOT NULL,
    total_cases BIGINT,
    new_cases BIGINT,
    total_deaths BIGINT,
    new_deaths BIGINT,
    total_cases_per_million DECIMAL(10, 2),
    new_cases_per_million DECIMAL(10, 2),
    total_deaths_per_million DECIMAL(10, 2),
    new_deaths_per_million DECIMAL(10, 2),
    reproduction_rate DECIMAL(4, 2),
    icu_patients BIGINT,
    icu_patients_per_million DECIMAL(10, 2),
    hosp_patients BIGINT,
    hosp_patients_per_million DECIMAL(10, 2),
    weekly_icu_admissions BIGINT,
    weekly_icu_admissions_per_million DECIMAL(10, 2),
    weekly_hosp_admissions BIGINT,
    weekly_hosp_admissions_per_million DECIMAL(10, 2),
    total_tests BIGINT,
    new_tests BIGINT,
    total_tests_per_thousand DECIMAL(10, 2),
    new_tests_per_thousand DECIMAL(10, 2),
    positive_rate DECIMAL(5, 4),
    tests_per_case DECIMAL(10, 2),
    total_vaccinations BIGINT,
    people_vaccinated BIGINT,
    people_fully_vaccinated BIGINT,
    total_boosters BIGINT,
    new_vaccinations BIGINT,
    total_vaccinations_per_hundred DECIMAL(10, 2),
    people_vaccinated_per_hundred DECIMAL(10, 2),
    people_fully_vaccinated_per_hundred DECIMAL(10, 2),
    total_boosters_per_hundred DECIMAL(10, 2),
    new_vaccinations_smoothed BIGINT,
    new_vaccinations_smoothed_per_million DECIMAL(10, 2),
    stringency_index DECIMAL(5, 2),
    excess_mortality_cumulative_absolute DECIMAL(10, 2),
    excess_mortality_cumulative DECIMAL(10, 2),
    excess_mortality DECIMAL(10, 2),
    excess_mortality_cumulative_per_million DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_covid_stats_date ON covid_stats(date);
CREATE INDEX IF NOT EXISTS idx_covid_stats_iso_code ON covid_stats(iso_code);
CREATE INDEX IF NOT EXISTS idx_countries_location ON countries(location);

-- Create view for summary statistics
CREATE OR REPLACE VIEW covid_summary AS
SELECT 
    c.location,
    c.continent,
    c.population,
    cs.total_cases,
    cs.total_deaths,
    cs.total_vaccinations,
    ROUND((cs.total_cases::DECIMAL / c.population * 1000000), 2) as cases_per_million,
    ROUND((cs.total_deaths::DECIMAL / cs.total_cases * 100), 2) as death_rate_percentage,
    cs.date as last_updated
FROM countries c
LEFT JOIN covid_stats cs ON c.iso_code = cs.iso_code
WHERE cs.date = (SELECT MAX(date) FROM covid_stats WHERE iso_code = c.iso_code);