/*

Data Cleaning in SQL Queries

*/

-- Selecting all  the data from the 'layoffs' table
SELECT * 
FROM layoffs;


------------------------------------------------------------------------------------------------------------------
-- STAGE TABLE
-- Creating a new stage table to maintain raw data integrity

CREATE TABLE layoffs_staging
LIKE layoffs; -- Creating 'layoffs_staging' table structure based on 'layoffs'

SELECT *
FROM layoffs_staging; -- Viewing data from the new staging table;

INSERT layoffs_staging
SELECT * 
FROM layoffs; -- Populating the staging table with data from 'layoffs'

------------------------------------------------------------------------------------------------------------------

-- REMOVING DUPLICATES

-- Identifying duplicate records
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,
 percentage_laid_off, 'date', stage,
 country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Creating a Common Table Expression (CTE) to identify and display duplicates
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,
 percentage_laid_off, 'date', stage,
 country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

-- Creating a new table to store cleaned data without duplicates
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Viewing data from the new table
SELECT * 
FROM layoffs_staging2;

-- Populating the new table with data from the staging table, assigning row numbers
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,
 percentage_laid_off, 'date', stage,
 country, funds_raised_millions) AS row_num
FROM layoffs_staging;
-- Removing duplicate records from the new table
DELETE 
FROM layoffs_staging2 
WHERE row_num > 1;

-- Checking for duplicates in the new table
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

------------------------------------------------------------------------------------------------------------------
-- STANDARDIZING

-- Viewing data from the standardized table
SELECT * 
FROM layoffs_staging2;

-- Standardizing company names by removing leading and trailing whitespaces
SELECT DISTINCT (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = (TRIM(company));

-- Standardizing industry names by categorizing 'Crypto'
SELECT DISTINCT (industry)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto' 
WHERE industry LIKE 'Crypto%';

-- Standardizing country names by removing trailing punctuation
UPDATE layoffs_staging2
SET country= TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Fixing date formatting issues
SELECT `date` FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=  STR_TO_DATE(`date`, '%m/%d/%Y') ;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

------------------------------------------------------------------------------------------------------------------
-- REMOVING BLANK AND NULL VALUES

-- Checking for rows with null values
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Removing blank industry values
UPDATE layoffs_staging2
SET industry= NULL
WHERE industry= '';

-- Checking for rows with null or empty industry values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

-- Checking specific company data to certify ('Airbnb')
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Updating null industry values based on matching company names
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company
    AND t1.location=t2.location
WHERE (t1.industry IS NULL OR t1.industry= '');

-- Updating null industry values with corresponding non-null values
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company
SET t1.industry= t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- Deleting rows with null total_laid_off and null percentage_laid_off values
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 

-- Dropping the 'row_num' column as it's no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Viewing final cleaned data
SELECT *
FROM layoffs_staging2;


