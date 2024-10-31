SELECT *
FROM layoffs;

CREATE TABLE layoffs_new
LIKE layoffs;

INSERT layoffs_new
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_new;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_new;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_new
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;

SELECT *
FROM layoffs_new
WHERE company = 'Casper';


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_new
)
DELETE 
FROM duplicate_cte
WHERE row_num>1;

CREATE TABLE `layoffs_new_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_new_2;

INSERT INTO layoffs_new_2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_new;

SELECT *
FROM layoffs_new_2
WHERE row_num >1;

DELETE
FROM layoffs_new_2
WHERE row_num >1;

SELECT *
FROM layoffs_new_2
WHERE row_num >1;


SELECT company, TRIM(company)
FROM layoffs_new_2;

UPDATE layoffs_new_2
SET company = trim(company);

SELECT DISTINCT industry
FROM layoffs_new_2
ORDER BY 1;

SELECT industry
FROM layoffs_new_2
WHERE industry like 'crypto%';

UPDATE layoffs_new_2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT distinct industry
FROM layoffs_new_2;

SELECT DISTINCT country
FROM layoffs_new_2
ORDER BY 1; 

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_new_2
ORDER BY 1;

UPDATE layoffs_new_2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT *
FROM layoffs_new_2;

SELECT `date`, 
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_new_2;

UPDATE layoffs_new_2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_new_2
MODIFY `date` DATE;

SELECT *
FROM layoffs_new_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_new_2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_new_2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_new_2
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_new_2 t1
JOIN layoffs_new_2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry ='')
AND t2.industry is not null; 

UPDATE layoffs_new_2 t1
JOIN layoffs_new_2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
    

SELECT *
FROM layoffs_new_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_new_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_new_2;

ALTER TABLE layoffs_new_2
DROP COLUMN row_num;

