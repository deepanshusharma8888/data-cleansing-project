-- data cleansing

-- use world_layoffs;
select * from layoffs;

-- remove duplicates
-- standardise the data
-- null values
-- remove the column

create table layoff_stagging
like layoffs;

insert layoff_stagging
select * from layoffs;

select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off, 'date',country,funds_raised_millions) as row_no
from layoff_stagging;
 
 with duplicate_cte as 
 (
 select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off, 'date',stage,country,funds_raised_millions) as row_no
from layoff_stagging
 )
 select * from duplicate_cte
 where row_no > 1;
 
 select * from layoff_stagging
 where company = 'oda';

with duplicate_cte as 
 (
 select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off, 'date',stage,country,funds_raised_millions) as row_no
from layoff_stagging
 )
 delete from duplicate_cte
 where row_no > 1;
 

CREATE TABLE `layoff_stagging2` (
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

insert into layoff_stagging2
 select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off, 'date',stage,country,funds_raised_millions) as row_no
from layoff_stagging;
select * from layoff_stagging2;

set sql_safe_updates = 0;
delete from layoff_stagging2
where row_num  > 1;

-- standardizing data
select company , trim(company) from layoff_stagging2;
update layoff_stagging
set company = trim(company);

select distinct industry
from layoff_stagging2 
;

update layoff_stagging
set industry = 'crypto'
where industry like 'crypto%';

select distinct country
from layoff_stagging2 
order by 1;

select distinct country , trim(trailing '.' from country )
from layoff_stagging2 
order by 1;

update layoff_stagging2
set country = trim(trailing '.' from country );
 
 
 
 select `date`,
 STR_TO_DATE(`date` , '%m /%d/ %Y' )
 from layoff_stagging2;
 
 update layoff_stagging2
 set `date` = STR_TO_DATE(`date` , '%m /%d/ %Y' );
 
 select * from layoff_stagging2;
 
 alter table layoff_stagging2
 modify column `date` date;
 
  select * from layoff_stagging2
  where  industry is null ;
  
  select * from layoff_stagging2
  where total_laid_off is null
   and percentage_laid_off is null;
  
  delete from layoff_stagging2
  where total_laid_off is null
   and percentage_laid_off is null;
  
  alter table layoff_stagging2
  drop column row_num;
  
  
  
  
  
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 



