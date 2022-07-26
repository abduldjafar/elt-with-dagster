{{ config(order_by='iso_code', engine='MergeTree()', materialized='table') }}

SELECT 
  iso_code, 
  continent, 
  location, 
  parseDateTimeBestEffortOrNull(`date`) as reported_date, 
  toDecimal32(total_cases, 1) as total_cases, 
  if(
    new_cases = '', 
    0, 
    toDecimal32(new_cases, 1)
  ) as new_cases, 
  if(
    total_deaths = '', 
    0, 
    toDecimal32(total_deaths, 1)
  ) as total_deaths, 
  if(
    new_deaths = '', 
    0, 
    toDecimal32(new_deaths, 1)
  ) as new_deaths, 
  if(
    people_vaccinated = '', 
    0, 
    toDecimal32(people_vaccinated, 1)
  ) as people_vaccinated, 
  if(
    people_fully_vaccinated = '', 
    0, 
    toDecimal32(people_fully_vaccinated, 1)
  ) as people_fully_vaccinated, 
  if(
    trimBoth(population)= '', 
    0, 
    toDecimal32(population, 1)
  ) as population 
from 
  covid_datasets 
where 
  location not ilike '%Asia' 
  and location != 'Europe' 
  and location not ILIKE '%America' 
  and continent != '' 
order by 
  reported_date asc
