select premiered, printf("%s (%s)", primary_title, original_title) 
from titles 
where primary_title != original_title 
order by premiered desc 
limit 10;