.read data.sql


CREATE TABLE bluedog AS
  SELECT color, pet
         from students
         where color = "blue" and pet = "dog";

CREATE TABLE bluedog_songs AS
  SELECT color, pet, song
         from students
         where color = "blue" and pet = "dog";


CREATE TABLE smallest_int_having AS
  SELECT time, smallest 
         from students
         group by smallest
         having count(*) = 1;


CREATE TABLE matchmaker AS
  SELECT a.pet, a.song, a.color, b.color
         from students as a, students as b
         where a.pet = b.pet and a.song = b.song and a.time < b.time;


CREATE TABLE sevens AS
  SELECT a.seven
         from students as a, numbers as b
         where a.number = 7 and a.time = b.time and b."7" = "True";


CREATE TABLE average_prices AS
  SELECT category, avg(MSRP) as average_price
         from products
         group by category;


CREATE TABLE lowest_prices AS
  SELECT store, item, min(price)
         from inventory
         group by item;


CREATE TABLE shopping_list AS
  SELECT a.name, b.store 
         from products as a, lowest_prices as b
         where a.name = b.item
         group by a.category
         having min(a.MSRP / a.rating);

CREATE TABLE total_bandwidth AS
  SELECT sum(b.Mbs)
         from shopping_list as a, stores as b
         where a.store = b.store;

