1) Extract ratings as float and count no of occurences of each number in reverse order of ratings?
Output:-
+------+-----------+
|rating|TotalRating|
+------+-----------+
|5.0   |13211      |
|4.5   |8551       |
|4.0   |26818      |
|3.5   |13136      |
|3.0   |20047      |
+------+-----------+

2) Find the sum of ratings and number of ratings of each movie (movieId, (totalRatings, count))
Output:-
+-------+-----------+-------------+
|movieId|Sum_Ratings|Count_Ratings|
+-------+-----------+-------------+
|99992  |3.0        |1            |
|99917  |9.5        |3            |
|99910  |6.5        |2            |
|999    |36.5       |12           |
|99853  |4.0        |1            |
+-------+-----------+-------------+

only showing top 5 rows

3) Given the totalRating and count (number of ratings), we can find the average rating by dividing totalRating by the count.
Output:-
+-------+-----------+
|movieId|Avg_Ratings|
+-------+-----------+
|99992  |3.0        |
|99917  |3.17       |
|99910  |3.25       |
|999    |3.04       |
|99853  |4.0        |
+-------+-----------+

4) From above movieIDs mapped to the average ratings. Sort the result by average ratings in descending order.
Output:-
+-------+-----------+
|movieId|Avg_Ratings|
+-------+-----------+
|467    |5.0        |
|139640 |5.0        |
|147330 |5.0        |
|160644 |5.0        |
|140627 |5.0        |
+-------+-----------+

5) From the movies file display 1st & 2nd column & split 2nd column value by ('|') and repeat on every row.
Hint: .flatMapValues(x => x.split('|))
Output:-
+----------------+---------+
|movieTitle      |genres   |
+----------------+---------+
|Toy Story (1995)|Adventure|
|Toy Story (1995)|Animation|
|Toy Story (1995)|Children |
|Toy Story (1995)|Comedy   |
|Toy Story (1995)|Fantasy  |
+----------------+---------+

6) From movies file split the data based of 2nd column and display only Action data.
Output:
+-----------------------+------+
|movieTitle             |genres|
+-----------------------+------+
|Heat (1995)            |Action|
|Sudden Death (1995)    |Action|
|GoldenEye (1995)       |Action|
|Cutthroat Island (1995)|Action|
|Money Train (1995)     |Action|
+-----------------------+------+

7) From movies file split the data based of 2nd column and display only Comedy data & sort on title.
Output:
+-----------------------------------------+------+
|movieTitle                               |genres|
+-----------------------------------------+------+
|À nous la liberté (Freedom for Us) (1931)|Comedy|
|¡Three Amigos! (1986)                    |Comedy|
|Zootopia (2016)                          |Comedy|
|Zoom (2015)                              |Comedy|
|Zoom (2006)                              |Comedy|
+-----------------------------------------+------+

8) Fetch only CLOSED orders & format date into mm/dd/yyyy format from orders data.
Output:-
+--------+----------+-----------------+------------+
|order_id|order_date|order_customer_id|order_status|
+--------+----------+-----------------+------------+
|1       |07-25-2013|11599            |CLOSED      |
|4       |07-25-2013|8827             |CLOSED      |
|12      |07-25-2013|1837             |CLOSED      |
|18      |07-25-2013|1205             |CLOSED      |
|24      |07-25-2013|11441            |CLOSED      |
+--------+----------+-----------------+------------+

9) Join Orders & Order_Items and display output of these two dataset?
Output:
+--------+-----------------+----------+-------------------+------------------------+---------------+
|order_id|order_customer_id|order_date|order_item_subtotal|order_item_product_price|order_status   |
+--------+-----------------+----------+-------------------+------------------------+---------------+
|1       |11599            |07-25-2013|299.98             |299.98                  |CLOSED         |
|2       |256              |07-25-2013|129.99             |129.99                  |PENDING_PAYMENT|
|2       |256              |07-25-2013|250.0              |50.0                    |PENDING_PAYMENT|
|2       |256              |07-25-2013|199.99             |199.99                  |PENDING_PAYMENT|
|4       |8827             |07-25-2013|199.92             |49.98                   |CLOSED         |
+--------+-----------------+----------+-------------------+------------------------+---------------+

10) Find the employee count & cost to company for each group consisting of dept, cadre, and state?
Output:
+---------+---------+-----+---------+---------------+
|dept     |cadre    |state|Emp_Count|Cost_To_Company|
+---------+---------+-----+---------+---------------+
|HR       |Manager  |IND  |1        |58000          |
|Sales    |Trainee  |UK   |1        |12000          |
|Sales    |Lead     |IND  |2        |64000          |
|Sales    |Lead     |AUS  |2        |64000          |
|Marketing|Associate|IND  |2        |36000          |
+---------+---------+-----+---------+---------------+
