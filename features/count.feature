Feature: Spark

  Scenario: Count products in the dataset
    Given a dataset of purchases
    |order_id|product_name|pieces_sold|price_per_item|order_date|shop_id|
    |1       |product A   |1          |1             |2021-09-19|shop_A |
    |2       |product B   |1          |1             |2021-09-19|shop_A |
    |3       |product B   |1          |1             |2021-09-19|shop_A |
    When we count the products
    Then the number of products is "2"

  Scenario: Count products per shop
    Given a dataset of purchases
    |order_id|product_name|pieces_sold|price_per_item|order_date|shop_id|
    |1       |product A   |1          |1             |2021-09-19|shop_A |
    |2       |product B   |1          |1             |2021-09-19|shop_A |
    |3       |product B   |1          |1             |2021-09-19|shop_B |
    |4       |product C   |1          |1             |2021-09-19|shop_B |
    |5       |product A   |1          |1             |2021-09-19|shop_B |
    When we count the products sold per shop
    Then the result dataset should equal
    |shop_id - str|count - long|
    |shop_A |2    |
    |shop_B |3    |

  Scenario: Find the most popular product
    Given a dataset of purchases
    |order_id|product_name|pieces_sold|price_per_item|order_date|shop_id|
    |1       |product A   |1          |1             |2021-09-19|shop_A |
    |2       |product B   |1          |1             |2021-09-19|shop_A |
    |3       |product B   |1          |1             |2021-09-19|shop_B |
    |4       |product C   |1          |1             |2021-09-19|shop_B |
    |5       |product A   |1          |1             |2021-09-19|shop_B |
    |6       |product A   |1          |1             |2021-09-19|shop_B |
    When we look for the most popular product
    Then the product name should be "product A"

  Scenario: Find the most popular category
    Given a dataset of purchases
    |order_id|product_name|pieces_sold|price_per_item|order_date|shop_id|
    |1       |Coffee - Frthy Coffee Crisp   |1          |1             |2021-09-19|shop_A |
    |2       |Tea - Banana Green Tea   |1          |1             |2021-09-19|shop_A |
    |3       |Tea - Apple Green Tea   |1          |1             |2021-09-19|shop_B |
    |4       |Tea - Apple Green Tea   |1          |1             |2021-09-19|shop_B |
    |5       |Bar Bran Honey Nut   |1          |1             |2021-09-19|shop_B |
    When we look for the most popular category
    Then the category name should be "Tea"

  Scenario: Find how many unique product were sold on every day
    Given a dataset of purchases
    |order_id|product_name|pieces_sold|price_per_item|order_date|shop_id|
    |1       |product A   |1          |1             |2021-09-19|shop_A |
    |2       |product B   |1          |1             |2021-09-19|shop_A |
    |3       |product B   |1          |1             |2021-09-19|shop_B |
    |4       |product C   |1          |1             |2021-09-20|shop_B |
    |5       |product A   |1          |1             |2021-09-20|shop_B |
    When we look for the number of unique products sold per day
    Then the result dataset should equal
    |order_date - date|count - long|
    |2021-09-19 |2    |
    |2021-09-20 |2    |

  Scenario: Find the average purchase price
    Given a dataset of purchases
    |order_id|product_name|pieces_sold|price_per_item|order_date|shop_id|
    |1       |product A   |3          |5             |2021-09-19|shop_A |
    |2       |product A   |           |5             |2021-09-19|shop_A |
    |3       |product B   |           |10            |2021-09-19|shop_B |
    When we look for the average purchase price
    Then the average price should be "10"

  Scenario: Find the average income per shop per day
    Given a dataset of purchases
    |order_id|product_name|pieces_sold|price_per_item|order_date|shop_id|
    |1       |product A   |5          |10            |2021-09-19|shop_A |
    |2       |product B   |           |10            |2021-09-19|shop_A |
    |3       |product B   |           |30            |2021-09-19|shop_B |
    |4       |product C   |3          |10            |2021-09-19|shop_B |
    |5       |product A   |1          |2             |2021-09-20|shop_B |
    When we look for the average income per shop per day
    Then the result dataset should equal
    |shop_id - str|avg - double|
    |shop_A       |60.0        |
    |shop_B       |31.0        |

  Scenario: Find the shop that does not write the number of products
    Given a dataset of purchases
    |order_id|product_name|pieces_sold|price_per_item|order_date|shop_id|
    |1       |product A   |           |10            |2021-09-19|shop_A |
    |2       |product B   |           |10            |2021-09-19|shop_A |
    |3       |product B   |           |30            |2021-09-19|shop_B |
    |4       |product C   |3          |10            |2021-09-19|shop_B |
    |5       |product A   |1          |2             |2021-09-20|shop_B |
    When we look for the shop that does not include the number of products
    Then the shop name should be "shop_A"

  Scenario: Find the day when the largest number of products was sold
    Given a dataset of purchases
    |order_id|product_name|pieces_sold|price_per_item|order_date|shop_id|
    |1       |product A   |           |10            |2021-09-19|shop_A |
    |2       |product B   |           |10            |2021-09-19|shop_A |
    |3       |product B   |           |30            |2021-09-19|shop_B |
    |4       |product C   |1          |10            |2021-09-20|shop_B |
    |5       |product A   |1          |2             |2021-09-20|shop_B |
    When we look for the day when the largest number of products was sold
    Then the date should be "2021-09-19"
