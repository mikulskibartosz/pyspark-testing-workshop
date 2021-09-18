# pyspark-testing-workshop

Dane testowe wygenerowane przez https://www.mockaroo.com/

Zadania

1. Ile produktów jest w zbiorze danych?
2. Ile produktów zostało sprzedanych w każdym z sklepów? 
3. Jaki jest najpopularniejszy produkt?
4. Jaka jest najpopularniejsza kategoria produktu?
5. Ile unikalnych produktów zostało sprzedanych każdego dnia?
6. Jaka jest średnia wartość pojedynczego zakupu? Załóż, że null w kolumnie `pieces_sold` oznacza 1
7. Ile średnio zarabia każdy ze sklepów każdego dnia? Załóż, że null w kolumnie `pieces_sold` oznacza 1
8. Który ze sklepów najczęściej pomija wpisywanie liczby produktów?
9. Którego dnia sprzedano najwięcej produktów (suma `pieces_sold` dla każdego dnia)?


Zadania walidacja danych

1. Sprawdź czy istnieją dane z brakującymi identyfikatorami sklepów.
2. Sprawdź czy istnieją zamówienia z liczbą produktów większą niż 15.
3. Sprawdź czy istneiją zamówienia w wybranej przez siebie kategorii.
4. Sprawdź czy wszystkie nazwy produktów są unikalne.
5. Sprawdź czy nazwy produktów nie są puste oraz nie dłuższe niż 20 znaków.
6. Sprawdź czy liczba unikalnych produktów jest większa niż 900.
7. Sprawdź czy istnieją zamówienia których cena pojedynczego przedmiotu jest większa niż: średnia cena przedmiotu + dwukrotność odchylenia standardowego
