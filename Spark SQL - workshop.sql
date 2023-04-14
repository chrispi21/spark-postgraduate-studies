-- Databricks notebook source
-- MAGIC %md # Zapytania dot. metadanych 
-- MAGIC W pracy z danymi często zachodzi potrzeba, aby sprawdzić w jaki sposób przechowywane są dane (typy kolumn w tabeli, partycje, format plików, dostępne bazy danych). Aby ułatwić nam to zadanie zapoznamy się z odpowiednimi poleceniami.

-- COMMAND ----------

-- MAGIC %md Polecenia dot. baz:
-- MAGIC 
-- MAGIC 1. `show databases` - wyświetla dostępne bazy
-- MAGIC 2. `describe database` nazwa_bazy - opis bazy o nazwie nazwa_bazy
-- MAGIC 3. `create database` nazwa_bazu - tworzenie nawej bazy o nazwie nazwa_bazy
-- MAGIC 4. `drop database` nazwa_bazy - usuwanie bazy o nazwie nazwa_bazy
-- MAGIC 
-- MAGIC W następnych komórkach można samodzielnie zapoznać się z wynikami tych funkcji

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database default

-- COMMAND ----------

create database test;

-- COMMAND ----------

show databases;

-- COMMAND ----------

drop database test;

-- COMMAND ----------

-- MAGIC %md Polecenia dot. tabel:
-- MAGIC 1. `show tables` [in nazwa_bazy] - wyświetla tabele w domyślnie wybranej bazie (opcjonalnie po in można podać inną bazę)
-- MAGIC 2. `show create table` nazwa_tabeli - wyświetla skrypt tworzący tabelę nazwa_tabeli
-- MAGIC 3. `show columns in` nazwa_tabeli - wyświetla kolumny w tabeli nazwa_tabeli
-- MAGIC 4. `show partitions` nazwa_tabeli - wyświetla partycje w tabeli partycjonowanej
-- MAGIC 5. `show tblproperties` - wyświetla właściwości tabeli
-- MAGIC 6. `describe table` - wyświetla opis tabeli
-- MAGIC 
-- MAGIC W następnych komórkach można samodzielnie zapoznać się z wynikami tych funkcji

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in default

-- COMMAND ----------

show create table uam_categories;

-- COMMAND ----------

-- MAGIC %md Możemy skorzystać z nazwy schematu, jeśli tabela znajduje się w innym schemacie niż domyślny.

-- COMMAND ----------

show create table default.uam_categories;

-- COMMAND ----------

show columns in uam_categories;

-- COMMAND ----------

-- MAGIC %md Żadna z tabel nie jest partycjonowana, więc poniższe polecenie zakończy się błędem

-- COMMAND ----------

show partitions uam_categories

-- COMMAND ----------

show tblproperties uam_categories

-- COMMAND ----------

describe uam_categories

-- COMMAND ----------

-- MAGIC %md `desc` jest aliasem `describe`

-- COMMAND ----------

desc uam_categories

-- COMMAND ----------

-- MAGIC %md Teraz możemy zapoznać się, co zawierają tabele umieszczone w bazie *default*. Do dyspozycji mamy uproszczone schematy 3 tabel opisujące podstawowe byty serwisu aukcyjnego:
-- MAGIC 1. *uam_categories* - kategorie, w których znajdują się oferty (*uam_offers*)
-- MAGIC 2. *uam_offers* - dostępne oferty
-- MAGIC 3. *uam_orders* - złożone zamówienia
-- MAGIC 
-- MAGIC Znaczenie poszczególnych kolumn będzie wyjaśnione w trakcie następnych części kursu

-- COMMAND ----------

desc uam_categories

-- COMMAND ----------

desc uam_offers

-- COMMAND ----------

desc uam_orders

-- COMMAND ----------

-- MAGIC %md # Klauzula SELECT
-- MAGIC 
-- MAGIC Za pomocą polecenie `SELECT` będziemy odpytywać nasze tabele. Składnia polecenie Spark SQL nie różni się znacząco od innych dialektów SQL:
-- MAGIC 
-- MAGIC 1. `SELECT`  - projekcja, czyli wybranie kolumn, które ma zwracać zapytanie
-- MAGIC 2. `FROM` - wskazanie relacji, z której odczytujemy dane
-- MAGIC 3. `WHERE` - selekcja, czyli wybór odpowiednich wierszy
-- MAGIC 4. `GROUP BY` - grupowanie
-- MAGIC 5. `ORDER BY`/`SORT BY` - sortowanie (globalne)/sortowanie wewnątrz partycji danych
-- MAGIC 6. `DISTRIBUTE BY` - repartycjonowanie po liście kolumn
-- MAGIC 7. `CLUSTER BY` - skrót od DISTRIBUTE BY oraz SORT BY
-- MAGIC 8. `WINDOW`  - funkcje def. okno (tzw. funkcje analityczne)
-- MAGIC 9. `LIMIT` - ograniczenia liczby zwracanych wierszy
-- MAGIC 
-- MAGIC W czasie zajęć pominiemy klauzule: `SORT BY`, `DISTRIBUTE BY` i `CLUSTER BY`.
-- MAGIC 
-- MAGIC Docs: https://docs.gcp.databricks.com/sql/language-manual/sql-ref-syntax-qry-select.html

-- COMMAND ----------

-- MAGIC %md ## Projekcja i selekcja - prosty przykład
-- MAGIC 
-- MAGIC Znajdźmy wszystkie oferty (identyfikator, ilość oraz łączną cenę), których sprzedano więcej niż 10.
-- MAGIC 
-- MAGIC Identyfikatorem jest kolumna *offer_id*, ilość znajduje się w kolumnie *quantity*, cena jednostkowa w kolumnie *unit_price*, zaś łączna cena w kolumnie *price* (*quantity* * *unit_price*)

-- COMMAND ----------

select
  offer_id,
  quantity,
  price as total
from
  uam_orders
where
  quantity > 10;

-- COMMAND ----------

-- MAGIC %md Projekcja i selekcja - zadanie
-- MAGIC 
-- MAGIC Znajdź wszystkie daty transakcji (`uam_orders.buyingTime`), gdzie cena przekracza 200 zł (`uam_orders.price`)

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md Rozwiązanie jest w ukrytej kolumnie poniżej

-- COMMAND ----------

select
  buyingTime
from
  uam_orders
where
  price > 200;

-- COMMAND ----------

-- MAGIC %md ## Projekcja i selekcja - `lateral view`
-- MAGIC 
-- MAGIC Klauzula `lateral view` służy do umieszczania elementów struktur złożonych (tablica, mapa) w osobnych wierszach. 
-- MAGIC 
-- MAGIC Załóżmy, że chcielibyśmy elementy poniższej tablicy umieścić w osobnych wierszach: 

-- COMMAND ----------

select
  array(1, 2, 3, 4) as arr

-- COMMAND ----------

-- MAGIC %md Możemy skorzystać z klauzuli `lateral view` i funkcji `explode`:

-- COMMAND ----------

select
  col
from
  lateral view explode(array(1, 2, 3, 4)) rel as col;

-- COMMAND ----------

-- MAGIC %md Lub wyłącznie z `explode` 

-- COMMAND ----------

select
  explode(array(1, 2, 3, 4))

-- COMMAND ----------

-- MAGIC %md `lateral view` - przykład
-- MAGIC 
-- MAGIC W tabeli *uam_offers* kolumna  *types* oznaczająca typ oferty jest typu *array < string >*
-- MAGIC 
-- MAGIC Aby sprawdzić, wszystkie możliwe typy ofert możemy posłużyć się zapytaniem

-- COMMAND ----------

select
  distinct types
from
  uam_offers 

-- COMMAND ----------

select
  distinct table_.col_
from
  uam_offers lateral view explode(types) table_ as col_

-- COMMAND ----------

-- MAGIC %md `lateral view` - zadanie
-- MAGIC 
-- MAGIC Znajdź wszystkie oferty (identyfikator *offer_id*, nazwę *offer_name*), które posiadają typ AUCTION. Spróbuj rozwiązać zadanie korzystając z `lateral view`, a następnie bez tej funkcji (użyj `array_contains`). Zbiór wynikowy ogranicz do 10 rekordów (klauzula `LIMIT`)
-- MAGIC 
-- MAGIC Podpowiedź: opis funkcji można sprawdzić poleceniem: `describe function array_contains`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md Rozwiązania znajdują się poniżej

-- COMMAND ----------

select
  offer_id,
  offer_name
from
  uam_offers lateral view explode(types) tab as col
where
  tab.col = 'AUCTION'
LIMIT
  10

-- COMMAND ----------

select
  offer_id,
  offer_name
from
  uam_offers
where
  array_contains(types, 'AUCTION')
limit
  10;

-- COMMAND ----------

-- MAGIC %md ## Funkcje wyższego rzędu (wyrażenia lambda)
-- MAGIC 
-- MAGIC Dla wersji Spark począwszy od 2.4.0 dostępne są funkcje wyższego rzędu (`higher-order functions`), które ułatwiają prace z typami kolumn array i map ([docs](https://docs.databricks.com/delta/data-transformation/higher-order-lambda-functions.html#)).
-- MAGIC 
-- MAGIC W przypadku, gdy chcielibyśmy zamienić wielkie litery na małe w *uam_offers.types*, możemy to zrobić następująco:

-- COMMAND ----------

select
  offer_id,
  types as original_types,
  transform(types, x -> lower(x)) as lowercased_types
from
  uam_offers

-- COMMAND ----------

-- MAGIC %md Funkcja `transform` jako pierwszy argument przyjmuje tablicę a następnie funkcję anonimową, która określa rodzaj transformacji. W powyższym przypadku, każdy element tablicy jest przekształcany funkcją `lower`.
-- MAGIC 
-- MAGIC 
-- MAGIC Możemy również posłużyć się klauzulami `lateral_view` i `group by` (`group by` dokładniej umówimy później), ale rozwiązanie będzie znacznie mniej zwięzłe:

-- COMMAND ----------

select
  offer_id,
  collect_list(original_type) as original_types,
  collect_list(lowercased_type) as lowercased_types
from
  (
    select
      offer_id,
      type_column as original_type,
      lower(type_column) as lowercased_type
    from
      uam_offers lateral view explode(types) exploded_type as type_column
  )
group by
  offer_id

-- COMMAND ----------

-- MAGIC %md Przydatne są też funkcje `exists` i `filter`. `exists` może być użyta w klauzuli WHERE do sprawdzenia, czy jakikolwiek element w tablicy spełnia warunek wyrażony funkcją.

-- COMMAND ----------

-- MAGIC %md Przykład - `exists`
-- MAGIC Policzymy oferty zawierające atrybut rozmiar

-- COMMAND ----------

desc uam_offers;

-- COMMAND ----------

select
  attributes
from
  uam_offers
where
  exists(attributes, a -> a.name = "rozmiar");

-- COMMAND ----------

select
  count(*)
from
  uam_offers
where
  exists(attributes, a -> a.name = "rozmiar");

-- COMMAND ----------

-- MAGIC %md Funkcja `filter` umożliwia usunięcie zbędnych elementów z tablicy. Możemy ograniczyć wyświetlane atrybuty w ofertach do tych, które opisują stan:

-- COMMAND ----------

select filter(attributes, a -> a.name = "stan") from uam_offers;

-- COMMAND ----------

-- MAGIC %md Ćwiczenie
-- MAGIC Znajdź wszystkie oferty (identyfikator offer_id, nazwę offer_name), które posiadają typ AUCTION - tym razem używając `exists`

-- COMMAND ----------

-- MAGIC %md Rozwiązanie

-- COMMAND ----------

select
  offer_id,
  offer_name
from
  uam_offers
where
  exists(types, t -> t = "AUCTION")

-- COMMAND ----------

-- MAGIC %md ## Operacje złączenia
-- MAGIC 
-- MAGIC W następnej części zajmiemy się operacjami złączenia dostępnymi w Spark SQL:
-- MAGIC 
-- MAGIC 1. `Inner join` - rekordy z obydwu łącząnych tabel muszą spełniać warunek połączeniowy
-- MAGIC 2. `Left outer join` - wszystkie rekordy z relacje po lewej stronie klauzuli są uwzględniane w zbiorze wynikowym i łączone są te z prawej strony, które spełniają warunek połączeniowy
-- MAGIC 3. `Right outer join` - wszystkie rekordy z relacje po prawej stronie klauzuli są uwzględniane w zbiorze wynikowym i łączone są te z lewej strony, które spełniają warunek połączeniowy
-- MAGIC 4. `Left semi join` - uwzględnia rekordy z lewej strony klauzuli, które spełniają warunek połączeniowy pomijając prawą tabelę w zbiorze wynikowym. Przy wielokrotnym złączeniu rekordów w zbiorze wynikowym umieszczany jest tylko jeden rekord
-- MAGIC 5. `Right semi join` - analogicznie jak `left semi join` z tym, że tabele zamienione są stronami
-- MAGIC 6. `Left anti join` - uwzględnia rekordy z lewej strony klauzuli, dla których nie istnieją łączące się rekordy z tabeli po prawej stronie
-- MAGIC 7. `Natural join` - złączenie, które bazuje na takich samych nazwach kolumn w obydwu tabelach
-- MAGIC 8. `Cross join` - iloczyn kartezjański tabel (brak warunków połączeniowych)

-- COMMAND ----------

-- MAGIC %md Operacja złączenia (`join`) - przykład `INNER JOIN`, `NATURAL JOIN` i `LEFT SEMI JOIN`
-- MAGIC 
-- MAGIC Sporządź zestawienie ofert sprzedanych danego dnia (identyfikator, nazwa). Wybierz tylko te oferty, które są droższe niż 200 zł.
-- MAGIC 
-- MAGIC Uwagi:
-- MAGIC 1. Warto zwrócić uwagę, że `SEMI JOIN` nie dubluje rekordów.
-- MAGIC 2. Proszę spróbować usunąć `DISTINCT` z przykładów poniżej i porównać wyniki.

-- COMMAND ----------

select
  distinct o.offer_id,
  o.offer_name
from
  uam_offers o
  join uam_orders t on o.offer_id = t.offer_id
where
  o.buynow_price > 200;

-- COMMAND ----------

select DISTINCT
  o.offer_id,
  o.offer_name
from
  uam_offers o
  natural join uam_orders
where
  o.buynow_price > 200;

-- COMMAND ----------

select
  o.offer_id,
  o.offer_name
from
  uam_offers o 
  left semi join uam_orders t on o.offer_id = t.offer_id
where
  o.buynow_price > 200;

-- COMMAND ----------

-- MAGIC %md Operacja złączenia - Przykład `LEFT/RIGHT OUTER JOIN` i `LEFT ANTI JOIN`
-- MAGIC 
-- MAGIC Znajdźmy oferty tańsze niż 10 zł, które nie znalazły nabywcy?

-- COMMAND ----------

select
  *
from
  uam_offers o
  left outer join uam_orders t on o.offer_id = t.offer_id
where
  t.offer_id is null
  and o.buynow_price < 10
limit
  10;

-- COMMAND ----------

select
  o.offer_id,
  o.offer_name
from
  uam_orders t
  right outer join uam_offers o on o.offer_id = t.offer_id
where
  t.offer_id is null
  and o.buynow_price < 10
limit
  10;

-- COMMAND ----------

select
  *
from
  uam_offers o 
  left anti join uam_orders t on o.offer_id = t.offer_id
where
  o.buynow_price < 10
limit
  10;

-- COMMAND ----------

-- MAGIC %md Operacja złączenia (join) - ćwiczenie
-- MAGIC 
-- MAGIC 
-- MAGIC Czy istnieją kategorie bez ofert?

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md Przykładowe rozwiązanie

-- COMMAND ----------

select
  count(*)
from
  uam_categories c left anti
  join uam_offers o on c.category_id = o.category_leaf
limit
  100;

-- COMMAND ----------

-- MAGIC %md ## Grupowanie `group by`
-- MAGIC 
-- MAGIC Klauzula działa analogicznie jak w innych dialektach SQL. 
-- MAGIC 
-- MAGIC 
-- MAGIC Oto prosty przykład:
-- MAGIC 
-- MAGIC Znajdź maksymalną i minimalną cenę (*buynow_price*) ofert w każdej z kategorii na 1. poziomie (*category_level1*)

-- COMMAND ----------

select
  c.category_level1,
  min(cast(buynow_price as double)),
  max(cast(buynow_price as double))
from
  uam_offers o
  join uam_categories c on c.category_id = o.category_leaf
group by
  c.category_level1;

-- COMMAND ----------

-- MAGIC %md Przejdźmy do bardziej zaawansowanego przykładu z wykorzystaniem klauzul `with rollup` oraz `grouping sets`:
-- MAGIC 
-- MAGIC Znajdźmy maksymalną i minimalną cenę ofert w każdej z kategorii na 1. poziomie oraz na 1. i 2. poziomie kategorii oraz najtańszą i najdroższą ofertę. Posortujmy wyniki tak, aby podsumowania poszczególnych kategorii były na początku (zaczynając od globalnego podsumowania).
-- MAGIC 
-- MAGIC Oto dwie propozycje rozwiązań:

-- COMMAND ----------

select
  c.category_level1,
  c.category_level2,
  min(cast(buynow_price as double)),
  max(cast(buynow_price as double))
from
  uam_offers o
  join uam_categories c on c.category_id = o.category_leaf
group by
  c.category_level1,
  c.category_level2 with rollup
order by
  category_level1 nulls first,
  category_level2 nulls first;

-- COMMAND ----------

select
  c.category_level1,
  c.category_level2,
  min(cast(buynow_price as double)),
  max(cast(buynow_price as double))
from
  uam_offers o
  join uam_categories c on c.category_id = o.category_leaf
group by
  c.category_level1,
  c.category_level2 grouping sets(
    (),
    (c.category_level1),
    (c.category_level1, c.category_level2)
  )
order by
  category_level1 nulls first,
  category_level2 nulls first;

-- COMMAND ----------

-- MAGIC %md Klauzula `with rollup` pozwala na tworzenie podsumowań hierarchicznych na wszystkich poziomach zgodnie z kolejnością grupowania, natomiast `grouping sets` pozwala dowolnie tworzyć grupy podsumowań. Dostępna jest jeszcze klauzula `WITH CUBE`, ale tworzy ona wszystkie możliwe podsumowania, co nie ma sensu w naszym przypadku:

-- COMMAND ----------

select
  c.category_level1,
  c.category_level2,
  min(cast(buynow_price as double)),
  max(cast(buynow_price as double))
from
  uam_offers o
  join uam_categories c on c.category_id = o.category_leaf
group by
  c.category_level1,
  c.category_level2 with cube
order by
  category_level1 nulls first,
  category_level2 nulls first;

-- COMMAND ----------

-- MAGIC %md Funkcje `grouping_id` pomaga określić jakiemu poziomu agregacji odpowiada dany rekord. Funkcja `grouping` określa, czy wartość `null` jest wynikiem agregacji, czy została wyliczona dla wartości `null` znajdujących się w zbiorze wejściowym. Najlepiej ilustruje to poniższy przykład.

-- COMMAND ----------

select
  c.category_level1,
  c.category_level2,
  min(cast(buynow_price as double)),
  max(cast(buynow_price as double)),
  grouping_id(),
  grouping(c.category_level1),
  grouping(c.category_level2)
from
  uam_offers o
  join uam_categories c on c.category_id = o.category_leaf
group by
  c.category_level1,
  c.category_level2 with rollup
order by
  category_level1 nulls first,
  category_level2 nulls first;

-- COMMAND ----------

-- MAGIC %md Zadanie
-- MAGIC 
-- MAGIC 
-- MAGIC Sporządź zestawienie obrotów (*double(unit_price)*  \*  *double(quantity)*) per kategoria na 1. poziomie (wraz z sumą całkowitą w ostatnim wierszu).

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md Rozwiązanie jest poniżej

-- COMMAND ----------

select
  c.category_level1,
  sum(double(unit_price) * double(quantity)) as total
from
  uam_orders o
  join uam_categories c on c.category_id = o.category_id
group by
  c.category_level1 with rollup
order by
  category_level1 nulls last;

-- COMMAND ----------

-- MAGIC %md Do nakładania ograniczeń na wynik funkcji grupujących służy klauzula `HAVING`

-- COMMAND ----------

-- MAGIC %md Przykład
-- MAGIC 
-- MAGIC Sporządź zestawienie obrotów jak w powyższym zadaniu ograniczając zbiór wynikowy do kategorii z obrotami powyżej 2000 zł

-- COMMAND ----------

select
  c.category_level1,
  sum(double(unit_price) * double(quantity)) as total
from
  uam_orders t
  join uam_categories c on c.category_id = t.category_id
group by
  c.category_level1 with rollup
having
  sum(double(unit_price) * double(quantity)) > 2000
order by
  category_level1 nulls last;

-- COMMAND ----------

-- MAGIC %md Warto zwrócić uwagę na kolejność wykonywania poszczególnych klauzul. Klauzula `HAVING` odfiltrowywuje po zgrupowaniu, obliczeniu sum cząstkowych i sumy globalnych obrotów (kolumna *null*).

-- COMMAND ----------

-- MAGIC %md Inne przydatne funkcje grupujące znajdują się poniżej:
-- MAGIC 
-- MAGIC `approxCount([distinct] …)`
-- MAGIC 
-- MAGIC `avg`
-- MAGIC 
-- MAGIC `collect_list`  
-- MAGIC 
-- MAGIC `collect_set`  
-- MAGIC 
-- MAGIC `corr`  
-- MAGIC 
-- MAGIC `count([distinct] ...) ` 
-- MAGIC 
-- MAGIC `first`  
-- MAGIC 
-- MAGIC `kurtosis`  
-- MAGIC 
-- MAGIC `last`  
-- MAGIC 
-- MAGIC `max`  
-- MAGIC 
-- MAGIC `mean`  
-- MAGIC 
-- MAGIC `min`  
-- MAGIC 
-- MAGIC `skewness`
-- MAGIC 
-- MAGIC `stddev_pop`  
-- MAGIC 
-- MAGIC `stddev_samp`  
-- MAGIC 
-- MAGIC `stddev`  
-- MAGIC 
-- MAGIC `sum([distinct] ...) `
-- MAGIC 
-- MAGIC `var_pop`  
-- MAGIC 
-- MAGIC `var_samp` 
-- MAGIC 
-- MAGIC `variance`

-- COMMAND ----------

-- MAGIC %md # Operacja `cache`
-- MAGIC 
-- MAGIC 
-- MAGIC W przypadku, gdy wiemy, że będziemy wielokrotnie używali daną tabelę 
-- MAGIC w zapytaniach możemy skorzystać z operacji cache. Spowoduje ona zachowanie danej tabeli w pamięci operacyjnej. 

-- COMMAND ----------

cache table uam_categories

-- COMMAND ----------

-- MAGIC %md Operacją powodującą zwolnienie pamięci podręcznej jest `uncache table`:

-- COMMAND ----------

uncache table uam_categories;

-- COMMAND ----------

-- MAGIC %md Porównajmy plany zapytań przed i po operacji `cache`:

-- COMMAND ----------

explain
select
  count(1)
from
  uam_categories;

-- COMMAND ----------

cache table uam_categories;

-- COMMAND ----------

explain
select
  count(1)
from
  uam_categories;

-- COMMAND ----------

-- MAGIC %md W planie zapytania przed operacją `cache` pojawia się:
-- MAGIC 
-- MAGIC *LocalTableScan [count(1) ...]*
-- MAGIC 
-- MAGIC co oznacza, że dane są odczytywane z dysku.
-- MAGIC 
-- MAGIC 
-- MAGIC Po operacji `cache` w planie odnajdujemy:
-- MAGIC 
-- MAGIC *Scan In-memory table uam_categories*
-- MAGIC 
-- MAGIC Co oznacza, że dane są odczytywane z pamięci operacyjnej.

-- COMMAND ----------

-- MAGIC %md  Całą pamięć podręczną możemy wyczyścić poleceniem `clear cache`:

-- COMMAND ----------

clear cache

-- COMMAND ----------

-- MAGIC %md # Funkcje analityczne
-- MAGIC 
-- MAGIC Działanie funkcji analitycznych jest analogiczne jak w przypadku innych dialektów SQL.
-- MAGIC 
-- MAGIC Prześledźmy działanie funkcji analitycznych na przykładach

-- COMMAND ----------

-- MAGIC %md Znajdźmy kategorię (1. poziom), w której użytkownicy (*buyer_id*) dokonali 1. transakcji (jako kupujący). Wynik ogranicz do 10 rekordów.

-- COMMAND ----------

select
  buyer_id,
  category_level1
from
  (
    select
      t.buyer_id,
      rank() over(
        partition by t.buyer_id
        order by
          t.buyingTime
      ) as rank_,
      t.category_id
    from
      uam_orders t
    qualify rank_ = 1
  ) t
  join uam_categories c on c.category_id = t.category_id
limit 10


-- COMMAND ----------

-- MAGIC %md Aby wywołać funkcję analityczną `rank()` musimy podać towarzyszącą jej definicję okna `over(partition by t.buyer_id order by t.buyingTime)`. W tym przypadku okno tworzy partycje dla każdego kupującego (*buyer_id*) i wymusza sortowanie po czasie zakupu (*buyingTime*). Zgodnie z czasem zakupu tworzony jest ranking (funkcja `rank`).
-- MAGIC 
-- MAGIC Alternatywne i bardziej zwięzłe rozwiązanie jest poniżej:

-- COMMAND ----------

select
  distinct *
from
  (
    select
      FIRST_VALUE (category_level1) OVER (
        partition by buyer_id
        ORDER BY
          buyingTime
      ) first_cat_level_1,
      buyer_id
    from uam_categories c
    join uam_orders o on c.category_id = o.category_id
  )
limit 10

-- COMMAND ----------

-- MAGIC %md Dla zwiększenia czytelności kodu możemy się posłużyć klauzulą `WINDOW`:

-- COMMAND ----------

select
  buyer_id,
  category_level1
from
  (
    select
      t.buyer_id,
      rank() over buying_time_window as rank_,
      t.category_id
    from
      uam_orders t 
    qualify rank_ = 1
    window buying_time_window as (
        partition by t.buyer_id
        order by
          t.buyingTime
      )
  ) t
  join uam_categories c on c.category_id = t.category_id
limit 10

-- COMMAND ----------

-- MAGIC %md Może być to szczególnie przydatne przy wykorzystywaniu tej samej definicji okna w jednym zapytaniu

-- COMMAND ----------

-- MAGIC %md Definicje okna mogą opierać się na sąsiednich wierszach (`rows between ... and ... `) lub na wartościach wierszy (`range between ... and ... `). 
-- MAGIC 
-- MAGIC W ten sposób możemy przykładowo:

-- COMMAND ----------

-- MAGIC %md Znaleźć maksymalną cenę sprzedanego towaru w odniesieniu do 2 poprzednich i następnych zakupów

-- COMMAND ----------

select
  seller_id,
  max(price) over (
    partition by seller_id
    order by
      buyingTime rows between 2 preceding
      and 2 following
  ) as max_price_in_window
from
  uam_orders
limit
  10;

-- COMMAND ----------

-- MAGIC %md Znajdźmy liczbę ofert w zakresie cenowym między ceną danej oferty a ceną danej oferty + 5 PLN

-- COMMAND ----------

select
  offer_id,
  count(*) over (
    order by
      cast(buynow_price as decimal(12, 2)) range between current row
      and 5.0 following
  ) as num_of_offers_with_the_same_price_or_higher_but_less_than_5PLN,
  cast(buynow_price as decimal(12, 2)) as buynow_price
from
  uam_offers
order by
  cast(buynow_price as decimal(12, 2)) nulls last
limit
  1000;

-- COMMAND ----------

-- MAGIC %md # Usuwanie i tworzenie tabel
-- MAGIC 
-- MAGIC Najważniejsze polecenia to:
-- MAGIC 1. `DROP TABLE [IF EXISTS] [nazwa_bazy.]nazwa_tabeli` - usuwa tabelę (z domyślnie ustawionej bazy). Jeśli tabela nie istnieje zwracany jest wyjątek. Wyjątek nie jest zwracany, gdy użyjemy opcjonalnej klauzuli `IF EXISTS`
-- MAGIC 2. `CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [nazwa_bazy.]nazwa_tabeli ... ` - tworzy tabelę (opcjonalnie temporalną, czyli na potrzeby bieżącej sesji) o nazwie nazwa_tabeli i w bazie nazwa_bazy. Przykładowe schematy tabel można sprawdzić poleceniem `SHOW CREATE TABLE`. Nie będziemy szczegółowo analizować składni tego polecenia.
-- MAGIC 3. `CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [nazwa_bazy.]nazwa_tabeli ... ` - podobne polecenie jak opisane w pkt. 2, ale służy do tworzenia tabel eksternalnych
-- MAGIC 4. `CREATE TABLE [IF NOT EXISTS] [nazwa_bazy.]nazwa_tabeli LIKE [nazwa_bazy_2.]nazwa_tabeli_2` - tworzy tabelę nazwa_tabeli o schemacie nazwa_tabeli_2 bez kopiowania danych
-- MAGIC 
-- MAGIC Dla zainteresowanych: https://docs.gcp.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-table.html
-- MAGIC 
-- MAGIC Tabele można również tworzyć korzystając ze składni kompatybilnej z Hive'em.

-- COMMAND ----------

-- MAGIC %md # Wstawianie i przechowywanie danych
-- MAGIC 
-- MAGIC Poniższe polecenia służą do wstawiania danych do tabeli [nazwa_bazy.]nazwa_tabeli (opcjonalnie konkretnej partycji specyfikacja_partycji) za pomocą klauzuli `SELECT`:
-- MAGIC 1. `INSERT INTO [TABLE] [nazwa_bazy.]nazwa_tabeli [PARTITION specyfikacja_partycji] SELECT ...` - powoduje dodanych nowych danych i zachowanie obecnych
-- MAGIC 2. `INSERT OVERWRITE TABLE [nazwa_bazy.]nazwa_tabeli [PARTITION specyfikacja_partycji] SELECT ...` - dodane nowe dane nadpisując dotychczasowe
-- MAGIC 
-- MAGIC Poniższe polecenia działają podobnie, ale umożliwiają samodzielnie specyfikowanie wstawianych rekordów:
-- MAGIC 1. `INSERT INTO [TABLE] [nazwa_bazy.]nazwa_tabeli [PARTITION specyfikacja_partycji] VALUES ...`
-- MAGIC 2. `INSERT OVERWRITE TABLE [nazwa_bazy.]nazwa_tabeli [PARTITION specyfikacja_partycji] VALUES ...`
-- MAGIC 
-- MAGIC Dostępne jest również polecenie `INSERT OVERWRITE [LOCAL] DIRECTORY ... `, które umożliwia zrzut rekordów bezpośrednio do systemu plików z pominięcięm tabel.

-- COMMAND ----------

-- MAGIC %md Przykłady
-- MAGIC 
-- MAGIC Utworzymy tabelę partycjonowaną i wstawimy rekord do konkretnej partycji:

-- COMMAND ----------

drop table if exists uam_user_segments;
create table uam_user_segments (
  us_id bigint,
  trans_count bigint,
  turnover double,
  segment string
) partitioned by (month_ string) stored as orc;

insert into
  uam_user_segments partition(month_ = '2020-01-01')
values
  (1, 1, 1.0, 'small');
  
insert into
  uam_user_segments partition(month_ = '2020-02-01')
values
  (2, 100, 100.0, 'medium');

-- COMMAND ----------

show create table uam_user_segments;

-- COMMAND ----------

-- MAGIC %md Sprawdzimy teraz w jakiej ścieżce zapisują się nasze dane na `DBFS` (szukamy atrybutu *Location*):

-- COMMAND ----------

desc formatted uam_user_segments;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/uam_user_segments

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/uam_user_segments/month_=2020-01-01/

-- COMMAND ----------

-- MAGIC %md Możemy odpytać naszą tabelę na dwa sposoby:

-- COMMAND ----------

-- 1: SQL
select
  *
from
  uam_user_segments;

-- COMMAND ----------

-- 2: bezpośrednio z katalogu
select
  *
from
  `orc`.`dbfs:/user/hive/warehouse/uam_user_segments/`

-- COMMAND ----------

-- MAGIC %md Nasza tabela jest partycjonowana, więc optymalniej będzie ją odpytywać zakładając warunek na klucz partycjonujący:

-- COMMAND ----------

-- 1: SQL
select
  *
from
  uam_user_segments
where
  month_ = '2020-02-01';

-- COMMAND ----------

-- 2: bezpośrednio z katalogu
select
  *
from
  `orc`.`dbfs:/user/hive/warehouse/uam_user_segments/month_=2020-02-01/`

-- COMMAND ----------

-- MAGIC %md Partycjonowanie jest jedną z najbardziej efektywnych i popularnych technik optymalizowania zapisu i odczytu danych. W przypadku zapisu możliwe są operacja dodawania, usuwania, zmiany ścieżki i nazwy partycji (szczegóły: https://docs.gcp.databricks.com/sql/language-manual/sql-ref-partition.html). Skupmy się jednak na samym odczycie i porównajmy plany zapytań z warunkiem na klucz partycji i bez:

-- COMMAND ----------

explain
select
  *
from
  uam_user_segments
where
  month_ = '2020-02-01';

-- COMMAND ----------

-- MAGIC %md W powyższym planie interesujące są pogrubione fragmenty świadczące o odfiltrowaniu odpowiednich partycji na etapie wczytywania danych:
-- MAGIC 
-- MAGIC == Physical Plan == \*(1) ColumnarToRow +- FileScan orc default.uam_user_segments[us_id#6219L,trans_count#6220L,turnover#6221,segment#6222,month_#6223] Batched: true, DataFilters: [], Format: ORC, **Location: InMemoryFileIndex[dbfs:/user/hive/warehouse/uam_user_segments/month_=2020-02-01]**, **PartitionFilters: [isnotnull(month_#6223), (month_#6223 = 2020-02-01)]**, PushedFilters: [], ReadSchema: struct<us_id:bigint,trans_count:bigint,turnover:double,segment:string>

-- COMMAND ----------

-- MAGIC %md Gdy nie założymy warunku na klucz partycji, to odczytywana jest cała tabela:

-- COMMAND ----------

explain
select
  *
from
  uam_user_segments
where
  us_id = 1;

-- COMMAND ----------

-- MAGIC %md Dygresja - lokalny system plików (`local file system`) vs. rozproszony system plików (`distributed file system`)
-- MAGIC 
-- MAGIC Porównajmy poniższe polecenia. Pierwsze z nich odpytuje lokalny system plików. Gdyby to był notebook uruchamiany na naszym komputerze, to byłby to nasz dysk twardy. 
-- MAGIC 
-- MAGIC Drugie polecenie odpytuje rozproszony system plików.

-- COMMAND ----------

-- MAGIC %sh ls /

-- COMMAND ----------

-- MAGIC %fs ls /

-- COMMAND ----------

-- MAGIC %md Między danymi na rozproszonym systemie plików a metadanymi (nazwy schematów i tabel) istnieje zależność zdefiniowana w atrybucie `Location`, typie odczytywanego i zapisywanego pliku oraz konwencji nazewniczej (katalogi mapowane są na partycje).
-- MAGIC 
-- MAGIC W związku z tym nic nie stoi na przeszkodzie, aby odwrócić sposób naszego przetwarzania - najpierw utworzyć plik z danymi, a potem tabelę ekstarnalną (`EXTERNAL TABLE`):

-- COMMAND ----------

INSERT
  OVERWRITE DIRECTORY '/tmp/sample_data' stored as parquet
select
  'Moje testowe dane' as testowa_kolumna

-- COMMAND ----------

-- MAGIC %fs ls /tmp/sample_data

-- COMMAND ----------

create external table testowa_tabela (testowa_kolumna string) location '/tmp/sample_data' stored as parquet;

-- COMMAND ----------

select * from testowa_tabela;

-- COMMAND ----------

select * from `parquet`.`/tmp/sample_data`

-- COMMAND ----------

-- MAGIC %md Co się stanie z danymi jeśli usuniemy tabelę eksternalną:

-- COMMAND ----------

DROP TABLE testowa_tabela;

-- COMMAND ----------

-- MAGIC %fs ls /tmp/sample_data

-- COMMAND ----------

-- MAGIC %md Musimy ręcznie usunąć dane:

-- COMMAND ----------

-- MAGIC %fs rm -r /tmp/sample_data

-- COMMAND ----------

-- MAGIC %md W przypadku dodawania nowych danych w tabelach partycjonowanych, dane będą zarejestrowane do odczytu po wykonaniu `MSCK REPAIR TABLE` (lub poleceniu `ALTER TABLE ... ADD PARTITION ... `):

-- COMMAND ----------

msck repair table uam_user_segments;

-- COMMAND ----------

-- MAGIC %md Dygresja - dane możemy również zrzucić na lokalny system plików

-- COMMAND ----------

-- MAGIC %sh mkdir /tmp/dump

-- COMMAND ----------

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/dump' using csv select 'Moje testowe dane' as testowa_kolumna

-- COMMAND ----------

-- MAGIC %sh ls /tmp/dump

-- COMMAND ----------

-- MAGIC %sh cat /tmp/dump/*.csv

-- COMMAND ----------

-- MAGIC %md Zadanie
-- MAGIC 
-- MAGIC Na bazie tabeli *uam_categories*:
-- MAGIC 1. Stwórz zrzut do katalogu */tmp/orc_categories* na rozproszonym systemie plików w formacie orc
-- MAGIC 2. Stwórz nową bazę o nazwie *moja_baza*
-- MAGIC 3. Stwórz tabelę eksternalną o nazwie *moja_baza.orc_categories*, która będzie odczytywać */tmp/orc_categories*
-- MAGIC 
-- MAGIC Czy możemy stworzyć tabelę o innej strukturze?
-- MAGIC Czy możemy odpytać taką tabelę?

-- COMMAND ----------

show create table uam_categories;

-- COMMAND ----------

-- MAGIC %md Rozwiązanie

-- COMMAND ----------

INSERT
  OVERWRITE DIRECTORY '/tmp/orc_categories' stored as orc
select
  *
from
  uam_categories;

CREATE DATABASE IF NOT EXISTS moja_baza;

DROP TABLE IF EXISTS moja_baza.orc_categories;

CREATE EXTERNAL TABLE moja_baza.orc_categories (
    `category_id` STRING,
    `category_level1` STRING,
    `category_level2` STRING,
    `category_level3` STRING
  ) STORED AS ORC LOCATION '/tmp/orc_categories';

SELECT
  count(1)
FROM
  moja_baza.orc_categories

-- COMMAND ----------

-- MAGIC %md Rozwiązanie - co się stanie gdy utworzymy tabelę o innej strukturze

-- COMMAND ----------

drop table if exists moja_baza.orc_categories_2;

CREATE EXTERNAL TABLE moja_baza.orc_categories_2 (
  `category_id` STRING,
  `category_level1` STRING,
  `category_level2_xxx` STRING,
  `category_level3` STRING
) STORED AS ORC LOCATION '/tmp/orc_categories';

select * from moja_baza.orc_categories_2;

-- COMMAND ----------

DROP TABLE IF EXISTS moja_baza.orc_categories_3;

CREATE EXTERNAL TABLE moja_baza.orc_categories_3 (
  `category_id` TIMESTAMP, -- <-- Inny typ danych
  `category_level1` INT -- <-- Inny typ danych
) STORED AS ORC LOCATION '/tmp/orc_categories';

select * from moja_baza.orc_categories_3;

-- COMMAND ----------

-- MAGIC %md Inne przykłady `CREATE` i `INSERT`
-- MAGIC 
-- MAGIC 
-- MAGIC Utwórzymy tabele za pomocą poleceń `SELECT` i wykorzystaniem klauzuli `TABLESAMPLE` (służącej do zwracania próbki danych):

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/user/hive/warehouse/uam_orders_sample

-- COMMAND ----------

create table if not exists uam_orders_sample as
select
  *
from
  uam_orders TABLESAMPLE (1 percent);
  
insert into
  uam_orders_sample
select
  *
from
  uam_orders TABLESAMPLE (10 rows);

-- COMMAND ----------

-- MAGIC %md Możemy też utworzyć tabelę bez danych:

-- COMMAND ----------

drop table if exists moja_baza.orc_categories_sample;
create table if not exists moja_baza.orc_categories_sample like moja_baza.orc_categories;

-- COMMAND ----------

-- MAGIC %md # Operacje na zbiorach
-- MAGIC 
-- MAGIC 
-- MAGIC Przeanalizujmy działania na zbiorach na przykładzie dwóch zapytań (odpowiednio liczby naturalne parzyste mniejsze od 10 i liczby naturalne mniejsze od 10). Wyjaśnienia działania funkcji `RANGE` znajduje się w kolejnej komórce.

-- COMMAND ----------

select
  *
from
  range(0, 10, 2);


-- COMMAND ----------

select
  *
from
  range(10);

-- COMMAND ----------

-- MAGIC %md Funkcja tabelaryczna `RANGE` służy do generowania tabel jednokolumnowych, gdzie kolejne wiersze są elementami ciągu arytmetycznego:
-- MAGIC 
-- MAGIC Funkcja obsługuje następujące parametry wejściowe: 
-- MAGIC 
-- MAGIC 1. (end: long)
-- MAGIC 2. (start: long, end: long)
-- MAGIC 3. (start: long, end: long, step: long)
-- MAGIC 4. (start: long, end: long, step: long, numPartitions: integer)
-- MAGIC 
-- MAGIC 
-- MAGIC start - 1. element ciągu
-- MAGIC 
-- MAGIC end - 2. element ostatni
-- MAGIC 
-- MAGIC step - określa o ile mają się zwiększać kolejne wiersze
-- MAGIC 
-- MAGIC numPartitions - liczba partycji 

-- COMMAND ----------

-- MAGIC %md Sumę zbiorów otrzymamy (może zawierać duplikaty):

-- COMMAND ----------

select
  *
from
  range(0, 10, 2)
union all
select
  *
from
  range(10);

-- COMMAND ----------

-- MAGIC %md Część wspólna zbiorów:

-- COMMAND ----------

select
  *
from
  range(0, 10, 2)
intersect
select
  *
from
  range(10);

-- COMMAND ----------

-- MAGIC %md Różnica zbiorów:

-- COMMAND ----------

select
  *
from
  range(10) 
  minus
select
  *
from
  range(0, 10, 2);

-- COMMAND ----------

-- MAGIC %md Zadanie
-- MAGIC 
-- MAGIC Znajdźmy oferty, które nie znalazły nabywcy (identyfikatory) korzystając z operacji na zbiorach.

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md Rozwiązanie jest poniżej:

-- COMMAND ----------

select
  offer_id
from
  uam_offers minus
select
  offer_id
from
  uam_orders;
