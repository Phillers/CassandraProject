# CassandraProject
## Problem
Zasoby reprezentowane rpzez liczby podzielone na bloki przydzielane procesom. Proces chce uzyskać n liczb obok siebie w jednym bloku, aplikacja musi mu je rozydzielić albo odpowiedzieć ze nie ma wystarczajaco wolnego miejsca.
## Struktura danych
### Liczby
Przedstawione jako numer bloku i liczbę, dodatkowo numer procesu któremu jest obecnie przydzielona.
```
"CREATE TABLE Numbers(\n" +
                "  block int,\n" +
                "  number int,\n" +
                "  process int,\n" +
                "  PRIMARY KEY (block, number)" +
");"
```
### Blokady bloków
Aby uniknąć sytuacji, że kilka procesów dostanie tą samą przestrzeń, dodatkowa struktura, przypisująca do każdego bloku proces, który w danym momencie ma prawo dostać przydział liczb z tego bloku.
```
"CREATE TABLE Blocks(\n" +
                "  block" +
                " int PRIMARY KEY,\n" +
                "  process int,\n" +
");"
```
### Blokada globalna
Żebyu zminimalizować sytuacje, że proces po otrzymaniu blokady bloku ją straci zanim zdąży dostać liczby, dodatkowa struktura z jednym rekordem, określająca, który proces w danym momencie może zostać wpisany do blokady bloku.
```
"CREATE TABLE Lock(\n" +
                "  key int PRIMARY KEY,\n" +
                "  process int,\n" +
");"
```
## Proces
Proces, który chce uzysać zasoby, wysyła ile liczb chce dostać, jeśli jest to większe niż rozmiar bloku to od razu dostaje odpowiedź o porażce. W przeciwnym razie stara się wpisać swoj identyfikator do globalnej blokady. 
Odczytuje jej wartośc, jeśli jest pusta to wysyła swój identyfikator i czeka aż odczyta z niej swój identyfikator, albo znowu będzie pusta. 
Jeśli udało się uzyskać blokadę to pobiera listę bloków, szuka pierwszego niezajętego i zajmuje blokadę. 
Jeśli się udało to pobiera listę liczb tego bloku i sprawdza czy jest wystarczająco liczb pod rząd. Jeśli tak to je zajmuje i zwracany jest sukces. W przeciwnym razie blok dopisywany jest do listy przeszukanych i cały proces zaczyna się od nowa.
### Błędy
Jeśli nie ma żadnego wolnego bloku lub nie ma wystarczająćo liczb w zadnym bloku to zwracana jest porażka. Jeśli nastąpi błąd połączenia to zwracana jest porażka, jednak jeśli nastąpi on podczas zajmowania liczb to zwracany jest częściowy sukces i proces powinien zwolnić liczby, które zajął.
Podczas zwalniania liczb sprawdzane jest czy wszystkie liczby, które mają być zwolnione faktycznie są zajęte przez ten prioces, jeśli nie to rzucany jest wyjątek.
## Potencjalne usprawnienia
Niestety, ponieważ lista bloków jest zwracana w tej samej kolejności, im więcej początkowych bloków jest wypełnione tym więcej bloków musi przeszukać proces zanim, wróci z wywołania. Przyspieszyłoby to na przykład mieszanie kolejności bloków przed ich przeszukiwaniem.
