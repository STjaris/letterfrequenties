# letterfrequenties

## Uitleg van de werking van de verschillende jobs

### Job 1:
In job 1 worden de verschillende bigrammen gecreeerd vanuit de gegeven text in de args. 

### Job 2:
In job 2 worden alle eerste letters verzamelt van de bigrammen. Deze worden dan per letter opgeteld.

### Job 3:
In job 3 worden de probabilities van elke bigram berekend. Dit wordt gedaan door het aantal per bigram te delen door de totale hoeveelheid van dezelfde eerste letter van de bigram.

### Job 4:
In job 4 worden er scores berekent voor de inputtext aan de hand van de gegenereerde matrixen van bigrammen voor NL en EN.
*De output van job 4 geeft de totale score over de gehele text aan en niet per regel.*

## Het aanmaken van de Matrixen
Voor het aanmaken van de matrixen voor NL en EN zijn de volgende texten gebruikt:
- Columbus_De_ontdekker_van_Amerika_by_John_S_C_Abbott (NL)
- The_Cosmic_Looters_by_Edmond_Hamilton (EN)

Beide texten zijn gevonden op https://www.gutenberg.org/

## Instructies
De .JAR is te vinden in de directory: letterfrequenties/target/letterfrequenties-1.0-SNAPSHOT.jar

Voor het uitvoeren van deze jar moet de volgende commando gebruikt worden in Windows:

hadoop jar [path to .jAR] LetterFrequenties [path to input] [path to output]


## Notities

- Deze opdracht is alleen uitgevoerd
- Het implementeren van de score bepalen en tellen van regels in niet gelukt i.v.m. te weinig tijd
- tijdens het uitvoeren van de commando op Windows werd deze error weergeven: *INFO ipc.Client: Retrying connect to server: localhost/127.0.0.1:8032. Already tried 1 time(s); retry policy is RetryUpToMaximumCountWithFixedSleep(maxRetries=10, sleepTime=1000 MILLISECONDS)*. Deze foutmelding heb ik niet weten op te lossen.
- Screenshots zijn te vinden in de folder: letterfrequenties/screenshots


