# MyBear : Librairie Python de gestion de données alternative à Pandas

MyBear est une librairie Python qui permet la gestion de données à la manière de la célèbre librairie Pandas. Le concept principal tourne autour des DataFrames, emprunté à Pandas mais intégrant également d'autres fonctionnalités élémentaires pour la rendre unique. MyBear utilise principalement deux classes : `Series` et `DataFrame`, grâce auxquelles toutes les opérations sont réalisées.

## Classe Series

Une `Series` peut être vue comme une colonne dans un `DataFrame` qui, en plus des données, contient une étiquette (ou un nom) et des informations statistiques. Ces statistiques (taille, nombre de valeurs manquantes et type de données) sont calculées automatiquement lors de la création de la `Series`.

### Implémentation de la Classe Series

- **Constructeur** : Prend une liste de valeurs comme données et une chaîne de caractères comme nom.
- **Propriété `iloc`** : Permet une indexation basée sur la position des éléments. Renvoie soit une seule valeur soit une Series contenant les valeurs mentionnées.
- **Fonctions statistiques** : Inclut `max`, `min`, `mean`, `std`, et `count`, chaque fonction renvoyant le calcul associé.

## Classe DataFrame

Un `DataFrame` contient un ensemble de Series ayant toutes la même liste d'indices.

### Implémentation de la Classe DataFrame

- **Constructeur** : Il existe plusieurs surcharges de constructeurs. Une version permet de charger une liste de Series en tant que DataFrame, tandis qu'une autre permet de charger directement les colonnes et les listes de valeurs comme paramètres.
- **Propriété `iloc`** : Permet une indexation basée sur la position des éléments. Renvoie soit une seule valeur, une Series, ou un DataFrame contenant les valeurs ou les lignes/colonnes mentionnées.
- **Fonctions statistiques** : Inclut `max`, `min`, `mean`, `std`, et `count`, chaque fonction renvoyant le calcul associé pour chaque colonne du DataFrame.
- **Chargement de fichiers CSV (`read_csv`)** : La fonction membre statique `read_csv` permet de charger un fichier CSV en tant que DataFrame.
- **Chargement de fichiers JSON (`read_json`)** : La fonction membre statique `read_json` permet de charger un fichier JSON en tant que DataFrame. Les formats de fichiers acceptables incluent "records" et "columns".
- **Groupement (`groupby`)** : Combine et agrège plusieurs lignes d'un DataFrame en formant des groupes à partir d'une ou plusieurs colonnes.
- **Jointure (`join`)** : Combine les données de deux DataFrames. Peut réaliser des jointures à gauche, à droite, internes et externes.

## Comment utiliser
Des instructions d'utilisation détaillées et des exemples seront fournis dans la documentation. Veuillez vous y référer pour un guide complet sur l'utilisation de la librairie MyBear.
