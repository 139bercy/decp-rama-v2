# decp-rama-v2
Decp-rama-v2 a pour objectif de recoder decp-rama (https://github.com/139bercy/decp-rama) en langage Python. Il s'agit encore d'un projet et le code présenté n'est donc pas celui aujourd'hui utilisé lors de la publication de base de données sur data.gouv.

Rappel de ce que sont les données essentielles de la commande publique (ou DECP) sur [sur le blog de data.gouv.fr](https://www.data.gouv.fr/fr/posts/le-point-sur-les-donnees-essentielles-de-la-commande-publique/).

L'objectif de ce projet est d'identifier toutes les sources de DECP, les aggréger afin de les publier (**[jeu de données sur data.gouv.fr](https://www.data.gouv.fr/fr/datasets/5cd57bf68b4c4179299eb0e9)**) aux formats JSON et XML réglementaires.

La procédure standard est la suivante pour chaque source de données:

**1. ETAPE GET**

Nous téléchargeons les données d'une source dans son format d'origine, XML ou JSON (les DECP n'existent pas dans d'autres formats) dans le dossier /sources dans un répertoire spécifique à la source des données.

**2. ETAPE CONVERT**

Nous convertissons par la suite en DataFrame afin de faire les opérations de nettoyage et d'aggrégation.

**3. ETAPE FIX**

Certaines données sources n'étant pas valides, nous corrigeons ce qui peut être corrigé (par exemple le format d'une date). Si certains champs manquent dans les données, nous avons pris le parti de les garder et de signaler ces anomalies. On supprime également les lignes dupliquées (marchés présents plusieurs fois dans la source de données).

**4. ETAPE GLOBAL**

Les DataFrames sont ensuite aggrégés et les duplicats entre sources différentes sont supprimés. Le DataFrame est ensuite exporté en XML et JSON.
 
**5. DESCRIPTION DU CODE**

**5.1.** *metadata.json*

**5.2.** *ProcessFactory.py*

La classe *ProcessFactory* initialise les "processes" correspondant aux sources des DECP.

**5.3.** *specific_process/XXXProcess.py* et *SourceProcess.py*

Chaque source XXX correspond à une classe *XXXProcess* dans le dossier *specific_process*. La classe *XXXProcess* est une sous-classe de la classe *SourceProcess*.

Étapes de traitement d'une source :
- initialisation et récupération des URL des fichiers à télécharger ;
- **get()** : nettoyage du dossier de la source et téléchargement des fichiers ;
- **convert()** : conversion du fichier XML ou JSON en DataFrame ;
- **fix()** : traitement de la source pour se ramener à un format défini et déduplication.

**5.4.** *GlobalProcess.py*

Cette classe a en attribut une liste de DataFrames et les méthodes :


- **merge_all()** : concaténation des DataFrames ;
- **fix_all()** : traitement du DataFrame global, notamment les formats ;
- **drop_duplicate()** : suppression des lignes dupliquées (marchés présents dans plusieurs sources de données) ;
- **export_to_xml()** : exportation du DataFrame au format XML ;
- **export_to_json()** : exportation du DataFrame au format JSON.

**6. CONVENTIONS DE FORMAT**

**6.1.** Avant aggrégation : **fix()**

Avant d'aggréger les DataFrames et de supprimer les doublons, il est nécessaire de les mettre au même format. Nous avons choisi de nous ramener au format d'un XML converti en DataFrame.


Les colonnes "titulaires", "modifications" et "concessionnaires" doivent être modifiées dans les sources JSON (*AwsProcess* et *EmarProcess*) pour se ramener au format des sources XML. Les None sont aussi remplacés par des listes vides.

Par exemple, dans la colonne "titulaires" d'un DataFrame provenant d'une source JSON, les valeurs sont de la forme :

[ { typeIdentifiant: "type1", id: "id1", denominationSociale: "den1" }, { typeIdentifiant: "type2", id: "id2", denominationSociale: "den2" } ]

et sont modifiées en

[ { titulaire: [ { typeIdentifiant: "type1", id: "id1", denominationSociale: "den1" }, { typeIdentifiant: "type2", id: "id2", denominationSociale: "den2" } ] } ].

On ajoute aussi la colonne source dans chaque DataFrame.



**6.2.** Après aggrégation : **fix_all()**

- Les identifiants des acheteurs et les codes des lieuExecution sont convertis en string ;
- Les colonnes inutiles sont supprimées ;
- Les dates sont mises au format YYYY-MM-DD ;
- Les id vides ou None sont remplacés par 0 ;
- Les dureeMois vides ou None sont remplacées par 0 et les autres sont converties en int.

**6.3.** Avant exportation XML

Le DataFrame aggrégé a été mis au format correspondant à une source XML et n'a pas besoin d'être modifié avant d'être exporté en XML.

Le fichier XML exporté est stocké à l'emplacement *results/decp.xml*.

**6.4.** Avant exportation JSON

Avant d'être exporté en JSON, nous devons modifier le DataFrame aggrégé, notamment pour supprimé les indices "titulaire" dans la colonne "titulaires" ou "modification" dans "modifications", propres au XML.
Pour cela, le DataFrame est converti en dictionnaire, et les modifications sont effectuées sur ce dictionnaire. Enfin les "titulaires" étant éventuellement None sont supprimés des listes de titulaires.

Le fichier JSON exporté est stocké à l'emplacement *results/decp.json*.



Si vous avez connaissance de données essentielles de la commande publique facilement accessibles (téléchargement en masse possible) et qui ne sont pas encore identifiées dans le fichier metadata.json, merci de [nous en informer](#contact).
