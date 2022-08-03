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

- **merge_all :** On aggrège les DataFrame en un DataFrame unique.
- **drop_duplicate :** On supprime les lignes dupliquées (marchés présents dans plusieurs sources de données).
- **export_to_xml :** On exporte au format XML réglementaire.
- **export_to_json :** On exporte au format JSON réglementaire.


**4. CONTINUOUS INTEGRATION**

Dans l'objectif de remplacement de decp-rama, decp-rama-v2 dispose d'une CI qui permet de publier automatiquement le résultat de son script de manière journalière. Tous les jours, la CI :
- Lance un docker via Github Action qui s'appuie sur une image publiée sur DockerHub. Cette image est générée via le DockerFile dans le dossier docker/
et publiée grâce au script publish_docker.sh.
- Récuperer le dossier github via actions/checkout@v2.
- Installer les dépendances nécessaires aux scripts de decp-rama-v2.
- Lancer main.py
- Publier sur le serveur FTP de economie.gouv dans le dossier decp/test le résultat du script : results/decp.json

Si vous avez connaissance de données essentielles de la commande publique facilement accessibles (téléchargement en masse possible) et qui ne sont pas encore identifiées dans le fichier metadata.json, merci de [nous en informer](#contact).
