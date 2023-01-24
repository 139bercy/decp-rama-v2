import logging
import sys
import requests
import json


api_url = "https://www.data.gouv.fr/api/1/datasets/"


def main_test():
    all_url = get_urls()
    test_url(all_url)


def get_urls() -> list:
    """Récupère la liste des urls dans le fichier metadata.json"""
    logging.info("Début de la récupération de la liste des url")
    with open("metadata/metadata.json", 'r+') as f:
        metadata_json = json.load(f)

    all_urls = []
    # vérification de la présence de la clé api, si elle n'est pas présente, on ne modifie pas l'url
    for key in metadata_json.keys():
        try:
            for cle in metadata_json[key]["cle_api"]:
                all_urls.append(api_url + cle)
        except KeyError:
            all_urls.append(metadata_json[key]["url_source"])
    return all_urls


def test_url(all_url):
    """Teste l'ensemble des urls"""
    break_circle = False
    for url in all_url:
        try:
            requests.get(url).status_code
        except requests.exceptions.ConnectionError:
            logging.error(f" Url {url} unreachable")
            break_circle = True
    if break_circle:
        sys.exit(404)


if __name__ == "__main__":
    main_test()
