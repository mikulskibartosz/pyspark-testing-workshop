import re
import requests as r

def test_workshop_name_contains_pyspark():
    response = r.get('https://stacja.it/produkt/jak-uniknac-bledow-w-przetwarzaniu-danych-przy-uzyciu-pyspark/')
    website_html = response.content

    website_title = re.search(r'<title>(.*?)</title>', str(website_html))
    assert 'PySpark' in website_title.group(1)