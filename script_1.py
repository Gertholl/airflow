import requests



def get_html():
    url = 'http://quotes.toscrape.com/page/1/'
    return requests.get('url').text

def save_html(html:str):
    with open('t.html', 'w') as file:
        file.write(html)

