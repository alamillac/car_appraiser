import time
import json
import requests
import logging
import re
from bs4 import BeautifulSoup


logger = logging.getLogger("CarrollaCrawler")


def load_html(content):
    return BeautifulSoup(content, 'html.parser')


def get_specs(specs_soup):
    specs = []
    if not specs_soup:
        logger.warning("Warning: Not specs found")
        return specs

    specs_names = specs_soup.find_all('th')
    specs_values = specs_soup.find_all('td')

    if len(specs_names) != len(specs_values):
        logger.warning("Warning: differences found in specs. %s - %s", specs_names, specs_values)
        return specs

    specs_names = [s.text.strip() for s in specs_names]
    specs_values = [s.text.strip() for s in specs_values]
    for idx, spec_name in enumerate(specs_names):
        specs.append({
            'name': spec_name,
            'value': specs_values[idx]
        })
    return specs


def get_soup_prop(soup, prop):
    prop_soup = soup.find('span', {'itemprop': prop})
    return prop_soup.text.strip() if prop_soup else ''


def get_new_vehicles(soup):
    # Parse new vehicles section
    new_vehicles_section_soup = soup.find('section', class_='new-vehicles')
    if not new_vehicles_section_soup:
        return []

    new_vehicles = []
    new_vehicles_soup = new_vehicles_section_soup.find_all('div', class_="new-vehicle")
    for new_vehicle_soup in new_vehicles_soup:
        link_soup = new_vehicle_soup.find('a')
        vehicle = {
            "image": new_vehicle_soup.find('div', class_='new-vehicle-img').find('img').get('data-original', ''),
            "detail_link": link_soup.get('href', ''),
            "name": new_vehicle_soup.find('span', {'itemprop': 'name'}).text.strip(),
            "model": new_vehicle_soup.find('span', {'itemprop': 'model'}).text.strip(),
            "id": new_vehicle_soup.find('span', {'itemprop': 'productID'}).text.strip(),
            "manufacturer": new_vehicle_soup.find('span', {'itemprop': 'manufacturer'}).text.strip(),
            "title": new_vehicle_soup.find('h2', class_="new-vehicle-heading-title").text.strip(),
            "model_year": new_vehicle_soup.find('span', class_="new-vehicle-model").text.strip(),
            "price": new_vehicle_soup.find('div', class_="new-vehicle-price").text.strip(),
            "specs": []
        }
        new_vehicles.append(vehicle)

        specs_soup = new_vehicle_soup.find('table', class_="specs-table")
        vehicle['specs'] = get_specs(specs_soup)

    return new_vehicles


def get_used_vehicles(soup):
    # Parse search section
    used_vehicles_soup = soup.find('section', class_='search-results').find_all('div', {'itemtype': 'http://schema.org/Offer'})
    used_vehicles = []
    for used_vehicle_soup in used_vehicles_soup:
        link_soup = used_vehicle_soup.find('a')
        vehicle_id = link_soup.get('data-idvehiculo') or get_soup_prop(used_vehicle_soup, 'productID')
        vehicle = {
            "whatsApp_user_id": "", # TODO
            "image": used_vehicle_soup.find('img').get('data-original', ''),
            "detail_link": link_soup.get('href', ''),
            "name": get_soup_prop(used_vehicle_soup, 'name'),
            "model": get_soup_prop(used_vehicle_soup, 'model'),
            "id": vehicle_id,
            "manufacturer": get_soup_prop(used_vehicle_soup, 'manufacturer'),
            "title": used_vehicle_soup.find('h2', class_="car-ad-name").text.strip(),
            "ad_year": used_vehicle_soup.find('h3', class_="car-ad-year").text.strip(),
            "ad_price": used_vehicle_soup.find('div', class_="car-ad-price").text.strip(),
            "specs": [],
        }
        used_vehicles.append(vehicle)

        specs_soup = used_vehicle_soup.find("table", class_="used-specs-table")
        vehicle['specs'] = get_specs(specs_soup)

    return used_vehicles


def parse_page(html):
    # Parse html
    soup = load_html(html)
    new_vehicles = get_new_vehicles(soup)
    used_vehicles = get_used_vehicles(soup)
    return new_vehicles, used_vehicles


def parse_pagination(html):
    soup = load_html(html)
    pagination_text = soup.find('span', class_="results-number").text.strip()
    _, current, total = re.findall(r'\d+', pagination_text)
    return int(current), int(total)


def save_trace(html):
    timestamp = int(time.time()*1000)
    filename = '/var/tmp/carrolla/trace_' + str(timestamp) + '.html'
    logger.info("Saving file %s", filename)
    with open(filename, 'w') as f:
        f.write(html)


def retrieve_data():
    base_url = "https://www.carroya.com"

    client = requests.Session()
    client.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.70 Safari/537.36'
    })

    # Get first page
    logger.info("Parsing page 1")
    home_url = base_url + "/buscar/vehiculos/t4.do"
    response = client.get(home_url)
    if response.status_code != 200:
        logger.error("Error retriving page %s with status code %s and error %s", home_url, response.status_code, response.text)
        return [], []
    new_vehicles, used_vehicles = parse_page(response.text)

    # Paginate
    client.headers.update({
        'X-Requested-With': 'XMLHttpRequest'
    })
    i = 0
    exit = False
    while not exit:
        page = i + 2
        logger.info("Parsing page %s", page)
        timestamp = int(time.time()*1000)
        url = base_url + "/buscar/vehiculos/ajax/filtrando.do?paginaActual={}&_={}".format(page, timestamp)
        try:
            response = client.get(url)
            if response.status_code != 200:
                error_msg = "Error retriving page %s with status code %s and error %s" % (url, response.status_code, response.text)
                save_trace(response.text)
                raise Exception(error_msg)

            tmp_new_vehicles, tmp_used_vehicles = parse_page(response.text)
        except Exception as e:
            save_trace(response.text)
            logger.exception(e)
            return new_vehicles, used_vehicles

        new_vehicles += tmp_new_vehicles
        used_vehicles += tmp_used_vehicles

        current, total = parse_pagination(response.text)
        logger.info("Processed %s from %s", current, total)
        i += 1
        if current >= total:
            logger.info("Retrieve done")
            exit = True

    return new_vehicles, used_vehicles


def save_json(data, name):
    timestamp = int(time.time()*1000)
    filename = '/var/tmp/carrolla/' + name + '_' + str(timestamp) + '.json'
    logger.info("Saving file %s", filename)
    with open(filename, 'w') as f:
        json.dump(data, f)


def main():
    logging.basicConfig(level=logging.DEBUG)

    logger.info("Retrieve data")
    new_vehicles, used_vehicles = retrieve_data()
    logger.info("Found %s new vehicles", len(new_vehicles))
    logger.info("Found %s used vehicles", len(used_vehicles))

    logger.info("Saving data")
    save_json(new_vehicles, 'new_vehicles')
    save_json(used_vehicles, 'used_vehicles')


main()
