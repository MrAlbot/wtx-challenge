import os

import requests
from bs4 import BeautifulSoup, PageElement
import csv
import re

from requests import Response


def port_scraper(filePath):
    base_url = "https://www.cogoport.com/en/knowledge-center/resources/port-info/"
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")
    port_info = soup.findAll("a", href=True)

    base_url = "https://www.cogoport.com"
    ports = []
    with open(filePath, "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Port Name", "Port ID", "Address", "Shipping Lines"])
        for port in port_info:
            if isinstance(port, PageElement):
                full_port_identifier = port.find("p", class_="styles_read_more__uRZbB")
                if isinstance(full_port_identifier, PageElement):
                    full_port_identifier = full_port_identifier.get_text()
                    port_identifier = re.findall(r"\(([^\)]+)\)", full_port_identifier)
                    if len(port_identifier) == 1:
                        port_identifier = port_identifier[0]
                    else:
                        port_identifier = 'null'
                    port_name = full_port_identifier.split(" ")[0]
                    port_page_relative_url = port['href']
                    port_response = requests.get(base_url + port_page_relative_url)
                    port_soup = BeautifulSoup(port_response.text, "html.parser")
                    port_find_info = port_soup.find("div", class_="styles_left__Ffucy")
                    if isinstance(port_find_info, PageElement):
                        port_find_address = port_find_info.find(lambda tag: tag.name == "p" and "Address:" in tag.text)
                        if isinstance(port_find_address, PageElement):
                            port_address = re.sub(r"Address: ", '', re.sub(r"\n", ', ', port_find_address.get_text()))
                        else:
                            print("fourth")
                    else:
                        port_address = 'null'
                    export_table = port_soup.find("p", class_="styles_accordion_content__COIAs")

                    shipping_lines = list(set(map(lambda x: x.findAll(string=True)[0], export_table.findAll(
                        lambda tag: tag.name == 'tr' and "Line" not in tag.text))))

                    result = {
                        'name': port_name,
                        'identifier': port_identifier,
                        'Address': port_address,
                        'ShippingLines': shipping_lines
                    }
                    ports.append(result)
                    csv_writer.writerow(result.values())
                    print(result.values())

                else:
                    print("second: " + port.get_text())
            else:
                print("first")


if __name__ == "__main__":
    port_scraper("../csv/port_info.csv")
