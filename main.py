from scripts import dataQuality, enrich, portsScraper, etl


def main():
    # seaport_info static table
    portsScraper.port_scraper("csv/port_info.csv")
    enrich.enrich_ports_with_LatLon()
    enrich.nearbyCities()
    enrich.enrich_ports_with_nearby_cities()

    # trades data quality
    dataQuality.qa_enforce()

    # trades ETL
    etl.average_importing_value_by_country()
    etl.average_exporting_value_by_country()
    etl.total_importing_value_by_country()
    etl.total_exporting_value_by_country()
    etl.popular_shipping_routes()


if __name__ == "__main__":
    main()
