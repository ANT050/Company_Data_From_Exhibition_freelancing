import requests
import concurrent.futures
import pandas as pd
from fake_useragent import UserAgent
from bs4 import BeautifulSoup


def get_url_pars(url: str) -> str:
    url_parts = url.split("/")
    url = "/".join(url_parts[:6])

    return url


def fetch_html_content(url: str, headers: dict) -> BeautifulSoup | None:
    try:
        response = requests.get(url, headers, timeout=10)

        if response.status_code == 200:
            html_content = response.text
            soup = BeautifulSoup(html_content, 'lxml')

            return soup

        else:
            print(f"Ошибка: {response.status_code}")

            return None

    except Exception:
        print(f"Ошибка: Неправильно указан URL {url}")

        return None


def get_partial_company_data(url: str, headers: dict) -> list[dict[str, str]]:
    base_url = get_url_pars(url)

    html_document = fetch_html_content(url, headers)
    matching_elements = html_document.find_all('tr', class_='ffTableSet', id=lambda x: x and x.startswith('1000024_'))

    all_links_companies = []
    for elements in matching_elements:
        link_elements = elements.find_all('a')

        if link_elements:
            company_data = {
                "Company_Name": link_elements[0].text,
                "Link_Company": f"{base_url}/{link_elements[0].get('href')}",
                "Booth_Number": link_elements[1].text if len(link_elements) > 1 else "",
                "Link_Booth": f"{base_url}/{link_elements[1].get('href')}" if len(link_elements) > 1 else ""
            }

            all_links_companies.append(company_data)

    return all_links_companies


def get_all_company_data(link: dict, headers: dict) -> dict:
    link_company = link['Link_Company']
    html_document = fetch_html_content(link_company, headers)

    if html_document:
        about_company = (html_document.find('div', class_="longString")
                         or html_document.find('div', style="padding:0px 5px 40px 5px;"))
        digital_press_releases = html_document.find('div', style="padding:0px 5px 10px 5px;")
        company_categories = html_document.find('ul', class_="ffListHelper")

        if about_company:
            description = about_company.text.replace('(less)', '').strip()
        else:
            description = ""
        link["About the Company"] = description

        if company_categories:
            categories = company_categories.text
            edited_data = categories.replace("SHOT - ", '').replace("Supplier - ", '').lstrip(',')
        else:
            edited_data = ""
        link["Company's Categories"] = edited_data

        if digital_press_releases:
            press_releases = digital_press_releases.text
        else:
            press_releases = ""
        link["Digital Press Releases"] = press_releases

        print(link)
    return link


def process_all_links(url: str, headers: dict, threads: int) -> list[dict]:
    all_links = get_partial_company_data(url, headers)

    with concurrent.futures.ThreadPoolExecutor(threads) as executor:
        processed_links = list(executor.map(lambda link: get_all_company_data(link, headers), all_links))

    return processed_links


def write_to_csv(data: list, filename: str) -> None:
    df = pd.DataFrame(data)
    df.columns = [
        "Company_Name",
        "Link_Company",
        "Booth_Number",
        "Link_Booth",
        "About the Company",
        "Company's Categories",
        "Digital Press Releases",
    ]
    df.to_csv(filename, index=False)


def main() -> None:
    url = 'https://n1b.goexposoftware.com/events/ss24/goExpo/exhibitor/listExhibitorProfiles.php'
    headers = {"User-Agent": UserAgent().random}
    number_threads = 10

    products_info = process_all_links(url, headers, number_threads)

    write_to_csv(products_info, 'output.csv')


if __name__ == '__main__':
    main()
